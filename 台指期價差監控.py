# -*- coding: utf-8 -*-
"""
台指期正逆價差即時監控 — 單檔 LAN 伺服器版 v2
================================================
功能：
  * TX/TE/TF/XIF（期交所 MIS，鉅亨網備援）＋ TWN 富台期（SGX 延遲報價）
  * 表面價差 ＋ 還原除息後真實價差，60 秒自動更新
  * 各報價附今日漲跌箭頭（vs 昨收，▲▼點數＋%，金融慣例）；價差附「較5分前／今日盤初」動量
  * 啟動即一次抓取近 5 年日線歷史（鉅亨網），並持續累積即時快照（伺服器端共用）
  * 歷史圖表：拖曳圖面＝平移、拖曳時間軸＝縮放時間刻度、拖曳價格軸＝縮放 Y、滾輪縮放
  * 無外部 JS 依賴，純內網可用

使用方式（僅需 Python 3.8+，無需安裝任何套件）：
    python 台指期價差監控.py            # 預設埠 8701
    python 台指期價差監控.py 8702       # 自訂埠號

啟動後終端機會顯示本機實際偵測到的區網網址。
資料檔（與本檔同目錄，全區網共用）：
    spread_config.json     除息點數設定
    spread_daily.json      日線歷史（累積式，只增量更新）
    spread_intraday.json   5分線歷史（累積式，跨日保留）
    spread_inst.json       外資部位/PCR/未平倉（累積式）
    spread_bench.json      對標數據歷史（累積式）
    spread_snapshots.jsonl 即時快照（每分鐘累積）
除息點數基準：富邦期貨 2026/6/11「除息影響點數分析」，逾期請至網頁「除息資料設定」更新。
本工具僅供參考，不構成投資建議。
"""
# build 0617a
import json
import os
import queue
import re
import socket
import ssl
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HOST = os.environ.get("SPREAD_HOST", "0.0.0.0")
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8701
BASE = Path(__file__).resolve().parent
CONFIG_FILE = BASE / "spread_config.json"
DAILY_FILE = BASE / "spread_daily.json"
SNAP_FILE = BASE / "spread_snapshots.jsonl"
QUOTE_CACHE_SECONDS = 15
SNAP_MIN_INTERVAL = 55          # 快照最小間隔（秒）
SNAP_MAX_RECORDS = 40000        # 快照上限（約一個月交易分鐘數）
DAILY_REFRESH_SECONDS = 6 * 3600

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

TZ_TW = timezone(timedelta(hours=8))
BOOT_ID = datetime.now(TZ_TW).strftime("%m%d-%H%M%S")  # 伺服器啟動識別碼(各電腦比對是否同一台)

PRODUCTS = ["TX", "TE", "TF", "XIF", "TWN"]
TAIFEX_CID = {"TX": "TXF", "TE": "EXF", "TF": "FXF", "XIF": "XIF"}
CNYES_FUT = {"TX": "TWF:TXF:FUTURES", "TE": "TWF:EXF:FUTURES",
             "TF": "TWF:FXF:FUTURES", "XIF": "TWF:XIF:FUTURES"}
CNYES_IDX = {"TX": "TWS:TSE01:INDEX", "TE": "TWS:TSE27:INDEX",
             "TF": "TWS:TSE31:INDEX", "XIF": "TWS:TSE34:INDEX"}

DEFAULT_CFG = {
    "seedDate": "2026-06-11",
    "seedRemaining": {
        "2026-06": [49.20, 1.81, 1.33, 80.39, 11.28],
        "2026-07": [336.93, 15.12, 60.88, 244.25, 45.72],
        "2026-08": [452.14, 17.75, 99.64, 366.20, 53.59],
        "2026-09": [537.26, 23.49, 104.65, 387.02, 56.61],
        "2026-10": [543.38, 23.53, 104.65, 405.41, 56.61],
    },
    "daily": [
        ["2026-06-12", 5.03, 0.10, 0.00, 12.20, 0.82],
        ["2026-06-15", 8.11, 0.44, 0.24, 7.51, 0.75],
        ["2026-06-16", 3.69, 0.16, 0.71, 2.87, 0.08],
        ["2026-06-17", 32.37, 1.12, 0.37, 57.81, 3.73],
        ["2026-06-18", 1.87, 0.02, 0.19, 4.53, 0.00],
        ["2026-06-19", 0.01, 0.00, 0.00, 0.00, 0.00],
        ["2026-06-22", 11.47, 0.72, 0.00, 7.69, 0.97],
        ["2026-06-23", 8.70, 0.23, 0.92, 15.55, 0.27],
        ["2026-06-24", 17.84, 0.89, 0.00, 21.29, 2.60],
        ["2026-06-25", 5.77, 0.31, 0.00, 5.80, 0.27],
    ],
    "twnSpot": 3769.01,
    "twnSpotNote": "2026-06-10 收盤（手動）",
    "twnSpotRatio": None,        # 富台現貨／TAIEX 校準比例；有值時自動以 TAIEX×比例推估當日現貨
    "costRate": 1.5,
    "miniTickers": [],           # 自訂子圖表代號（最多8個）：台股輸入數字代號如 2330、美股如 nvda
    "refBarOn": False,           # 對照行情列：預設隱藏，於參數設定中開啟
    "refBarHide": [],            # 對照行情列：要隱藏的標準項目 key
    "refBarAdd": [],             # 對照行情列：手動加入 ticker（台股數字自動轉 .TW；美股代號）
}


# ---------------------------------------------------------------- 公用
def lan_ips():
    ips = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ips.append(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if not ip.startswith("127.") and ip not in ips:
                ips.append(ip)
    except Exception:
        pass
    return ips


def _http(url, data=None, headers=None, timeout=12):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read()
    except ssl.SSLError:
        ctx = ssl._create_unverified_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read()


def num(v):
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return f
    except (TypeError, ValueError):
        return None


TAIFEX_M = "ABCDEFGHIJKL"
SGX_M = {"F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
         "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12}


def third_wed(y, m):
    d = date(y, m, 1)
    off = (2 - d.weekday() + 7) % 7  # weekday: Mon=0 … Wed=2
    return date(y, m, 1 + off + 14).isoformat()


# ---------------------------------------------------------------- 即時報價抓取
def fetch_taifex(cid):
    body = json.dumps({
        "MarketType": "0", "SymbolType": "F", "KindID": "1", "CID": cid,
        "ExpireMonth": "", "RowSize": "全部", "PageNo": "",
        "SortColumn": "", "AscDesc": "A",
    }).encode("utf-8")
    raw = _http(
        "https://mis.taifex.com.tw/futures/api/getQuoteList", data=body,
        headers={"Content-Type": "application/json", "User-Agent": UA,
                 "Accept": "application/json",
                 "Referer": "https://mis.taifex.com.tw/futures/"})
    rows = json.loads(raw)["RtData"]["QuoteList"]
    keep = []
    for q in rows:
        sid = q.get("SymbolID", "")
        if not (sid.endswith("-S") or re.match(r"^[A-Z]{3}[A-L]\d-F$", sid)):
            continue
        keep.append({"sym": sid, "name": q.get("DispCName"),
                     "last": q.get("CLastPrice"), "ref": q.get("CRefPrice"),
                     "date": q.get("CDate"), "time": q.get("CTime"),
                     "bid": q.get("CBestBidPrice"), "ask": q.get("CBestAskPrice"),
                     "vol": q.get("CTotalVolume"),
                     "test": q.get("CTestPrice"), "ttime": q.get("CTestTime")})
    return keep


def fetch_sgx():
    raw = _http("https://api.sgx.com/derivatives/v1.0/?category=futures",
                headers={"User-Agent": UA}, timeout=20)
    d = json.loads(raw)
    tw = {}
    for x in (d.get("data") or []):
        s = str(x.get("symbol", ""))
        if s.startswith("TWN") and "_" not in s:
            cur = tw.get(s)
            if not cur or str(x.get("record-update-time", "")) > str(cur.get("record-update-time", "")):
                tw[s] = x
    return [{"sym": x.get("symbol"), "last": x.get("last-traded-price-adj"),
             "bid": x.get("best-bid-price-abs"), "ask": x.get("best-ask-price-abs"),
             "settle": x.get("daily-settlement-price-adj"),
             "ltd": x.get("last-trading-date"),
             "chg": x.get("change-abs"), "chgPct": x.get("change-percentage"),
             "upd": x.get("record-update-time")} for x in tw.values()]


REF_SYMS = ["TWS:2330:STOCK", "TWS:2317:STOCK", "TWS:2454:STOCK", "TWS:0050:STOCK",
            "TWS:OTC01:INDEX", "FX:USDTWD:FOREX", "GI:NDX:INDEX", "GI:DJI:INDEX"]


def fetch_cnyes():
    syms = ",".join(list(CNYES_FUT.values()) + list(CNYES_IDX.values()) + REF_SYMS)
    raw = _http("https://ws.api.cnyes.com/ws/api/v1/quote/quotes/" + syms,
                headers={"User-Agent": UA})
    d = json.loads(raw)
    cn = {}
    for x in d.get("data", []):
        cn[str(x.get("0", ""))] = {
            "name": x.get("200009"), "last": x.get("6"), "t": x.get("200007"),
            "chg": x.get("11"), "chgPct": x.get("56"), "prev": x.get("21")}
    return cn or None


_cache = {"t": 0.0, "data": None}
_cache_lock = threading.Lock()
_yref_cache = {"t": 0.0, "data": None}
_usidx_cache = {"t": 0.0, "data": None}


def fetch_usidx():
    """美股主要指數 5 分線（道瓊/S&P/那指/那指100/費半，Yahoo v8 chart，快取5分鐘）"""
    with _cache_lock:
        if _usidx_cache["data"] and time.time() - _usidx_cache["t"] < 300:
            return _usidx_cache["data"]
    out = {}
    for sym, enc in [("^DJI", "%5EDJI"), ("^GSPC", "%5EGSPC"),
                     ("^IXIC", "%5EIXIC"), ("^NDX", "%5ENDX"),
                     ("^SOX", "%5ESOX")]:
        try:
            d = json.loads(_http(
                "https://query1.finance.yahoo.com/v8/finance/chart/" + enc +
                "?interval=5m&range=1d&includePrePost=true", headers={"User-Agent": UA}, timeout=12))
            r0 = d["chart"]["result"][0]
            ts = r0.get("timestamp") or []
            cl = (r0.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
            t2, c2 = [], []
            for a, b in zip(ts, cl):
                if b is not None:
                    t2.append(a)
                    c2.append(round(float(b), 2))
            out[sym] = {"t": t2, "c": c2,
                        "prev": r0.get("meta", {}).get("chartPreviousClose"),
                        "sess": _yahoo_sessions(r0.get("meta", {}))}
        except Exception:
            pass
    with _cache_lock:
        _usidx_cache["t"] = time.time()
        _usidx_cache["data"] = out or None
    return out or {}


def fetch_yahoo():
    """台積電ADR/費半/日經/KOSPI＋對照行情(比特幣/Brent/美10年債/四大期指)＋手動 ticker（Yahoo meta，快取5分鐘）"""
    syms = [("TSM", "TSM"), ("^SOX", "%5ESOX"),
            ("^N225", "%5EN225"), ("^KS11", "%5EKS11"),
            ("BTC-USD", "BTC-USD"), ("BZ=F", "BZ=F"), ("^TNX", "%5ETNX"),
            ("YM=F", "YM=F"), ("ES=F", "ES=F"), ("NQ=F", "NQ=F"), ("RTY=F", "RTY=F")]
    try:                                     # 對照行情手動 ticker：台股數字→.TW，其餘大寫
        seen = {s for s, _ in syms}
        for raw in (load_cfg().get("refBarAdd") or [])[:12]:
            raw = (raw or "").strip()
            if not raw:
                continue
            s = (raw + ".TW") if raw.isdigit() else raw.upper()
            if s in seen:
                continue
            seen.add(s)
            syms.append((s, s.replace("^", "%5E")))
    except Exception:
        pass
    sig = tuple(s for s, _ in syms)
    with _cache_lock:
        if (_yref_cache["data"] and time.time() - _yref_cache["t"] < 300
                and _yref_cache.get("sig") == sig):
            return _yref_cache["data"]
    out = {}
    for sym, enc in syms:
        try:
            d = json.loads(_http(
                "https://query1.finance.yahoo.com/v8/finance/chart/" + enc +
                "?interval=1d&range=2d",
                headers={"User-Agent": UA}, timeout=12))
            m = d["chart"]["result"][0]["meta"]
            out[sym] = {"last": m.get("regularMarketPrice"),
                        "prev": m.get("chartPreviousClose")}
        except Exception:
            pass
    with _cache_lock:
        _yref_cache["t"] = time.time()
        _yref_cache["sig"] = sig
        _yref_cache["data"] = out or None
    return out or None


def fetch_all():
    with _cache_lock:
        if _cache["data"] and time.time() - _cache["t"] < QUOTE_CACHE_SECONDS:
            return _cache["data"]
    out = {"ts": int(time.time()), "taifex": {}, "cnyes": None, "sgx": [], "yahoo": None, "err": []}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fetch_taifex, cid): "taifex:" + cid
                for cid in TAIFEX_CID.values()}
        futs[ex.submit(fetch_sgx)] = "sgx"
        futs[ex.submit(fetch_cnyes)] = "cnyes"
        futs[ex.submit(fetch_yahoo)] = "yahoo"
        for f in futs:
            tag = futs[f]
            try:
                r = f.result()
                if tag.startswith("taifex:"):
                    if r:
                        out["taifex"][tag.split(":")[1]] = r
                elif tag == "sgx":
                    out["sgx"] = r
                elif tag == "yahoo":
                    out["yahoo"] = r
                else:
                    out["cnyes"] = r
            except Exception as e:
                out["err"].append(tag + ":" + str(e)[:80])
    with _cache_lock:
        _cache["t"] = time.time()
        _cache["data"] = out
    record_snapshot(out)
    return out


# ---------------------------------------------------------------- 快照（伺服器端共用歷史）
def _parse_taifex_contracts(rows, today_str):
    spot = None
    futs = []
    for r in rows:
        if r["sym"].endswith("-S"):
            spot = num(r["last"]) or num(r["ref"])
            continue
        m = re.match(r"^[A-Z]{3}([A-L])(\d)-F$", r["sym"])
        if not m:
            continue
        mon = TAIFEX_M.index(m.group(1)) + 1
        yr = 2020 + int(m.group(2))
        ym = "%d-%02d" % (yr, mon)
        settle = third_wed(yr, mon)
        px = num(r["last"])
        if px is None:
            b, a = num(r["bid"]), num(r["ask"])
            if b is not None and a is not None:
                px = (b + a) / 2
        if px is None:
            px = num(r["ref"])
        if px is not None and settle >= today_str:
            futs.append({"ym": ym, "settle": settle, "px": px})
    futs.sort(key=lambda x: x["ym"])
    return spot, futs


def _parse_sgx_contracts(sgx, today_str):
    futs = []
    for x in sgx:
        m = re.match(r"^TWN([FGHJKMNQUVXZ])(\d\d)$", str(x.get("sym", "")))
        if not m:
            continue
        ym = "%d-%02d" % (2000 + int(m.group(2)), SGX_M[m.group(1)])
        px = num(x.get("last"))
        if px is None:
            b, a = num(x.get("bid")), num(x.get("ask"))
            if b is not None and a is not None:
                px = (b + a) / 2
        ltd = x.get("ltd") or ""
        if px is not None and ltd >= today_str:
            futs.append({"ym": ym, "settle": ltd, "px": px})
    futs.sort(key=lambda x: x["ym"])   # 以合約年月(保證 ISO 格式)排序，避免 ltd 字串格式不一致時近/次月排錯
    return futs


def _in_session():
    t = datetime.now(TZ_TW)
    return t.weekday() <= 4 and 840 <= t.hour * 100 + t.minute <= 1355


_snaps = []
_snap_lock = threading.Lock()
# SSE 即時推播：每個連線一個 queue，record_snapshot 廣播新快照
_sse_subs = set()
_sse_lock = threading.Lock()


def _sse_broadcast(entry):
    payload = ("data: " + json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n\n").encode("utf-8")
    with _sse_lock:
        subs = list(_sse_subs)
    for q in subs:
        try:
            q.put_nowait(payload)
        except Exception:
            pass


def load_snaps():
    if not SNAP_FILE.exists():
        return
    with _snap_lock:
        for line in SNAP_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    _snaps.append(json.loads(line))
                except Exception:
                    pass
        del _snaps[:-SNAP_MAX_RECORDS]


def record_snapshot(raw):
    if not _in_session():
        return
    now = int(time.time())
    with _snap_lock:
        if _snaps and now - _snaps[-1]["t"] < SNAP_MIN_INTERVAL:
            return
    today_str = datetime.now(TZ_TW).date().isoformat()
    entry = {"t": now, "p": {}}
    for p in ["TX", "TE", "TF", "XIF"]:
        rows = raw["taifex"].get(TAIFEX_CID[p])
        if rows:
            spot, futs = _parse_taifex_contracts(rows, today_str)
        elif raw.get("cnyes"):
            f = raw["cnyes"].get(CNYES_FUT[p]) or {}
            i = raw["cnyes"].get(CNYES_IDX[p]) or {}
            spot = num(i.get("last"))
            y, m = int(today_str[:4]), int(today_str[5:7])
            if today_str > third_wed(y, m):
                m += 1
                if m > 12:
                    m, y = 1, y + 1
            futs = ([{"ym": "%d-%02d" % (y, m), "settle": third_wed(y, m),
                      "px": num(f.get("last"))}] if num(f.get("last")) is not None else [])
        else:
            continue
        if spot is None and not futs:
            continue
        e = {"s": spot}
        if len(futs) > 0:
            e.update({"n": futs[0]["px"], "ny": futs[0]["ym"], "ns": futs[0]["settle"]})
        if len(futs) > 1:
            e.update({"x": futs[1]["px"], "xy": futs[1]["ym"], "xs": futs[1]["settle"]})
        entry["p"][p] = e
    tw = _parse_sgx_contracts(raw.get("sgx") or [], today_str)
    if tw:
        e = {"s": None, "n": tw[0]["px"], "ny": tw[0]["ym"], "ns": tw[0]["settle"]}
        if len(tw) > 1:
            e.update({"x": tw[1]["px"], "xy": tw[1]["ym"], "xs": tw[1]["settle"]})
        entry["p"]["TWN"] = e
    if not entry["p"]:
        return
    with _snap_lock:
        _snaps.append(entry)
        try:
            with SNAP_FILE.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception:
            pass
        if len(_snaps) > SNAP_MAX_RECORDS + 2000:  # 壓縮檔案
            del _snaps[:-SNAP_MAX_RECORDS]
            try:
                SNAP_FILE.write_text(
                    "\n".join(json.dumps(s, ensure_ascii=False, separators=(",", ":"))
                              for s in _snaps) + "\n", encoding="utf-8")
            except Exception:
                pass
    _sse_broadcast(entry)


def get_snaps(after=0):
    with _snap_lock:
        if after:
            return [s for s in _snaps if s["t"] > after]
        return list(_snaps)


# ---------------------------------------------------------------- 歷史資料（累積式儲存：只增量抓取、永久保留）
INTRA_FILE = BASE / "spread_intraday.json"
INTRA_MAX_BARS = 40000          # 每商品 5 分線上限（約半年）
INTRA_FETCH_GAP = 60            # 5 分線最短重抓間隔（秒）；盤中要即時跟上當日
DAILY_FETCH_GAP = 3600          # 日線最短重抓間隔（秒）

_daily_lock = threading.Lock()
_daily = {"fetched": 0, "data": {}}      # data[p] = {"d":[],"fut":[],"spot":[]}
_intra_lock = threading.Lock()
_intra = {"fetched": {}, "data": {}}     # data[p] = {"t":[],"fut":[],"spot":[]}


def _fetch_chart(symbol, resolution):
    url = ("https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol=" + symbol +
           "&resolution=" + resolution + "&from=" + str(int(time.time())) + "&to=0")
    d = json.loads(_http(url, headers={"User-Agent": UA}, timeout=20))
    dd = d.get("data") or {}
    return dd.get("t") or [], dd.get("c") or []


def load_daily_cache():
    if DAILY_FILE.exists():
        try:
            obj = json.loads(DAILY_FILE.read_text(encoding="utf-8"))
            with _daily_lock:
                _daily["fetched"] = obj.get("fetched", obj.get("ts", 0))
                _daily["data"] = obj.get("data", {})
        except Exception:
            pass


def _save_daily():
    with _daily_lock:
        payload = json.dumps({"fetched": _daily["fetched"], "data": _daily["data"]},
                             ensure_ascii=False, separators=(",", ":"))
    try:
        DAILY_FILE.write_text(payload, encoding="utf-8")
    except Exception:
        pass


def _merge_daily(p, rows):
    """rows: [(dstr, fut, spot)] 依日期合併進累積儲存（新值覆蓋同日舊值）"""
    with _daily_lock:
        cur = _daily["data"].setdefault(p, {"d": [], "fut": [], "spot": []})
        m = {d: (f, s) for d, f, s in zip(cur["d"], cur["fut"], cur["spot"])}
        for d, f, s in rows:
            m[d] = (f, s)
        ds = sorted(m)
        cur["d"] = ds
        cur["fut"] = [m[d][0] for d in ds]
        cur["spot"] = [m[d][1] for d in ds]


def _daily_needs_fetch():
    with _daily_lock:
        age = time.time() - _daily["fetched"]
        if age < DAILY_FETCH_GAP:
            return False
        d = (_daily["data"].get("TX") or {}).get("d") or []
        today = datetime.now(TZ_TW).date().isoformat()
        if d and d[-1] >= today and age < DAILY_REFRESH_SECONDS:
            return False        # 今日已入庫且未超過刷新週期
        if not d:
            return True
    return True


def refresh_daily(force=False):
    """從累積儲存供應日線；僅在必要時呼叫 API 並把新資料合併入庫"""
    if force or _daily_needs_fetch():
        changed = False
        for p in ["TX", "TE", "TF", "XIF"]:
            try:
                ft, fc = _fetch_chart(CNYES_FUT[p], "D")
                it, ic = _fetch_chart(CNYES_IDX[p], "D")
                fmap = {}
                for ts, c in zip(ft, fc):
                    if c is not None:
                        fmap[datetime.fromtimestamp(ts, TZ_TW).date().isoformat()] = c
                rows = []
                for ts, c in zip(it, ic):
                    dstr = datetime.fromtimestamp(ts, TZ_TW).date().isoformat()
                    if c is not None and dstr in fmap:
                        rows.append((dstr, round(fmap[dstr], 2), round(c, 2)))
                if rows:
                    _merge_daily(p, rows)
                    changed = True
            except Exception:
                pass
        with _daily_lock:
            _daily["fetched"] = time.time()
        if changed:
            _save_daily()
    with _daily_lock:
        return {p: dict(v) for p, v in _daily["data"].items()}


def load_intra_cache():
    if INTRA_FILE.exists():
        try:
            obj = json.loads(INTRA_FILE.read_text(encoding="utf-8"))
            with _intra_lock:
                _intra["data"] = obj.get("data", {})
        except Exception:
            pass


def _save_intra():
    with _intra_lock:
        payload = json.dumps({"data": _intra["data"]},
                             ensure_ascii=False, separators=(",", ":"))
    try:
        INTRA_FILE.write_text(payload, encoding="utf-8")
    except Exception:
        pass


def _merge_intra(p, t_list, fut_list, spot_list):
    with _intra_lock:
        cur = _intra["data"].setdefault(p, {"t": [], "fut": [], "spot": []})
        m = {ts: (f, s) for ts, f, s in zip(cur["t"], cur["fut"], cur["spot"])}
        for i, ts in enumerate(t_list):
            old = m.get(ts)
            s = spot_list[i] if spot_list[i] is not None else (old[1] if old else None)
            m[ts] = (fut_list[i], s)
        keys = sorted(m)[-INTRA_MAX_BARS:]
        cur["t"] = keys
        cur["fut"] = [m[k][0] for k in keys]
        cur["spot"] = [m[k][1] for k in keys]


def _in_cash_5m(ts):
    """台股現貨指數連續交易時段 09:00–13:30（snap 現貨在此區間才有效）"""
    d = datetime.fromtimestamp(ts, TZ_TW)
    hm = d.hour * 100 + d.minute
    return d.weekday() <= 4 and 900 <= hm <= 1330


def _snap_spot_5m(p):
    """把伺服器每分鐘快照的現貨對齊到 5 分鐘格 {5min_ts: spot}，僅取盤中現貨時段"""
    out = {}
    with _snap_lock:
        for s in _snaps:
            if not _in_cash_5m(s["t"]):
                continue
            e = s["p"].get(p)
            if e and e.get("s") is not None:
                out[(s["t"] // 300) * 300] = round(e["s"], 2)
    return out


def intraday_5m(p):
    """從累積儲存供應 5 分線；超過間隔才呼叫 API 把當日新 K 棒合併入庫"""
    nowt = time.time()
    with _intra_lock:
        fresh = nowt - _intra["fetched"].get(p, 0) < INTRA_FETCH_GAP
    if not fresh:
        try:
            ft, fc = _fetch_chart(CNYES_FUT[p], "5")
            it, ic = _fetch_chart(CNYES_IDX[p], "5")
            imap = {ts: c for ts, c in zip(it, ic) if c is not None}
            rows = sorted((ts, fc[i]) for i, ts in enumerate(ft) if fc[i] is not None)
            t_list = [r[0] for r in rows]
            fut_list = [round(r[1], 2) for r in rows]
            spot_list = [round(imap[ts], 2) if ts in imap else None for ts in t_list]
            # cnyes 不提供台股「指數」盤中分線(只有日線)，當日盤中現貨會缺；用伺服器每分鐘快照的現貨
            # (來自即時報價，可靠)補上 09:00–13:30 的現貨，盤中5分價差圖才能即時跟上當日
            if any(s is None for s in spot_list):
                ss = _snap_spot_5m(p)
                if ss:
                    for i, ts in enumerate(t_list):
                        if spot_list[i] is None:
                            v = ss.get((ts // 300) * 300)
                            if v is not None:
                                spot_list[i] = v
            if t_list:
                _merge_intra(p, t_list, fut_list, spot_list)
            with _intra_lock:
                _intra["fetched"][p] = nowt
            _save_intra()
        except Exception:
            pass   # API 失敗仍供應既有累積資料
    with _intra_lock:
        return dict(_intra["data"].get(p, {"t": [], "fut": [], "spot": []}))


# ---------------------------------------------------------------- 市場脈絡（外資部位/PCR/換月OI，累積式）
INST_FILE = BASE / "spread_inst.json"
INST_FETCH_GAP = 3600
_inst_lock = threading.Lock()
_inst = {"fetched": 0, "dates": {}}


def load_inst_cache():
    if INST_FILE.exists():
        try:
            obj = json.loads(INST_FILE.read_text(encoding="utf-8"))
            with _inst_lock:
                _inst["dates"] = obj.get("dates", {})
        except Exception:
            pass


def _save_inst():
    with _inst_lock:
        payload = json.dumps({"dates": _inst["dates"]},
                             ensure_ascii=False, separators=(",", ":"))
    try:
        INST_FILE.write_text(payload, encoding="utf-8")
    except Exception:
        pass


def _openapi(ep):
    raw = _http("https://openapi.taifex.com.tw/v1/" + ep,
                headers={"User-Agent": UA, "Accept": "application/json"}, timeout=20)
    return json.loads(raw)


def _dkey(s):
    s = str(s)
    return s[:4] + "-" + s[4:6] + "-" + s[6:8] if len(s) == 8 else s


def _toint(v):
    try:
        return int(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def refresh_inst(force=False):
    with _inst_lock:
        if not force and time.time() - _inst["fetched"] < INST_FETCH_GAP:
            return
    changed = False
    try:    # 三大法人（臺股期貨淨未平倉）
        for x in _openapi("MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate"):
            if x.get("ContractCode") != "臺股期貨":
                continue
            d = _dkey(x.get("Date"))
            lo = _toint(x.get("OpenInterest(Long)"))
            sh = _toint(x.get("OpenInterest(Short)"))
            key = {"外資及陸資": "fiN", "投信": "itN", "自營商": "dN"}.get(x.get("Item"))
            if key and lo is not None and sh is not None:
                with _inst_lock:
                    _inst["dates"].setdefault(d, {})[key] = lo - sh
                changed = True
    except Exception:
        pass
    try:    # Put/Call Ratio（API 自帶近一個月歷史）
        for x in _openapi("PutCallRatio"):
            d = _dkey(x.get("Date"))
            with _inst_lock:
                ent = _inst["dates"].setdefault(d, {})
                ent["pcrV"] = num(x.get("PutCallVolumeRatio%"))
                ent["pcrOI"] = num(x.get("PutCallOIRatio%"))
            changed = True
    except Exception:
        pass
    try:    # TX 各月份未平倉（換月觀察）
        oi, d = {}, None
        for x in _openapi("DailyMarketReportFut"):
            if x.get("Contract") != "TX" or x.get("TradingSession") != "一般":
                continue
            d = _dkey(x.get("Date"))
            mon = str(x.get("ContractMonth(Week)", ""))
            v = _toint(x.get("OpenInterest"))
            if re.match(r"^\d{6}$", mon) and v is not None:
                oi[mon] = v
        if d and oi:
            with _inst_lock:
                _inst["dates"].setdefault(d, {})["txOI"] = oi
            changed = True
    except Exception:
        pass
    with _inst_lock:
        _inst["fetched"] = time.time()
    if changed:
        _save_inst()


def get_context():
    with _inst_lock:
        ds = sorted(_inst["dates"])
        return {"dates": [dict(_inst["dates"][d], d=d) for d in ds]}


# ---------------------------------------------------------------- 對標數據（累積式，供圖表比較）
BENCH_FILE = BASE / "spread_bench.json"
BENCH_ALLOWED = {"TWS:2330:STOCK", "TWS:0050:STOCK", "TWS:OTC01:INDEX",
                 "TWS:2317:STOCK", "TWS:2454:STOCK",
                 "GI:NDX:INDEX", "FX:USDTWD:FOREX"}
_bench_lock = threading.Lock()
_bench = {"fetched": {}, "data": {}}     # data[code][res] = {"t":[],"c":[]}


def load_bench_cache():
    if BENCH_FILE.exists():
        try:
            obj = json.loads(BENCH_FILE.read_text(encoding="utf-8"))
            with _bench_lock:
                _bench["data"] = obj.get("data", {})
        except Exception:
            pass


def _save_bench():
    with _bench_lock:
        payload = json.dumps({"data": _bench["data"]},
                             ensure_ascii=False, separators=(",", ":"))
    try:
        BENCH_FILE.write_text(payload, encoding="utf-8")
    except Exception:
        pass


def get_bench(code, res):
    key = code + "|" + res
    gap = DAILY_FETCH_GAP if res == "D" else INTRA_FETCH_GAP
    with _bench_lock:
        fresh = time.time() - _bench["fetched"].get(key, 0) < gap
    if not fresh:
        try:
            ts, cs = _fetch_chart(code, "D" if res == "D" else "5")
            rows = []
            for t, c in zip(ts, cs):
                if c is None:
                    continue
                k = (datetime.fromtimestamp(t, TZ_TW).date().isoformat()
                     if res == "D" else t)
                rows.append((k, round(float(c), 4)))
            with _bench_lock:
                cur = _bench["data"].setdefault(code, {}).setdefault(res, {"t": [], "c": []})
                m = dict(zip(cur["t"], cur["c"]))
                for k, c in rows:
                    m[k] = c
                ks = sorted(m)
                if res == "5":
                    ks = ks[-INTRA_MAX_BARS:]
                cur["t"] = ks
                cur["c"] = [m[k] for k in ks]
                _bench["fetched"][key] = time.time()
            _save_bench()
        except Exception:
            with _bench_lock:
                _bench["fetched"][key] = time.time() - gap + 120
    with _bench_lock:
        d = _bench["data"].get(code, {}).get(res, {"t": [], "c": []})
        return {"t": list(d["t"]), "c": list(d["c"])}


# ---------------------------------------------------------------- 自訂子圖表 ticker（台股 cnyes／美股 Yahoo，5分線，快取）
_ticker_lock = threading.Lock()
_ticker_cache = {}      # key(upper) -> {"t": ts, "d": {...}}
TW_TICKER_RE = re.compile(r"^\d{3,6}[A-Za-z]?$")


def _is_tw_ticker(raw):
    return bool(TW_TICKER_RE.fullmatch(raw))


# Yahoo 代號後綴 → 市場中文名（無後綴視為美股）
YH_SUFFIX_MKT = {
    "KS": "韓股", "KQ": "韓股", "T": "日股", "HK": "港股",
    "SS": "陸股", "SZ": "陸股", "TW": "台股", "TWO": "台股",
    "L": "英股", "DE": "德股", "PA": "法股", "AS": "荷股",
    "MI": "義股", "MC": "西股", "SW": "瑞士股", "ST": "瑞典股",
    "HE": "芬蘭股", "OL": "挪威股", "CO": "丹麥股", "AX": "澳股",
    "NZ": "紐股", "TO": "加股", "V": "加股", "SI": "星股",
    "BO": "印度股", "NS": "印度股", "SA": "巴西股", "MX": "墨股",
}


def _yahoo_market(sym, meta):
    s = sym.upper()
    suf = s.rsplit(".", 1)[1] if "." in s else ""
    if suf in YH_SUFFIX_MKT:
        return YH_SUFFIX_MKT[suf]
    cur = (meta or {}).get("currency")
    if cur and cur != "USD":
        return (meta or {}).get("fullExchangeName") or (meta or {}).get("exchangeName") or "海外"
    return "美股"


def _yahoo_sessions(meta):
    """由 Yahoo meta.currentTradingPeriod 取盤前/盤中/盤後的起訖秒（供前端陰影/延長時段窗）"""
    ctp = (meta or {}).get("currentTradingPeriod") or {}
    out = {}
    for src, dst in (("pre", "pre"), ("regular", "reg"), ("post", "post")):
        seg = ctp.get(src)
        if isinstance(seg, dict) and seg.get("start") is not None and seg.get("end") is not None:
            out[dst] = [int(seg["start"]), int(seg["end"])]
    return out or None


def fetch_ticker_5m(raw):
    """台股數字代號→cnyes 5分線；其餘視為 Yahoo 標的 5分線。回傳 {t,c,prev,market,name,mkt}"""
    raw = raw.strip()
    if _is_tw_ticker(raw):
        code = "TWS:%s:STOCK" % raw.upper()
        ts, cs = _fetch_chart(code, "5")
        t2, c2 = [], []
        for a, b in zip(ts, cs):
            if b is not None:
                t2.append(a)
                c2.append(round(float(b), 4))
        prev = None
        name = None
        try:
            q = json.loads(_http(
                "https://ws.api.cnyes.com/ws/api/v1/quote/quotes/" + code,
                headers={"User-Agent": UA}, timeout=12))
            for x in q.get("data", []):
                prev = num(x.get("21"))
                name = x.get("200009") or name
        except Exception:
            pass
        return {"t": t2, "c": c2, "prev": prev, "market": "tw",
                "name": name or raw.upper(), "mkt": "台股"}
    sym = raw.upper()
    d = json.loads(_http(
        "https://query1.finance.yahoo.com/v8/finance/chart/" +
        urllib.parse.quote(sym) + "?interval=5m&range=1d&includePrePost=true",
        headers={"User-Agent": UA}, timeout=12))
    r0 = d["chart"]["result"][0]
    ts = r0.get("timestamp") or []
    cl = (r0.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
    t2, c2 = [], []
    for a, b in zip(ts, cl):
        if b is not None:
            t2.append(a)
            c2.append(round(float(b), 2))
    meta = r0.get("meta", {})
    name = meta.get("shortName") or meta.get("longName") or sym
    return {"t": t2, "c": c2, "prev": meta.get("chartPreviousClose"),
            "market": "us", "name": name, "mkt": _yahoo_market(sym, meta),
            "sess": _yahoo_sessions(meta)}


def get_ticker(raw):
    key = raw.strip().upper()
    gap = INTRA_FETCH_GAP if _is_tw_ticker(key) else 300
    with _ticker_lock:
        c = _ticker_cache.get(key)
        if c and time.time() - c["t"] < gap:
            return c["d"]
    try:
        d = fetch_ticker_5m(raw)
    except Exception:
        d = {"t": [], "c": [], "prev": None, "market": "tw" if _is_tw_ticker(key) else "us"}
    with _ticker_lock:
        _ticker_cache[key] = {"t": time.time(), "d": d}
    return d


# ---------------------------------------------------------------- 通用 Yahoo 圖表代理（宏觀卡片/子圖：多 range/interval，含 prev close，允許清單＋快取）
EXT_ALLOWED = {"ES=F", "NQ=F", "YM=F", "RTY=F", "NKD=F", "NIY=F", "^KS11", "^KS200",
               "^N225", "^DJI", "^GSPC", "^IXIC", "^SOX", "^GDAXI",
               "TWD=X", "JPY=X",
               "DX-Y.NYB", "^TNX", "^VIX", "BTC-USD", "BZ=F", "CL=F"}
EXT_RANGES = {"1d", "5d", "1mo", "3mo", "6mo", "1y", "5y"}
EXT_INTERVALS = {"5m", "15m", "30m", "60m", "1d", "1wk"}
_ext_lock = threading.Lock()
_ext_cache = {}      # key "sym|range|interval" -> {"t": ts, "d": {...}}


def fetch_ext(sym, rng, iv):
    enc = urllib.parse.quote(sym, safe="")
    pp = "&includePrePost=true" if iv in ("5m", "15m", "30m", "60m") else ""
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/" + enc +
           "?interval=" + iv + "&range=" + rng + pp)
    d = json.loads(_http(url, headers={"User-Agent": UA}, timeout=15))
    r0 = d["chart"]["result"][0]
    ts = r0.get("timestamp") or []
    cl = (r0.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
    t2, c2 = [], []
    for a, b in zip(ts, cl):
        if b is not None:
            t2.append(a)
            c2.append(round(float(b), 4))
    m = r0.get("meta", {})
    return {"t": t2, "c": c2, "prev": m.get("chartPreviousClose"),
            "last": m.get("regularMarketPrice"), "cur": m.get("currency"),
            "sess": _yahoo_sessions(m)}


def get_ext(sym, rng, iv):
    key = sym + "|" + rng + "|" + iv
    gap = 60 if iv in ("5m", "15m", "30m", "60m") else 600
    with _ext_lock:
        c = _ext_cache.get(key)
        if c and time.time() - c["t"] < gap:
            return c["d"]
    try:
        d = fetch_ext(sym, rng, iv)
    except Exception:
        d = {"t": [], "c": [], "prev": None, "last": None, "cur": None}
    with _ext_lock:
        _ext_cache[key] = {"t": time.time(), "d": d}
    return d


# ---------------------------------------------------------------- 設定存取
_cfg_lock = threading.Lock()


def load_cfg():
    with _cfg_lock:
        if CONFIG_FILE.exists():
            try:
                return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            except Exception:
                pass
        return json.loads(json.dumps(DEFAULT_CFG))


def save_cfg(cfg):
    with _cfg_lock:
        CONFIG_FILE.write_text(
            json.dumps(cfg, ensure_ascii=False, indent=1), encoding="utf-8")


# ---------------------------------------------------------------- 前端頁面
HTML = r"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>台指期正逆價差即時監控</title>
<style>
:root { color-scheme: light; }
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: "Segoe UI", "Microsoft JhengHei", system-ui, sans-serif; background: #f4f6fa; color: #1a2233; padding: 14px; font-size: 14px; }
h1 { font-size: 19px; margin-bottom: 2px; }
.sub { color: #667; font-size: 12px; margin-bottom: 10px; }
.toolbar { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin-bottom: 12px; }
button { border: 1px solid #c5cede; background: #fff; border-radius: 7px; padding: 7px 14px; min-height: 34px; font-size: 13px; cursor: pointer; color: #1a2233; }
button:hover { background: #eef2f8; }
button.primary { background: #1f4e8c; color: #fff; border-color: #1f4e8c; }
button.primary:hover { background: #2a62ad; }
button.on { background: #e7f3e9; border-color: #5da46a; color: #2c6e3b; }
.badge { font-size: 11px; padding: 3px 9px; border-radius: 10px; background: #e3e9f2; color: #445; }
.badge.live { background: #e7f3e9; color: #2c6e3b; }
.badge.warn { background: #fdeeee; color: #a33; }
#status { font-size: 12px; color: #667; }
.alert { background: #fff7e6; border: 1px solid #e8c87a; color: #7a5b12; padding: 8px 12px; border-radius: 8px; margin-bottom: 12px; font-size: 12.5px; display: none; }
.ctxbar { display: none; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; font-size: 12.5px; align-items: center; }
.ctxbar .ctxitem { background: #fff; border: 1px solid #dde3ec; border-radius: 9px; padding: 5px 11px; }
.chip[title], .ctxitem[title], th[title] { cursor: help; border-bottom-style: dotted; }
.minigrid { display: flex; flex-wrap: wrap; gap: 10px; }
#miniGrid { display: flex; flex-wrap: wrap; gap: 4px 22px; align-items: flex-start; }
.minicat { margin-bottom: 8px; }
.minicat-h { font-size: 12px; font-weight: 600; color: #556; margin: 2px 0 6px; padding-bottom: 3px; border-bottom: 1px solid #eef1f6; }
.mini { width: 224px; border: 1px solid #eef1f6; border-radius: 9px; padding: 8px 10px 5px; background: #fdfdfe; }
@media (max-width: 820px) { #miniGrid { display: block; } .minicat { width: 100%; } .mini { width: calc(50% - 6px); } }
.mini .mt { display: flex; justify-content: space-between; font-size: 12px; color: #445; margin-bottom: 4px; gap: 6px; }
.mini .mt .mv { font-variant-numeric: tabular-nums; white-space: nowrap; }
.mini canvas { width: 100%; height: 130px; display: block; cursor: crosshair; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 12px; margin-bottom: 14px; }
.card { background: #fff; border: 1px solid #dde3ec; border-radius: 11px; padding: 13px 15px; overflow-x: auto; }
.card h2 { font-size: 15px; display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 8px; }
.card h2 .src { font-size: 10.5px; color: #889; font-weight: normal; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: right; color: #667; font-weight: 500; padding: 4px 5px; border-bottom: 1px solid #e7ebf2; font-size: 12px; }
th:first-child, td:first-child { text-align: left; }
td { text-align: right; padding: 5px 5px; border-bottom: 1px solid #f0f3f8; font-variant-numeric: tabular-nums; white-space: nowrap; }
tr:last-child td { border-bottom: none; }
.pos { color: #c92a2a; font-weight: 600; }
.neg { color: #2b8a3e; font-weight: 600; }
.big { font-size: 15px; }
.dim { color: #99a; }
.chg { font-size: 10.5px; display: block; font-weight: 500; }
.spotline { font-size: 12.5px; color: #556; margin-bottom: 7px; }
.sig { font-size: 11.5px; margin-top: 8px; padding: 5px 9px; border-radius: 7px; background: #f2f5fa; color: #445; line-height: 1.7; }
.chip { display: inline-block; font-size: 10.5px; background: #fff; border: 1px solid #dde3ec; border-radius: 9px; padding: 1px 7px; margin-left: 5px; }
.panel { background: #fff; border: 1px solid #dde3ec; border-radius: 11px; padding: 14px 16px; margin-bottom: 14px; }
.panel h3 { font-size: 14px; margin-bottom: 8px; }
textarea { width: 100%; height: 220px; font-family: Consolas, monospace; font-size: 12px; border: 1px solid #c5cede; border-radius: 7px; padding: 8px; }
.panel .row { display: flex; gap: 10px; align-items: center; margin: 8px 0; flex-wrap: wrap; }
input[type=number], input[type=text], input[type=password] { border: 1px solid #c5cede; border-radius: 6px; padding: 5px 8px; width: 120px; font-size: 13px; }
.tabs { display: flex; gap: 6px; margin-bottom: 8px; flex-wrap: wrap; align-items: center; }
.tabs button.sel { background: #1f4e8c; color: #fff; border-color: #1f4e8c; }
.tabs .sep { width: 1px; height: 20px; background: #dde3ec; margin: 0 4px; }
.note { font-size: 11.5px; color: #778; margin-top: 8px; line-height: 1.6; }
#err { white-space: pre-wrap; }
.hidden { display: none; }
#hcv { width: 100%; height: 380px; display: block; cursor: crosshair; border: 1px solid #eef1f6; border-radius: 8px; background: #fdfdfe; touch-action: none; }
.hint { font-size: 11px; color: #99a; margin-top: 5px; }
.macrowrap { display: flex; gap: 12px; align-items: flex-start; margin-bottom: 14px; }
.maincol { flex: 1 1 auto; min-width: 0; }
.maincol > .grid { margin-bottom: 12px; }
.maincol > .grid:last-child { margin-bottom: 0; }
#sidePanel { flex: 0 0 244px; max-width: 264px; background: #fff; border: 1px solid #dde3ec; border-radius: 11px; padding: 8px 11px; }
#sidePanel h3 { font-size: 13px; margin-bottom: 6px; }
.sp-row { border-bottom: 1px solid #f0f3f8; padding: 6px 2px; cursor: pointer; }
.sp-row:last-child { border-bottom: none; }
.sp-row:hover { background: #f7f9fc; }
.sp-top { display: flex; justify-content: space-between; align-items: baseline; gap: 6px; font-size: 12.5px; }
.sp-name { color: #445; }
.sp-px { font-variant-numeric: tabular-nums; white-space: nowrap; }
.sp-sub { font-size: 10.5px; color: #889; margin-top: 1px; display: flex; justify-content: space-between; }
.ic-head { display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 13px; }
.ic-name { flex: 1; color: #1a2233; font-weight: 600; }
.ic-val { font-variant-numeric: tabular-nums; white-space: nowrap; }
.ic-tg { color: #99a; width: 12px; text-align: center; }
.ic-body { margin-top: 6px; }
.ic-ranges { display: flex; gap: 3px; flex-wrap: wrap; margin-bottom: 4px; }
.ic-rbtn { border: 1px solid #c5cede; background: #fff; border-radius: 5px; padding: 1px 7px; min-height: 0; font-size: 10.5px; cursor: pointer; color: #1a2233; }
.ic-rbtn.sel { background: #1f4e8c; color: #fff; border-color: #1f4e8c; }
.ic-cv { width: 100%; height: 168px; display: block; cursor: crosshair; }
.ic-hint { font-size: 10px; color: #aab; margin-top: 2px; }
@media (max-width: 980px) { .macrowrap { flex-direction: column; } #sidePanel { flex: 1 1 auto; max-width: none; width: 100%; } }
#macroCards { grid-template-columns: repeat(5, minmax(0, 1fr)); }
@media (max-width: 1400px) { #macroCards { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
@media (max-width: 760px) { #macroCards { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
.chartrow { --leftw: 860px; display: flex; gap: 0; align-items: flex-start; }
.chartleft { flex: 0 0 var(--leftw); display: flex; gap: 12px; min-width: 0; align-items: flex-start; }
.chartleft .txcol { flex: 1 1 auto; grid-template-columns: 1fr; margin-bottom: 0; min-width: 0; }
.chartleft #sidePanel { flex: 0 0 300px; max-width: 320px; }
.chartsplit { flex: 0 0 8px; align-self: stretch; min-height: 380px; margin: 0 8px; cursor: col-resize; background: #e3e9f2; border-radius: 4px; }
.chartsplit:hover { background: #b9c4d8; }
.chartmain { flex: 1 1 auto; min-width: 0; }
@media (max-width: 1000px) { .chartrow { flex-direction: column; } .chartleft { flex: 1 1 auto; width: 100%; flex-direction: column; } .chartleft #sidePanel { flex: 1 1 auto; max-width: none; width: 100%; } .chartsplit { display: none; } }
/* ===== 放大重要資訊字級＋左側兩卡填滿空白（UI/UX 微調） ===== */
body { font-size: 15px; }
.card h2 { font-size: 17px; }
table { font-size: 14.5px; }
th { font-size: 13px; padding: 6px 5px; }
td { padding: 8px 5px; }
.big { font-size: 22px; }
.chg { font-size: 12px; }
.spotline { font-size: 15.5px; margin-bottom: 9px; }
.sig { font-size: 13.5px; padding: 7px 11px; }
.chip { font-size: 12px; padding: 2px 8px; }
.ctxbar { font-size: 13.5px; }
.mini .mt { font-size: 13px; }
.ic-head { font-size: 14.5px; }
.ic-val { font-size: 14px; }
#hcv { height: 330px; }
.chartsplit { min-height: 310px; }
.chartrow { align-items: flex-start; }
.chartleft { align-items: flex-start; }
.chartleft .card h2 { font-size: 19px; margin-bottom: 6px; }
.chartleft .card table { font-size: 16px; }
.chartleft .card th { font-size: 13.5px; padding: 5px 6px; }
.chartleft .card td { padding: 8px 6px; }
.chartleft .card .big { font-size: 25px; }
.chartleft .card .spotline { font-size: 17px; margin-bottom: 6px; }
.chartleft .card .sig { font-size: 13.5px; line-height: 1.6; margin-top: 6px; }
.chartleft #sidePanel h3 { font-size: 15px; margin-bottom: 6px; }
.chartleft #sidePanel .sp-top { font-size: 15.5px; }
.chartleft #sidePanel .sp-sub { font-size: 12.5px; }
.chartleft #sidePanel .sp-row { padding: 9px 4px; }

/* ============ 行動版全面優化（≤640px；對桌面零影響） ============ */
html { -webkit-text-size-adjust: 100%; }
@media (max-width: 640px) {
  body { padding: 8px 8px 30px; font-size: 14px; }
  h1 { font-size: 18px; }
  .sub { font-size: 11.5px; }
  /* 觸控目標放大：工具列按鈕好按 */
  .toolbar button, .toolbar .btn, .tabs button {
    min-height: 38px; padding: 7px 12px; font-size: 14px;
  }
  .toolbar { gap: 6px; }
  /* 報價表：改堆疊卡片式——每個合約一個區塊、欄位直排帶標籤，免橫向滑動 */
  #cards table, #cards table tbody { display: block; }
  #cards table tr { display: block; border: 1px solid #e2e8f2; border-radius: 9px;
    padding: 6px 10px 5px; margin: 0 0 8px; background: #fdfdfe; }
  #cards table tr:first-child { display: none; }   /* 表頭列改由各值前置標籤取代 */
  #cards table td { display: flex; justify-content: space-between; align-items: baseline;
    gap: 8px; padding: 3.5px 0; border-bottom: none; text-align: right; font-size: 13.5px; }
  #cards table td:first-child { display: block; text-align: left; font-weight: 600;
    border-bottom: 1px dashed #e2e8f2; padding-bottom: 6px; margin-bottom: 4px; }
  #cards table td::before { color: #778; font-size: 11.5px; font-weight: 400; flex: 0 0 auto; }
  #cards table td:first-child::before { content: none; }
  #cards table td:nth-child(2)::before { content: "期貨價"; }
  #cards table td:nth-child(3)::before { content: "表面價差"; }
  #cards table td:nth-child(4)::before { content: "未除息"; }
  #cards table td:nth-child(5)::before { content: "還原價差"; }
  #cards table td:nth-child(5) { font-size: 16px; font-weight: 600; }
  /* 宏觀卡改單欄，標題/數值不再直排擠壓 */
  #macroCards { grid-template-columns: 1fr !important; }
  .ic-head { flex-wrap: nowrap; }
  .ic-name { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .ic-val { font-size: 15px; }
  /* 桌面操作提示（拖曳/滾輪）在觸控裝置只佔空間，隱藏 */
  .hint, .ic-hint { display: none; }
  /* 主歷史圖高度收斂，留更多視野給下方內容 */
  #hcv { height: 300px; }
  /* 盤中子圖：極窄螢幕改單欄 */
  .card { padding: 10px 10px; }
  /* 盤中子圖：標題單行省略不再多行換行、數值獨立一行、圖形收斂為 16:9 左右 */
  .mini { padding: 6px 9px 3px; }
  .mini .mt { flex-direction: column; align-items: stretch; gap: 0; margin-bottom: 2px; }
  .mini .mt > span:first-child { white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    font-size: 11px; color: #667; }
  .mini .mt .mv { font-size: 13.5px; font-weight: 600; }
  .mini canvas { height: 96px; }
  /* iOS 聚焦自動放大會打亂版面：表單字級固定 16px */
  input[type=number], input[type=text], input[type=password], select, textarea { font-size: 16px; }
}
@media (max-width: 360px) {
  .mini { width: 100% !important; }
  .mini canvas { height: 110px; }
}
@media (pointer: coarse) {
  .hint, .ic-hint { display: none; }
}
</style>
</head>
<body>
<h1>台指期正逆價差即時監控</h1>
<div class="sub">期現貨價差 ＋ 還原除息後真實價差｜TX・TE・TF・XIF（期交所）＋ TWN 富台期（SGX 延遲）｜LAN 版 build 0617a</div>

<div class="toolbar">
  <button class="primary" id="btnRefresh" onclick="refresh()">立即更新</button>
  <button id="btnAuto" onclick="toggleAuto()">自動更新：開</button>
  <button onclick="openParams()">參數設定</button>
  <span class="badge" id="srcBadge">尚未更新</span>
  <span id="status"></span>
  <span class="badge" id="metaBadge" title="伺服器識別碼｜累積快照筆數｜最後一筆時間。不同電腦顯示相同識別碼=連到同一台伺服器；筆數/時間相同=已同步">—</span>
</div>

<div class="alert" id="staleAlert"></div>

<div class="alert" id="err" style="background:#fdeeee;border-color:#dba6a6;color:#a33"></div>
<div class="ctxbar" id="ctxBar"></div>
<div class="ctxbar" id="refBar"></div>

<div class="grid" id="macroCards" style="margin-bottom:14px"></div>

<div class="panel">
  <h3>台指期價差圖表</h3>
  <div class="chartrow">
    <div class="chartleft">
      <div class="grid txcol" id="cards"><div class="card"><div class="dim">載入即時報價…</div></div></div>
      <div id="sidePanel">
        <h3>其他期貨 <span class="dim" style="font-size:10.5px;font-weight:normal">電子/金融/非金電/富台・點載入下圖</span></h3>
        <div id="sideList"></div>
      </div>
    </div>
    <div class="chartsplit" id="chartSplit" title="拖曳調整左右寬度"></div>
    <div class="chartmain">
      <div class="tabs" id="histTabs"></div>
      <canvas id="hcv"></canvas>
      <div class="hint">拖曳圖面＝平移｜拖曳下方時間軸＝縮放時間刻度｜拖曳右側價格軸＝縮放Y軸｜滾輪＝縮放｜雙擊＝重置｜拖曳左側灰色分隔條＝調整左右寬度</div>
    </div>
  </div>
</div>

<div class="panel">
  <h3>盤中子圖表 <span class="dim" style="font-size:11.5px;font-weight:normal">台灣商品＝當天開盤起（指數 08:45–13:45、台幣 09:00–16:00）｜美股/亞股/DXY/債息/VIX＝各自最近交易時段（台北時間）｜滑鼠移動可查數值</span></h3>
  <div id="miniGrid"></div>
</div>

<div class="panel hidden" id="cfgPanel">
  <h3>參數設定（儲存於伺服器，全區網共用）</h3>
  <div style="border:1px solid #e7ebf2;border-radius:8px;padding:10px 12px;margin-bottom:12px;background:#fbfcfe">
    <h3 style="font-size:13px;margin-bottom:6px">自訂子圖表代號（最多 8 個）</h3>
    <p class="note" style="margin-top:0">台股直接輸入數字代號（例如 <b>2330</b>、<b>0050</b>）；美股輸入英文代號（例如 <b>nvda</b>、<b>aapl</b>）。留空表示不顯示。儲存後子圖表會立即更新。</p>
    <div class="row">
      <input type="text" id="tk0" placeholder="如 2330" style="width:110px">
      <input type="text" id="tk1" placeholder="如 nvda" style="width:110px">
      <input type="text" id="tk2" placeholder="代號" style="width:110px">
      <input type="text" id="tk3" placeholder="代號" style="width:110px">
      <input type="text" id="tk4" placeholder="代號" style="width:110px">
      <input type="text" id="tk5" placeholder="代號" style="width:110px">
      <input type="text" id="tk6" placeholder="代號" style="width:110px">
      <input type="text" id="tk7" placeholder="代號" style="width:110px">
      <button class="primary" onclick="saveTickers()">儲存代號</button>
      <span id="tkMsg" style="font-size:12px;color:#2c6e3b"></span>
    </div>
  </div>
  <div style="border:1px solid #e7ebf2;border-radius:8px;padding:10px 12px;margin-bottom:12px;background:#fbfcfe">
    <h3 style="font-size:13px;margin-bottom:6px">卡片／子圖表標的設定（下拉切換，儲存於伺服器）</h3>
    <p class="note" style="margin-top:0">每個位置可從清單挑選顯示的標的。上方 5 張宏觀卡片僅支援國際（Yahoo）標的；下方子圖表可選台股、美股期／現貨、匯率、商品、債息、加密等。儲存後立即重建。</p>
    <div id="slotCfg"></div>
    <div class="row" style="margin-top:8px">
      <button class="primary" onclick="saveSlots()">儲存標的設定</button>
      <button onclick="resetSlots()">回復預設</button>
      <span id="slotMsg" style="font-size:12px;color:#2c6e3b"></span>
    </div>
  </div>
  <div style="border:1px solid #e7ebf2;border-radius:8px;padding:10px 12px;margin-bottom:12px;background:#fbfcfe">
    <h3 style="font-size:13px;margin-bottom:6px">對照行情列（頂部橫幅，預設隱藏）</h3>
    <label style="display:block;margin:2px 0 8px"><input type="checkbox" id="rbOn"> 顯示對照行情列</label>
    <p class="note" style="margin:0 0 4px">勾選要顯示的標準項目：</p>
    <div id="rbStd" class="row" style="flex-wrap:wrap;gap:4px 14px"></div>
    <p class="note" style="margin:8px 0 2px">手動加入 ticker（台股輸入數字如 <b>2330</b>，自動轉 Yahoo 2330.TW；美股如 <b>nvda</b>；逗號或空白分隔）：</p>
    <input type="text" id="rbAdd" placeholder="如 2330, nvda, GC=F" style="width:100%">
    <div class="row" style="margin-top:8px">
      <button class="primary" onclick="saveRefBar()">儲存對照行情設定</button>
      <span id="rbMsg" style="font-size:12px;color:#2c6e3b"></span>
    </div>
  </div>
  <h3 style="font-size:13px;margin-bottom:6px">除息資料設定</h3>
  <div class="row">
    <label>富台（TWN）現貨基準（SGX 無免費現貨源；輸入今日現貨值即重新校準，平時自動以 TAIEX×比例推估到當日）：</label>
    <input type="number" id="twnSpot" step="0.01">
    <button onclick="saveTwnSpot()">儲存</button>
  </div>
  <div class="row">
    <label>資金成本年利率 %（理論持有成本＝現貨×利率×剩餘天數/365）：</label>
    <input type="number" id="costRate" step="0.05">
    <button onclick="saveRate()">儲存</button>
  </div>
  <p class="note">下方 JSON 為除息點數設定（基準日 seedDate、各合約月份尚未除息點數 seedRemaining、未來每日除息明細 daily）。每月初取得新的除息預估報告（如富邦「除息影響點數分析」）後，依格式更新並儲存。daily 每列格式：[日期, TX, TE, TF, XIF, TWN]（正數，代表當日指數除息蒸發點數）。</p>
  <textarea id="cfgText"></textarea>
  <div class="row">
    <button class="primary" onclick="saveCfg()">儲存設定</button>
    <button onclick="resetCfg()">還原預設（2026/6/11 富邦報告）</button>
    <span id="cfgMsg" style="font-size:12px;color:#2c6e3b"></span>
  </div>
</div>

<div class="panel">
  <h3>判讀說明</h3>
  <p class="note">
    還原價差 ＝ 期貨價 − 現貨價 ＋ 該合約到期前尚未除息點數。除息季現貨指數會因除息機械性下跌、期貨已預先反映，故表面價差會低估真實基差。
    <span class="pos">紅色（還原正價差）</span>＝期貨相對現貨溢價，市場偏多；<span class="neg">綠色（還原逆價差）</span>＝貼水，偏空或避險賣壓重。
    報價旁箭頭為今日漲跌（相對昨日收盤／結算價，▲▼點數與百分比，紅漲綠跌）；價差動量晶片顯示「較5分前」與「今日盤初以來」變化。
    台指期結算日為每月第三個週三；富台期為每月倒數第二個交易日。除息點數為預估值，實際以各公司公告為準。
    歷史圖表：日線為近月連續期貨收盤−現貨收盤之表面價差（除息還原不適用於歷史）；快照為本伺服器累積之還原價差。本工具僅供參考，不構成投資建議。
  </p>
</div>

<script>
"use strict";
const PRODUCTS = ["TX","TE","TF","XIF","TWN"];
const PROD_NAMES = {TX:"台股期貨 TX", TE:"電子期貨 TE", TF:"金融期貨 TF", XIF:"非金電期貨 XIF", TWN:"富台期貨 TWN"};
const TAIFEX_CID = {TX:"TXF", TE:"EXF", TF:"FXF", XIF:"XIF"};
const CNYES_FUT = {TX:"TWF:TXF:FUTURES", TE:"TWF:EXF:FUTURES", TF:"TWF:FXF:FUTURES", XIF:"TWF:XIF:FUTURES"};
const CNYES_IDX = {TX:"TWS:TSE01:INDEX", TE:"TWS:TSE27:INDEX", TF:"TWS:TSE31:INDEX", XIF:"TWS:TSE34:INDEX"};

function thirdWednesday(year, month) {
  const d = new Date(Date.UTC(year, month - 1, 1));
  const dow = d.getUTCDay();
  const offset = (3 - dow + 7) % 7;
  return year + "-" + String(month).padStart(2,"0") + "-" + String(1 + offset + 14).padStart(2,"0");
}
const TAIFEX_M = "ABCDEFGHIJKL";
const SGX_M = {F:1,G:2,H:3,J:4,K:5,M:6,N:7,Q:8,U:9,V:10,X:11,Z:12};

function parseTaifexSym(sym) {
  const m = sym.match(/^[A-Z]{3}([A-L])(\d)-F$/);
  if (!m) return null;
  const mon = TAIFEX_M.indexOf(m[1]) + 1;
  const yr = 2020 + parseInt(m[2]);
  return { ym: yr + "-" + String(mon).padStart(2,"0"), settle: thirdWednesday(yr, mon) };
}
function parseSgxSym(sym) {
  const m = sym.match(/^TWN([FGHJKMNQUVXZ])(\d\d)$/);
  if (!m) return null;
  return { ym: (2000 + parseInt(m[2])) + "-" + String(SGX_M[m[1]]).padStart(2,"0") };
}

function remainingDiv(cfg, prodIdx, contractYM, settleDate, todayStr) {
  const seed = cfg.seedRemaining[contractYM];
  if (seed == null) return null;
  let r = seed[prodIdx];
  for (const row of cfg.daily) {
    const d = row[0];
    if (d > cfg.seedDate && d <= todayStr && d <= settleDate) r -= row[prodIdx + 1];
  }
  return Math.round(Math.max(r, 0) * 100) / 100;
}
function num(v) {
  if (v === null || v === undefined || v === "" ) return null;
  const n = parseFloat(v);
  return isNaN(n) ? null : n;
}

function buildQuotes(raw, todayStr) {
  const out = {};
  for (const p of ["TX","TE","TF","XIF"]) {
    const cid = TAIFEX_CID[p];
    let spot = null, spotChg = null, spotPct = null, near = null, next = null, src = null, qTime = null;
    const rows = (raw.taifex && raw.taifex[cid]) || [];
    if (rows.length) {
      src = "期交所MIS";
      const spotRow = rows.find(r => r.sym.endsWith("-S"));
      if (spotRow) {
        spot = num(spotRow.last) ?? num(spotRow.ref);
        const ref = num(spotRow.ref);
        if (spot != null && ref != null && num(spotRow.last) != null) {
          spotChg = Math.round((spot - ref) * 100) / 100;
          spotPct = ref ? Math.round(spotChg / ref * 10000) / 100 : null;
        }
        const ds = String(spotRow.date || ""), ts2 = String(spotRow.time || "");
        qTime = (ds.length === 8 && ts2.length >= 4)
          ? ds.slice(4, 6) + "/" + ds.slice(6, 8) + " " + ts2.slice(0, 2) + ":" + ts2.slice(2, 4)
          : (ds + " " + ts2).trim();
      }
      const futs = rows.filter(r => !r.sym.endsWith("-S")).map(r => {
        const info = parseTaifexSym(r.sym);
        let px = num(r.last), isTest = false;
        if (px == null && num(r.test) != null) { px = num(r.test); isTest = true; }
        if (px == null) { const b = num(r.bid), a = num(r.ask); if (b != null && a != null) px = (b + a) / 2; }
        if (px == null) px = num(r.ref);
        let chg = null, pct = null;
        const ref = num(r.ref);
        if (px != null && ref != null) { chg = Math.round((px - ref) * 100) / 100; pct = ref ? Math.round(chg / ref * 10000) / 100 : null; }
        return info ? { ...info, px, chg, pct, vol: num(r.vol), isTest, sym: r.sym } : null;
      }).filter(x => x && x.px != null && x.settle >= todayStr).sort((a, b) => a.ym.localeCompare(b.ym));
      near = futs[0] || null; next = futs[1] || null;
    } else if (raw.cnyes) {
      src = "鉅亨備援";
      const f = raw.cnyes[CNYES_FUT[p]], i = raw.cnyes[CNYES_IDX[p]];
      if (i) { spot = num(i.last); spotChg = num(i.chg); spotPct = num(i.chgPct); }
      if (f && num(f.last) != null) {
        qTime = new Date((f.t || 0) * 1000).toLocaleString("zh-TW", { timeZone: "Asia/Taipei", hour12: false });
        let y = +todayStr.slice(0,4), m = +todayStr.slice(5,7);
        if (todayStr > thirdWednesday(y, m)) { m++; if (m > 12) { m = 1; y++; } }
        const ym = y + "-" + String(m).padStart(2,"0");
        near = { ym, settle: thirdWednesday(y, m), px: num(f.last), chg: num(f.chg), pct: num(f.chgPct), sym: "連續近月" };
      }
    }
    out[p] = { spot, spotChg, spotPct, near, next, src, qTime };
  }
  {
    const futs = (raw.sgx || []).map(x => {
      const info = parseSgxSym(x.sym);
      let px = num(x.last);
      if (px == null) { const b = num(x.bid), a = num(x.ask); if (b != null && a != null) px = (b + a) / 2; }
      // SGX change-abs 為絕對值，正負號以 change-percentage 為準
      let chg = num(x.chg), pctv = num(x.chgPct);
      if (chg != null && pctv != null) chg = (pctv < 0 ? -1 : (pctv > 0 ? 1 : 0)) * Math.abs(chg);
      return info ? { ym: info.ym, settle: x.ltd, px, chg, pct: pctv != null ? Math.round(pctv * 100) / 100 : null, sym: x.sym, upd: x.upd } : null;
    }).filter(x => x && x.px != null && x.settle >= todayStr).sort((a, b) => a.settle.localeCompare(b.settle));
    out.TWN = { spot: null, spotChg: null, spotPct: null, near: futs[0] || null, next: futs[1] || null, src: "SGX延遲", qTime: futs[0] ? futs[0].upd : null };
  }
  return out;
}

// 富台（TWN）現貨：有校準比例時以 TAIEX×比例即時推估（自動跟到當日），否則退回最後手動值
function twnSpotEst(cfg, taiex) {
  if (!cfg) return null;
  if (cfg.twnSpotRatio && taiex != null) return Math.round(taiex * cfg.twnSpotRatio * 100) / 100;
  return cfg.twnSpot != null ? cfg.twnSpot : null;
}

function computeAll(quotes, cfg, todayStr) {
  const res = {};
  PRODUCTS.forEach((p, idx) => {
    const q = quotes[p];
    if (!q) { res[p] = null; return; }
    const spot = (p === "TWN") ? twnSpotEst(cfg, (quotes.TX && quotes.TX.spot != null) ? quotes.TX.spot : null) : q.spot;
    const r = { spot, spotChg: q.spotChg, spotPct: q.spotPct, src: q.src, qTime: q.qTime, near: null, next: null, calSpread: null, calAdj: null };
    for (const leg of ["near", "next"]) {
      const c = q[leg];
      if (!c) continue;
      const rem = remainingDiv(cfg, idx, c.ym, c.settle, todayStr);
      const raw = (spot != null) ? Math.round((c.px - spot) * 100) / 100 : null;
      const adj = (raw != null && rem != null) ? Math.round((raw + rem) * 100) / 100 : null;
      r[leg] = { ym: c.ym, settle: c.settle, px: c.px, chg: c.chg, pct: c.pct, vol: c.vol, isTest: c.isTest, raw, rem, adj, sym: c.sym };
    }
    if (r.near && r.next) {
      r.calSpread = Math.round((r.next.px - r.near.px) * 100) / 100;
      if (r.near.rem != null && r.next.rem != null)
        r.calAdj = Math.round((r.calSpread + r.next.rem - r.near.rem) * 100) / 100;
    }
    res[p] = r;
  });
  return res;
}

// ---------- 狀態 ----------
let cfg = null;
let autoOn = true, timer = null, lastResult = null, lastRaw = null;
let snaps = [];        // 伺服器快照（共用歷史）
let lastSnapT = 0;
let es = null;         // SSE 即時推播連線

function todayTW() {
  return new Intl.DateTimeFormat("sv-SE", { timeZone: "Asia/Taipei" }).format(new Date());
}
function dateOfTs(ts) {
  return new Intl.DateTimeFormat("sv-SE", { timeZone: "Asia/Taipei" }).format(new Date(ts * 1000));
}

async function loadCfgFromServer() {
  cfg = await (await fetch("/api/config")).json();
}
async function postCfg(body) {
  const r = await fetch("/api/config", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  cfg = await r.json();
}
// SSE：伺服器一記錄新快照就推來，立即併入並重繪（與 60 秒輪詢互補、去重）
function startStream() {
  if (es) { try { es.close(); } catch (_) {} }
  try {
    es = new EventSource("/api/stream");
    es.onmessage = (ev) => {
      let e = null;
      try { e = JSON.parse(ev.data); } catch (_) { return; }
      if (!e || !e.t) return;
      if (snaps.length && e.t <= snaps[snaps.length - 1].t) return;   // 去重
      snaps.push(e); lastSnapT = e.t;
      if (hist.res === "snap" && !hist.drag) { hist.dirty = true; loadHist(); }
      if (typeof refreshMeta === "function") refreshMeta();
    };
    es.onerror = () => {};   // 瀏覽器會自動重連
  } catch (_) {}
}
// 診斷狀態列：顯示伺服器識別碼＋累積快照筆數＋最後一筆時間，供各電腦比對是否同一台/是否同步
async function refreshMeta() {
  try {
    const m = await (await fetch("/api/meta")).json();
    const el = document.getElementById("metaBadge");
    if (!el) return;
    const last = m.lastSnapT ? new Date(m.lastSnapT * 1000).toLocaleTimeString("zh-TW", { timeZone: "Asia/Taipei", hour12: false }) : "—";
    el.textContent = "伺服器#" + m.boot + "｜快照 " + m.snapCount + " 筆｜最後 " + last;
  } catch (e) {}
}
async function pullSnaps() {
  try {
    const d = await (await fetch("/api/snapshots?after=" + lastSnapT)).json();
    if (d.snaps && d.snaps.length) {
      snaps = snaps.concat(d.snaps);
      lastSnapT = snaps[snaps.length - 1].t;
    }
  } catch (e) {}
}

// 由快照計算某商品的還原近月價差序列
function snapSeries(p) {
  const idx = PRODUCTS.indexOf(p);
  const t = [], adj = [], raw = [];
  for (const s of snaps) {
    const e = s.p[p];
    if (!e || e.n == null) continue;
    if (!inCashSession(s.t)) continue;   // 只取現貨指數真正交易的時段(09:00–13:30)，排除盤前(昨收當現貨)造成的假價差
    const spot = (p === "TWN") ? twnSpotEst(cfg, (s.p.TX && s.p.TX.s != null) ? s.p.TX.s : null) : e.s;
    if (spot == null) continue;
    const rw = e.n - spot;
    if (Math.abs(rw) > spot * 0.03) continue;
    let a = null;
    if (cfg && e.ny && e.ns) {
      const rem = remainingDiv(cfg, idx, e.ny, e.ns, dateOfTs(s.t));
      if (rem != null) a = rw + rem;
    }
    t.push(s.t); raw.push(Math.round(rw * 100) / 100); adj.push(a != null ? Math.round(a * 100) / 100 : null);
  }
  return { t, adj, raw };
}

// 價差動量（較5分前、今日盤初以來）
function momentum(p) {
  const ser = snapSeries(p);
  if (ser.t.length < 2) return null;
  const nowT = ser.t[ser.t.length - 1];
  const cur = ser.adj[ser.adj.length - 1] ?? ser.raw[ser.raw.length - 1];
  if (cur == null) return null;
  let m5 = null, day0 = null;
  const today = dateOfTs(nowT);
  for (let i = ser.t.length - 2; i >= 0; i--) {
    if (nowT - ser.t[i] >= 300) {
      if (nowT - ser.t[i] <= 900) {
        const v = ser.adj[i] ?? ser.raw[i];
        if (v != null) m5 = Math.round((cur - v) * 100) / 100;
      }
      break;
    }
  }
  for (let i = 0; i < ser.t.length; i++) {
    if (dateOfTs(ser.t[i]) === today) {
      const v = ser.adj[i] ?? ser.raw[i];
      if (v != null) day0 = Math.round((cur - v) * 100) / 100;
      break;
    }
  }
  return { m5, day0 };
}

// ---------- 抓取 ----------
async function refresh() {
  const btn = document.getElementById("btnRefresh");
  btn.disabled = true; btn.textContent = "更新中…";
  setStatus("抓取報價中…");
  try {
    if (!cfg) await loadCfgFromServer();
    const resp = await fetch("/api/quotes");
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    const raw = await resp.json();
    lastRaw = raw;
    await pullSnaps();
    refreshMacroHeaders();
    refreshMeta();
    const tStr = todayTW();
    const quotes = buildQuotes(raw, tStr);
    lastResult = computeAll(quotes, cfg, tStr);
    // 富台現貨：若尚無校準比例，首次以「最後手動值÷當前 TAIEX」自動校準並存檔，之後即自動跟到當日
    if (cfg && !cfg.twnSpotRatio && cfg.twnSpot && quotes.TX && quotes.TX.spot) {
      const ratio = Math.round(cfg.twnSpot / quotes.TX.spot * 1e6) / 1e6;
      await postCfg({ ...cfg, twnSpotRatio: ratio, twnSpotNote: "自動校準 " + tStr + "（TAIEX×" + ratio + "）" });
      lastResult = computeAll(quotes, cfg, tStr);
    }
    await loadContext();
    for (const pp of ["TX", "TE", "TF", "XIF"]) await ensureDaily(pp);
    render(lastResult, raw);
    renderCtxBar(lastResult);
    renderRefBar(raw);
    loadMinis();
    if (hist.res === "snap" || hist.res === "m5") { hist.dirty = true; loadHist(); }
    setStatus("最後更新：" + new Date().toLocaleTimeString("zh-TW", { timeZone: "Asia/Taipei", hour12: false }));
    setErr((raw.err && raw.err.length) ? "部分來源異常：" + raw.err.join("；") : "");
  } catch (e) {
    setStatus("更新失敗");
    setErr("連線錯誤：" + (e.message || e) + "（將於下次自動更新時重試）");
  }
  btn.disabled = false; btn.textContent = "立即更新";
}
function setStatus(s) { document.getElementById("status").textContent = s; }
function setErr(msg) {
  const el = document.getElementById("err");
  el.textContent = msg || "";
  el.style.display = msg ? "block" : "none";
}

// ---------- 渲染 ----------
function fmt(v, dec = 2) { return v == null ? "—" : v.toLocaleString("zh-TW", { minimumFractionDigits: dec, maximumFractionDigits: dec }); }
function cls(v) { return v == null ? "dim" : (v > 0 ? "pos" : (v < 0 ? "neg" : "")); }
function arrowHtml(chg, pct) {
  if (chg == null) return "";
  const a = chg > 0 ? "▲" : (chg < 0 ? "▼" : "—");
  const c = cls(chg);
  const pctStr = pct != null ? "（" + (pct > 0 ? "+" : "") + pct.toFixed(2) + "%）" : "";
  return "<span class='chg " + c + "'>" + a + fmt(Math.abs(chg)) + pctStr + "</span>";
}
function chipHtml(label, v, tip) {
  if (v == null) return "";
  const a = v > 0 ? "▲" : (v < 0 ? "▼" : "±");
  return "<span class='chip'" + (tip ? " title='" + tip + "'" : "") + ">" + label + " <span class='" + cls(v) + "'>" + a + fmt(Math.abs(v), 1) + "</span></span>";
}

function render(res, raw) {
  const tStr = todayTW();
  const usedTaifex = raw.taifex && Object.keys(raw.taifex).length > 0;
  const badge = document.getElementById("srcBadge");
  const nowTW2 = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
  const hm = nowTW2.getHours() * 100 + nowTW2.getMinutes(), wd = nowTW2.getDay();
  let sess = "收盤中";
  if (wd >= 1 && wd <= 5 && hm >= 845 && hm <= 1345) sess = "日盤";
  else if ((wd >= 1 && wd <= 5 && hm >= 1500) || (wd >= 2 && wd <= 6 && hm < 500)) sess = "夜盤";
  // 現貨加權僅 09:00–13:30 交易；此區間外現貨為「已收盤定盤值/昨收」，而期貨仍在跑（日盤尾盤 13:30–13:45、夜盤），
  // 此時還原價差是「跳動期貨 對 靜止現貨」，常因尾盤集合競價跳動而放大，須提示參考性下降
  const cashOpen = (wd >= 1 && wd <= 5 && hm >= 900 && hm <= 1330);
  const cashClosedLive = !cashOpen && (sess === "日盤" || sess === "夜盤");
  badge.textContent = (usedTaifex ? "期交所MIS ＋ SGX" : "鉅亨備援 ＋ SGX（期交所暫不可用）") + "｜" + sess;
  badge.className = "badge " + (usedTaifex ? "live" : "warn");
  const alert = document.getElementById("staleAlert");
  const ageDays = Math.floor((new Date(tStr) - new Date(cfg.seedDate)) / 86400000);
  const lastDaily = cfg.daily.length ? cfg.daily[cfg.daily.length - 1][0] : cfg.seedDate;
  if (tStr > lastDaily || ageDays > 14) {
    alert.style.display = "block";
    alert.textContent = "⚠ 除息資料基準日為 " + cfg.seedDate + "（每日明細至 " + lastDaily + "），已超出涵蓋範圍或超過兩週。還原價差可能失準，請至「除息資料設定」更新最新除息預估數據。";
  } else alert.style.display = "none";

  const cards = document.getElementById("cards");
  cards.innerHTML = "";
  for (const p of ["TX"]) {                 // 只在主區顯示台指期 TX；其餘改放右側宏觀面板
    const r = res[p];
    const card = document.createElement("div");
    card.className = "card";
    if (!r || (!r.near && !r.next)) {
      card.innerHTML = "<h2>" + PROD_NAMES[p] + "</h2><div class='dim'>無報價資料</div>";
      cards.appendChild(card);
      continue;
    }
    const spotLabel = (p === "TWN") ? (cfg.twnSpotRatio ? "現貨（TAIEX推估）" : "現貨（手動）") : "現貨";
    let html = "<h2>" + PROD_NAMES[p] + "<span class='src'>" + (r.src || "") + "｜" + (r.qTime || "") + "</span></h2>";
    html += "<div class='spotline'>" + spotLabel + "：<b>" + fmt(r.spot) + "</b> " + arrowHtml(r.spotChg, r.spotPct) +
      (p === "TWN" && cfg.twnSpotNote ? " <span class='dim'>(" + cfg.twnSpotNote + ")</span>" : "") +
      (cashClosedLive && p !== "TWN" ? " <span class='chip neg' title='現貨加權指數已於 13:30 收盤定盤，但期貨仍在交易（日盤至 13:45、夜盤 15:00 起）。此時的還原價差是「跳動的期貨」對「靜止的現貨收盤值」，常因尾盤集合競價跳動而被放大，參考性下降。'>現貨已收盤</span>" : "") + "</div>";
    html += "<table><tr><th>合約</th><th>期貨價</th><th title='期貨價−現貨價，未調整除息的原始差距'>表面價差</th><th title='合約到期前還會除掉的股息點數（預估值），除息會讓指數機械性下跌'>未除息</th><th title='表面價差＋未除息點數＝真實價差。紅(正)＝期貨比合理水準貴、市場偏多；綠(負)＝期貨貼水、偏空'>還原價差</th></tr>";
    for (const leg of ["near", "next"]) {
      const c = r[leg];
      if (!c) continue;
      let label = (leg === "near" ? "近月 " : "次月 ") + c.ym.slice(2).replace("-", "/");
      if (leg === "near") {
        const dts2 = daysToSettle(c.settle, tStr);
        if (dts2 != null && dts2 <= 2) label += " <span class='chip pos' title='近月合約即將結算，價差會快速收斂、訊號易受換月干擾，建議改看次月合約'>即將到期</span>";
      }
      html += "<tr><td>" + label + "<span class='dim' style='font-size:10.5px'>（結算 " + (c.settle || "—").slice(5) + "）</span></td>" +
        "<td>" + fmt(c.px) + (c.isTest ? "<span class='chg dim' title='開盤前試撮合價格（08:30–08:45），為預估開盤價'>盤前試撮</span>" : arrowHtml(c.chg, c.pct)) + "</td>" +
        "<td class='" + cls(c.raw) + "'>" + fmt(c.raw) + "</td>" +
        "<td class='dim'>" + fmt(c.rem) + "</td>" +
        "<td class='" + cls(c.adj) + " big'>" + fmt(c.adj) + "</td></tr>";
    }
    if (r.calSpread != null) {
      html += "<tr><td>近遠月價差</td><td>—</td><td class='" + cls(r.calSpread) + "'>" + fmt(r.calSpread) + "</td><td>—</td><td class='" + cls(r.calAdj) + "'>" + fmt(r.calAdj) + "</td></tr>";
    }
    html += "</table>";
    const a = r.near ? r.near.adj : null;
    if (a != null) {
      const pct = r.spot ? (a / r.spot * 100) : null;
      let sig;
      if (a > 0) sig = "還原後正價差 <b class='pos'>" + fmt(a) + "</b> 點" + (pct != null ? "（" + pct.toFixed(2) + "%）" : "") + "，期貨溢價、偏多";
      else if (a < 0) sig = "還原後逆價差 <b class='neg'>" + fmt(a) + "</b> 點" + (pct != null ? "（" + pct.toFixed(2) + "%）" : "") + "，期貨貼水、偏空";
      else sig = "還原後價差近於零，期現貨大致平衡";
      const mo = momentum(p);
      if (mo) sig += chipHtml("5分鐘變化", mo.m5, "還原價差與約5分鐘前相比的變化") + chipHtml("今日以來", mo.day0, "還原價差與今天第一筆紀錄相比的變化");
      const fair = fairCost(r.spot, r.near.settle, tStr);
      if (fair != null) {
        const dev = Math.round((a - fair) * 100) / 100;
        sig += chipPlain("合理價差(利息成本)", fmt(fair), "依資金利率估算的合理正價差＝現貨×利率×剩餘天數÷365。價差高於這個值才算真的貴") +
          "<span class='chip' title='還原價差減去合理價差。正值越大＝期貨被買得越貴（偏多訊號）；負值＝期貨被賣得便宜（偏空或避險賣壓）'>超出合理值 <b class='" + cls(dev) + "'>" + fmt(dev) + "</b></span>";
      }
      if (p !== "TWN" && r.near.raw != null) {
        const pq = monthPctl(p, r.near.raw);
        if (pq != null) sig += chipPlain("同月歷史百分位", pq + "%", "目前價差在近5年同月份分布中的百分位。98%＝比歷年同期98%的交易日都高");
      }
      html += "<div class='sig'>" + sig + "</div>";
    }
    card.innerHTML = html;
    cards.appendChild(card);
  }
  renderSidePanel(res);
}

// 右側「其他期貨」收合面板：電子/金融/非金電/富台 近月期貨值＋對昨收漲跌＋還原價差；點擊載入下方台指期價差圖
function renderSidePanel(res) {
  const el = document.getElementById("sideList");
  if (!el) return;
  const defs = [["TE", "電子期"], ["TF", "金融期"], ["XIF", "非金電期"], ["TWN", "富台期"]];
  let html = "";
  for (const [p, name] of defs) {
    const r = res && res[p];
    const near = r && r.near ? r.near : null;
    if (!near) { html += "<div class='sp-row' data-p='" + p + "'><div class='sp-top'><span class='sp-name'>" + name + "</span><span class='dim'>—</span></div></div>"; continue; }
    const a = (near.chg == null) ? "" : (near.chg > 0 ? "▲" : (near.chg < 0 ? "▼" : ""));
    const pctStr = near.pct != null ? (near.pct > 0 ? "+" : "") + near.pct.toFixed(2) + "%" : "";
    html += "<div class='sp-row' data-p='" + p + "' title='點此把下方台指期價差圖表切換到 " + name + "'>" +
      "<div class='sp-top'><span class='sp-name'>" + name + "</span>" +
      "<span class='sp-px'><b>" + fmt(near.px) + "</b> <span class='" + cls(near.chg) + "'>" + a + pctStr + "</span></span></div>" +
      "<div class='sp-sub'><span>還原價差</span><span class='" + cls(near.adj) + "'>" + fmt(near.adj) + "</span></div></div>";
  }
  el.innerHTML = html;
  for (const row of el.querySelectorAll(".sp-row")) {
    row.onclick = () => {
      const p = row.getAttribute("data-p");
      hist.prod = p;
      if (p === "TWN" && hist.res !== "snap") { hist.res = "snap"; }   // 富台僅快照圖支援
      loadHist();
      const hc = document.getElementById("hcv"); if (hc) hc.scrollIntoView({ behavior: "smooth", block: "center" });
    };
  }
}

// ═══════════════════════════════════════════════════
// 歷史圖表（自繪 canvas：拖曳平移／時間軸縮放／Y軸縮放）
// ═══════════════════════════════════════════════════
const hist = {
  prod: "TX", res: "m5", autoAnchor: true,
  data: null, dirty: true, timeMode: false, win: null,
  zoom: 1, panX: 0, yZoom: 1, panY: 0,
  drag: null, mouse: null,
  fiMode: "bar",   // 外資TX淨未平倉量疊加：bar=柱狀 / line=獨立左軸折線 / off=隱藏（僅日線×TX）
};
const RES_LIST = [["snap","盤中每分鐘快照(還原)"],["m5","盤中每5分鐘(還原)"],["D","每日收盤・近5年(未還原)"]];
const RES_TIPS = {
  D: "近5年每個交易日收盤的價差（期貨收盤−現貨收盤，未加回除息點數）",
  m5: "盤中每5分鐘的還原價差（已加回合約到期前未除息點數，與上方卡片一致）為主、未還原為輔；預設顯示當日、各盤同寬便於比較，往右拖曳或縮小可看更早日子",
  snap: "本程式每分鐘自動記錄的「還原價差」（已加回未除息點數）；預設顯示當日、各盤同寬便於比較，往右拖曳或縮小可看更早日子"
};
const cv = document.getElementById("hcv");
const ctx = cv.getContext("2d");
const M = { l: 10, r: 64, t: 12, b: 28 };
let W = 0, H = 0, DPR = 1;

function sizeCanvas() {
  DPR = window.devicePixelRatio || 1;
  W = cv.clientWidth; H = cv.clientHeight;
  cv.width = Math.round(W * DPR); cv.height = Math.round(H * DPR);
  ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
  drawHist();
}
window.addEventListener("resize", sizeCanvas);

function histProducts() { return hist.res === "snap" ? PRODUCTS : ["TX","TE","TF","XIF"]; }

function buildHistTabs() {
  const box = document.getElementById("histTabs");
  box.innerHTML = "";
  const addBtn = (txt, sel, fn, tip) => { const b = document.createElement("button"); b.textContent = txt; if (tip) b.title = tip; b.className = sel ? "sel" : ""; b.onclick = fn; box.appendChild(b); };
  const addSep = () => { const s = document.createElement("div"); s.className = "sep"; box.appendChild(s); };
  for (const p of histProducts()) addBtn(p, p === hist.prod, () => { hist.prod = p; loadHist(); }, PROD_NAMES[p]);
  addSep();
  for (const [k, label] of RES_LIST) {
    addBtn(label, k === hist.res, () => { hist.res = k; resetView(); loadHist(); }, RES_TIPS[k] || "");
  }
  addSep();
  addBtn("放大 ＋", false, () => doZoom(0.25), "放大時間刻度（也可用滾輪或拖曳下方時間軸）");
  addBtn("縮小 －", false, () => doZoom(-0.25), "縮小時間刻度，看更長的期間");
  addBtn("重置視圖", false, () => { resetView(); applyAnchor(); drawHist(); }, "恢復預設視圖：盤中週期＝本交易日、日線＝近120日（也可雙擊圖面）");
  addBtn("跳到最新", false, () => { hist.panX = 0; drawHist(); }, "把畫面拉回最新一筆資料");
  if (hist.res === "D" && hist.prod === "TX") {
    addSep();
    const fiLbl = { bar: "外資OI：柱狀", line: "外資OI：折線", off: "外資OI：隱藏" }[hist.fiMode] || "外資OI：柱狀";
    addBtn(fiLbl, hist.fiMode !== "off",
      () => { hist.fiMode = hist.fiMode === "bar" ? "line" : (hist.fiMode === "line" ? "off" : "bar"); buildHistTabs(); drawHist(); },
      "外資台指期淨未平倉量疊加方式：柱狀／折線(獨立左軸·口)／隱藏。僅日線×台指期(TX)有資料");
  }
}

function resetView() { hist.zoom = 1; hist.panX = 0; hist.yZoom = 1; hist.panY = 0; hist.autoAnchor = true; }

// 圖表資料前端快取：同一商品/週期短時間內重複切換不再重打 API（避免切換卡頓）
const histFetchCache = {};
async function fetchHistory(prod, res) {
  const key = prod + "|" + res;
  const c = histFetchCache[key];
  if (c && Date.now() - c.t < (res === "5" ? 12000 : 45000)) return c.d;   // 盤中5分線縮短前端快取
  const d = await (await fetch("/api/history?product=" + prod + "&res=" + res)).json();
  histFetchCache[key] = { d, t: Date.now() };
  return d;
}

async function loadHist() {
  buildHistTabs();
  hist.loading = true;
  drawHist();                 // 立即顯示「載入中…」，避免切換時畫面空白卡住
  await loadContext();
  hist.timeMode = (hist.res === "m5" || hist.res === "snap");
  if (false) {  } else if (hist.res === "snap") {
    if (!cfg) await loadCfgFromServer();
    await pullSnaps();
    const s = snapSeries(hist.prod);
    hist.data = { t: s.t, main: s.adj, sub: s.raw, mainLabel: "還原價差(每分鐘)", subLabel: "未還原價差", isDate: false };
  } else {
    try {
      const d = await fetchHistory(hist.prod, hist.res === "D" ? "D" : "5");
      if (hist.res === "D") {
        const spread = d.d.map((_, i) => (d.fut[i] != null && d.spot[i] != null) ? Math.round((d.fut[i] - d.spot[i]) * 100) / 100 : null);
        hist.data = { t: d.d, main: spread, sub: null, mainLabel: "未還原價差(每日收盤・近5年無除息還原)", subLabel: null, isDate: true, fut: d.fut, spot: d.spot };
      } else {
        if (!cfg) await loadCfgFromServer();
        const idx = PRODUCTS.indexOf(hist.prod);
        const tt = [], mainArr = [], subArr = [], ft = [], sptArr = [];
        for (let i = 0; i < d.t.length; i++) {
          if (d.fut[i] == null || d.spot[i] == null) continue;
          const raw = Math.round((d.fut[i] - d.spot[i]) * 100) / 100;
          const ds = dateOfTs(d.t[i]);
          let y = +ds.slice(0, 4), m = +ds.slice(5, 7);
          if (ds > thirdWednesday(y, m)) { m++; if (m > 12) { m = 1; y++; } }   // 該日的近月合約
          const ym = y + "-" + String(m).padStart(2, "0"), settle = thirdWednesday(y, m);
          const rem = cfg ? remainingDiv(cfg, idx, ym, settle, ds) : null;
          const adj = (rem != null) ? Math.round((raw + rem) * 100) / 100 : null;  // 還原＝原始價差＋未除息點數
          tt.push(d.t[i]); ft.push(d.fut[i]); sptArr.push(d.spot[i]); subArr.push(raw); mainArr.push(adj);
        }
        hist.data = { t: tt, main: mainArr, sub: subArr, mainLabel: "還原價差(每5分鐘)", subLabel: "未還原價差", isDate: false, fut: ft, spot: sptArr };
      }
    } catch (e) { hist.data = null; }
  }
  hist.loading = false;
  if (hist.timeMode) {
    buildTimeCoords();
    // 偵測到新交易日（例如開盤後第一根當日價差K出現）→ 強制跳到當日，即使先前曾手動拖曳/縮放
    const D = hist.data, nd = (D && D.dayList && D.dayList.length) ? D.dayList[D.dayList.length - 1] : null;
    if (nd && hist._lastDay && nd !== hist._lastDay) hist.autoAnchor = true;
    hist._lastDay = nd;
  }
  applyAnchor();
  hist.dirty = false;
  drawHist();
}

// 盤中固定時間軸：當日 08:45–13:45 視窗
function dayWindow(ts) {
  const base = new Date(ts * 1000);
  const tw = new Date(base.toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
  const off = tw.getTime() - base.getTime();
  const s = new Date(tw); s.setHours(8, 45, 0, 0);
  const t0 = Math.floor((s.getTime() - off) / 1000);
  return [t0, t0 + 5 * 3600];
}
const SESSION_MIN = 300; // 盤中一個交易時段 08:45–13:45 = 300 分鐘
// 盤中圖：保留全部資料，計算「交易時段壓縮座標」(各盤同寬、去除隔夜空檔)。
// 預設框在最新交易日(右錨定),手動往左拖或縮小即可看更早的日子。
function buildTimeCoords() {
  const D = hist.data;
  D.dayList = [];
  if (!D || !D.t || !D.t.length) { D.coord = []; D.coordMax = SESSION_MIN; return; }
  const dayIdx = {};
  D.coord = D.t.map(ts => {
    const ds = dateOfTs(ts);
    if (!(ds in dayIdx)) { dayIdx[ds] = D.dayList.length; D.dayList.push(ds); }
    let m = (ts - dayWindow(ts)[0]) / 60;
    if (m < 0) m = 0; else if (m > SESSION_MIN) m = SESSION_MIN;
    return dayIdx[ds] * SESSION_MIN + m;
  });
  D.coordMax = D.dayList.length * SESSION_MIN;  // 最新盤結束(13:45)座標
}

// 對比模式：把兩條序列轉為「可視範圍起點＝0%」的相對漲跌
function viewArrays(i0, i1) {
  const D = hist.data;
  if (!D.comp) return { mainArr: D.main, subArr: D.sub };
  let bi = -1;
  for (let i = Math.max(0, i0); i <= i1; i++) {
    if (D.main[i] != null && (!D.sub || D.sub[i] != null)) { bi = i; break; }
  }
  if (bi < 0) {
    for (let i = Math.max(0, i0); i <= i1; i++) if (D.main[i] != null) { bi = i; break; }
  }
  if (bi < 0) return { mainArr: D.main, subArr: D.sub };
  const bA = D.main[bi];
  let bB = null;
  if (D.sub) { bB = D.sub[bi]; if (bB == null) { for (let i = bi; i <= i1; i++) if (D.sub[i] != null) { bB = D.sub[i]; break; } } }
  const mainArr = D.main.map(v => (v == null || !bA) ? null : Math.round((v / bA - 1) * 10000) / 100);
  const subArr = (D.sub && bB) ? D.sub.map(v => v == null ? null : Math.round((v / bB - 1) * 10000) / 100) : null;
  return { mainArr, subArr };
}

function inSessionTs(ts) {
  const d = new Date(new Date(ts * 1000).toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
  const hm = d.getHours() * 100 + d.getMinutes();
  return d.getDay() >= 1 && d.getDay() <= 5 && hm >= 840 && hm <= 1355;
}
// 現貨交易時段（台股加權 09:00–13:30）；上限放寬到 13:45 含尾盤集合競價定盤＋期貨收盤段，
// 讓快照圖也畫出「現貨13:30定盤跳動 vs 期貨續跑」造成的價差放大，與卡片即時值對齊；
// 盤前(08:45–09:00)現貨仍是昨收、與已開盤的期貨相減會得到假價差，仍須排除
function inCashSession(ts) {
  const d = new Date(new Date(ts * 1000).toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
  const hm = d.getHours() * 100 + d.getMinutes();
  return d.getDay() >= 1 && d.getDay() <= 5 && hm >= 900 && hm <= 1345;
}
// 預設視圖：盤中週期錨定「本交易日」，日線錨定近120個交易日
function applyAnchor() {
  if (!hist.autoAnchor || !hist.data || !hist.data.t.length) return;
  if (hist.timeMode) { hist.zoom = 1; hist.panX = 0; return; }   // 盤中：固定時間視窗鋪滿
  const n = hist.data.t.length;
  const vis = Math.min(n, 120);                                  // 日線：近120個交易日
  hist.zoom = Math.max(1, n / vis);
  hist.panX = 0;
}

// X 座標：日線以 bar index 為單位；盤中（timeMode）以固定時間軸為單位
function ppb() {
  const n = hist.data ? hist.data.t.length : 1;
  return Math.max(0.0005, (W - M.l - M.r) / Math.max(n, 2) * hist.zoom);
}
// 盤中：每分鐘像素數（zoom=1 時一個交易時段剛好鋪滿繪圖區）；座標→像素為右錨定
function ppCoord() { return (W - M.l - M.r) / SESSION_MIN * hist.zoom; }
function xOfCoord(c) { return (W - M.r) + hist.panX - (hist.data.coordMax - c) * ppCoord(); }
function xOf(i) {
  if (hist.timeMode) return xOfCoord(hist.data.coord[i]);
  const n = hist.data.t.length;
  return W - M.r + hist.panX - (n - 1 - i) * ppb();
}
function iOfX(x) {
  if (hist.timeMode) {
    const C = hist.data.coord, n = C.length;
    if (!n) return 0;
    const c = hist.data.coordMax - ((W - M.r) + hist.panX - x) / ppCoord();
    let lo = 0, hi = n - 1;
    while (lo < hi) { const mid = (lo + hi) >> 1; if (C[mid] < c) lo = mid + 1; else hi = mid; }
    if (lo > 0 && Math.abs(C[lo - 1] - c) <= Math.abs(C[lo] - c)) lo--;
    return lo;
  }
  const n = hist.data.t.length;
  return Math.round(n - 1 - (W - M.r + hist.panX - x) / ppb());
}
function visRange() {
  const n = hist.data.t.length;
  if (hist.timeMode) {
    const C = hist.data.coord;
    const cL = hist.data.coordMax - ((W - M.r) + hist.panX - M.l) / ppCoord();
    const cR = hist.data.coordMax - hist.panX / ppCoord();
    let i0 = 0; while (i0 < n && C[i0] < cL) i0++;
    let i1 = n - 1; while (i1 >= 0 && C[i1] > cR) i1--;
    i0 = Math.max(0, i0 - 1); i1 = Math.min(n - 1, i1 + 1);
    if (i1 < i0) return [0, -1];   // 視窗落在空白區（無資料）
    return [i0, i1];
  }
  let i0 = Math.max(0, Math.floor(iOfX(M.l)) - 1);
  let i1 = Math.min(n - 1, Math.ceil(iOfX(W - M.r)) + 1);
  if (i1 < i0) { i0 = 0; i1 = n - 1; }
  return [i0, i1];
}

function drawHist() {
  ctx.clearRect(0, 0, W, H);
  if (hist.loading) {
    ctx.fillStyle = "#99a"; ctx.font = "13px sans-serif"; ctx.textAlign = "center"; ctx.textBaseline = "middle";
    ctx.fillText("載入中…", W / 2, H / 2);
    return;
  }
  if (!hist.data || !hist.data.t.length) {
    ctx.fillStyle = "#99a"; ctx.font = "13px sans-serif"; ctx.textAlign = "center";
    ctx.fillText(hist.res === "snap" ? "尚無快照資料（伺服器啟動後開始累積）" : "載入中或無資料…", W / 2, H / 2);
    return;
  }
  const D = hist.data;
  const [i0, i1] = visRange();
  const { mainArr, subArr } = viewArrays(i0, i1);
  let mn = Infinity, mx = -Infinity;
  for (let i = i0; i <= i1; i++) {
    for (const arr of [mainArr, subArr]) {
      if (!arr) continue;
      const v = arr[i];
      if (v != null) { if (v < mn) mn = v; if (v > mx) mx = v; }
    }
  }
  if (!isFinite(mn)) { mn = -1; mx = 1; }
  if (mx - mn < 1e-6) { mn -= 1; mx += 1; }
  if (hist.timeMode) {                 // 盤中5分/快照：縱軸對齊 niceStep 邊界，兩圖刻度一致、可直接比較
    const s2 = niceStep((mx - mn) / 4) || 1;
    mn = Math.floor(mn / s2) * s2;
    mx = Math.ceil(mx / s2) * s2;
  }
  let mid = (mn + mx) / 2 + hist.panY;
  let half = (mx - mn) / 2 * 1.15 * hist.yZoom;
  const pn = mid - half, px = mid + half;
  const yOf = v => M.t + (px - v) / (px - pn) * (H - M.t - M.b);

  ctx.font = "10.5px Consolas, monospace"; ctx.textAlign = "left"; ctx.textBaseline = "middle";
  const step = niceStep((px - pn) / 6);
  for (let v = Math.ceil(pn / step) * step; v <= px; v += step) {
    const y = yOf(v);
    ctx.strokeStyle = Math.abs(v) < step / 2 ? "#c5cede" : "#eef1f6";
    ctx.lineWidth = Math.abs(v) < step / 2 ? 1.2 : 1;
    ctx.beginPath(); ctx.moveTo(M.l, y); ctx.lineTo(W - M.r, y); ctx.stroke();
    ctx.fillStyle = "#778";
    ctx.fillText(fmtAxis(v) + (D.comp ? "%" : ""), W - M.r + 6, y);
  }
  if (pn < 0 && px > 0) {
    const y = yOf(0);
    ctx.strokeStyle = "#aab"; ctx.lineWidth = 1.2; ctx.setLineDash([4, 3]);
    ctx.beginPath(); ctx.moveTo(M.l, y); ctx.lineTo(W - M.r, y); ctx.stroke();
    ctx.setLineDash([]);
  }
  ctx.textAlign = "center"; ctx.textBaseline = "top"; ctx.fillStyle = "#778";
  if (hist.timeMode) {
    // 固定時間刻度（交易時段壓縮座標）：每個交易日標整點 09:00–13:00 ＋日期；縮太小則每盤只標日期
    const ppc = ppCoord();
    const cL = hist.data.coordMax - ((W - M.r) + hist.panX - M.l) / ppc;
    const cR = hist.data.coordMax - hist.panX / ppc;
    const nd = D.dayList ? D.dayList.length : 1;
    const ordFirst = Math.max(0, Math.floor(cL / SESSION_MIN));
    const ordLast = Math.min(nd - 1, Math.floor((cR - 1e-6) / SESSION_MIN));
    const sessPx = SESSION_MIN * ppc;
    const hours = [15, 75, 135, 195, 255];   // 09:00,10:00,11:00,12:00,13:00 距 08:45 的分鐘
    for (let ord = ordFirst; ord <= ordLast; ord++) {
      const ds = D.dayList[ord]; if (!ds) continue;
      const dlbl = ds.slice(5).replace("-", "/");
      const base = ord * SESSION_MIN;
      if (sessPx >= 130) {
        let firstTick = true;
        for (let hi = 0; hi < hours.length; hi++) {
          const x = xOfCoord(base + hours[hi]);
          if (x < M.l || x > W - M.r) continue;
          let lbl = String(9 + hi).padStart(2, "0") + ":00";
          if (firstTick) { lbl = dlbl + " " + lbl; firstTick = false; }
          ctx.fillText(lbl, x, H - M.b + 6);
          ctx.strokeStyle = "#f0f3f8";
          ctx.beginPath(); ctx.moveTo(x, M.t); ctx.lineTo(x, H - M.b); ctx.stroke();
        }
      } else {
        const xMid = xOfCoord(base + SESSION_MIN / 2);
        if (xMid >= M.l && xMid <= W - M.r) ctx.fillText(dlbl, xMid, H - M.b + 6);
        const xb = xOfCoord(base);
        if (xb >= M.l && xb <= W - M.r) { ctx.strokeStyle = "#eef1f6"; ctx.beginPath(); ctx.moveTo(xb, M.t); ctx.lineTo(xb, H - M.b); ctx.stroke(); }
      }
    }
  } else {
    const labelEvery = Math.max(1, Math.round(80 / ppb()));
    let lastLblDay = null;
    for (let i = i0; i <= i1; i++) {
      if (i % labelEvery !== 0) continue;
      const x = xOf(i);
      if (x < M.l || x > W - M.r) continue;
      let lbl = xLabel(D, i);
      if (!D.isDate) {
        const dayStr = dateOfTs(D.t[i]);
        if (lastLblDay !== null && dayStr !== lastLblDay) lbl = dayStr.slice(5).replace("-", "/") + " " + lbl;
        lastLblDay = dayStr;
      }
      ctx.fillText(lbl, x, H - M.b + 6);
      ctx.strokeStyle = "#f0f3f8";
      ctx.beginPath(); ctx.moveTo(x, M.t); ctx.lineTo(x, H - M.b); ctx.stroke();
    }
  }
  const plot = (arr, color, dash) => {
    if (!arr) return;
    ctx.strokeStyle = color; ctx.lineWidth = 1.6;
    if (dash) ctx.setLineDash([5, 4]); else ctx.setLineDash([]);
    ctx.beginPath();
    let started = false, lastOrd = null;
    const stride = Math.max(1, Math.floor((i1 - i0) / (W * 2)));
    for (let i = i0; i <= i1; i += stride) {
      const v = arr[i];
      if (v == null) { started = false; continue; }
      if (hist.timeMode) {                       // 跨交易日斷線(不連隔夜空檔)
        const ord = Math.floor(hist.data.coord[i] / SESSION_MIN);
        if (lastOrd != null && ord !== lastOrd) started = false;
        lastOrd = ord;
      }
      const x = xOf(i), y = yOf(v);
      if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
    }
    ctx.stroke(); ctx.setLineDash([]);
  };
  ctx.save();
  ctx.beginPath(); ctx.rect(M.l, M.t, W - M.l - M.r, H - M.t - M.b); ctx.clip();
  plot(subArr, D.comp ? "#1f4e8c" : "#aab", !D.comp);
  plot(mainArr, "#c92a2a", false);
  ctx.restore();
  // 外資TX淨未平倉量疊加（僅價差模式×日線×TX；柱狀／獨立左軸折線／隱藏）
  if (!D.comp && D.isDate && hist.prod === "TX" && hist.fiMode !== "off" && ctxSeries.length) {
    if (hist.fiMode === "bar") {
      let maxAbs = 0;
      for (let i = i0; i <= i1; i++) { const e = ctxMap[D.t[i]]; if (e && e.fiN != null) maxAbs = Math.max(maxAbs, Math.abs(e.fiN)); }
      if (maxAbs > 0) {
        const bandH = (H - M.t - M.b) * 0.22, yBase = H - M.b - 1;
        ctx.save(); ctx.beginPath(); ctx.rect(M.l, M.t, W - M.l - M.r, H - M.t - M.b); ctx.clip();
        const bw = Math.max(1, Math.min(9, ppb() * 0.6));
        for (let i = i0; i <= i1; i++) {
          const e = ctxMap[D.t[i]];
          if (!e || e.fiN == null) continue;
          const h = Math.abs(e.fiN) / maxAbs * bandH;
          ctx.fillStyle = e.fiN >= 0 ? "rgba(201,42,42,.45)" : "rgba(43,138,62,.45)";
          ctx.fillRect(xOf(i) - bw / 2, yBase - h, bw, h);
        }
        ctx.restore();
        ctx.fillStyle = "#889"; ctx.font = "10px sans-serif"; ctx.textAlign = "left"; ctx.textBaseline = "bottom";
        ctx.fillText("柱狀＝外資TX淨未平倉量（紅多綠空，資料自啟用日起累積）", M.l + 6, H - M.b - 4);
      }
    } else {   // line：以獨立左軸畫折線
      let fmn = Infinity, fmx = -Infinity, hasPt = false;
      for (let i = i0; i <= i1; i++) { const e = ctxMap[D.t[i]]; if (e && e.fiN != null) { hasPt = true; if (e.fiN < fmn) fmn = e.fiN; if (e.fiN > fmx) fmx = e.fiN; } }
      if (hasPt) {
        if (fmx - fmn < 1) { fmn -= 1; fmx += 1; }
        const padF = (fmx - fmn) * 0.12; fmn -= padF; fmx += padF;
        const yFi = v => M.t + (fmx - v) / (fmx - fmn) * (H - M.t - M.b);
        ctx.save(); ctx.beginPath(); ctx.rect(M.l, M.t, W - M.l - M.r, H - M.t - M.b); ctx.clip();
        ctx.strokeStyle = "#1f6feb"; ctx.lineWidth = 1.6; ctx.setLineDash([]);
        ctx.beginPath();
        let started = false;
        for (let i = i0; i <= i1; i++) {
          const e = ctxMap[D.t[i]];
          if (!e || e.fiN == null) { started = false; continue; }
          const x = xOf(i), y = yFi(e.fiN);
          if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
        }
        ctx.stroke();
        let li = -1;
        for (let i = i1; i >= i0; i--) { const e = ctxMap[D.t[i]]; if (e && e.fiN != null) { li = i; break; } }
        if (li >= 0) { ctx.fillStyle = "#1f6feb"; ctx.beginPath(); ctx.arc(xOf(li), yFi(ctxMap[D.t[li]].fiN), 2.6, 0, 7); ctx.fill(); }
        ctx.restore();
        // 左側獨立軸刻度（口；萬以上以「萬」顯示）
        const fmtOI = v => (Math.abs(v) >= 10000 ? (v / 10000).toFixed(1) + "萬" : Math.round(v).toLocaleString());
        ctx.font = "10px Consolas, monospace"; ctx.textAlign = "left"; ctx.textBaseline = "middle"; ctx.fillStyle = "#1f6feb";
        for (const fv of [fmx - padF, (fmn + fmx) / 2, fmn + padF]) ctx.fillText(fmtOI(fv), M.l + 2, yFi(fv));
        ctx.textBaseline = "bottom"; ctx.font = "10px sans-serif";
        ctx.fillText("藍線＝外資TX淨未平倉量（左軸·口，自啟用日起累積）", M.l + 60, H - M.b - 4);
      }
    }
  }
  ctx.textAlign = "left"; ctx.textBaseline = "middle"; ctx.font = "11px sans-serif";
  let lx = M.l + 6;
  const legend = (color, label, dash) => {
    if (!label) return;
    ctx.strokeStyle = color; ctx.lineWidth = 2;
    if (dash) ctx.setLineDash([4, 3]); else ctx.setLineDash([]);
    ctx.beginPath(); ctx.moveTo(lx, M.t + 8); ctx.lineTo(lx + 16, M.t + 8); ctx.stroke(); ctx.setLineDash([]);
    ctx.fillStyle = "#445"; ctx.fillText(label, lx + 20, M.t + 8);
    lx += 20 + ctx.measureText(label).width + 14;
  };
  legend("#c92a2a", D.mainLabel + (D.comp ? "（起點歸零%）" : ""), false);
  legend(D.comp ? "#1f4e8c" : "#aab", D.subLabel, !D.comp);
  if (hist.mouse && hist.mouse.x > M.l && hist.mouse.x < W - M.r && hist.mouse.y > M.t && hist.mouse.y < H - M.b) {
    const i = Math.max(0, Math.min(D.t.length - 1, iOfX(hist.mouse.x)));
    const x = xOf(i);
    ctx.strokeStyle = "#bbc"; ctx.lineWidth = 1; ctx.setLineDash([3, 3]);
    ctx.beginPath(); ctx.moveTo(x, M.t); ctx.lineTo(x, H - M.b); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(M.l, hist.mouse.y); ctx.lineTo(W - M.r, hist.mouse.y); ctx.stroke();
    ctx.setLineDash([]);
    const v = mainArr[i];
    if (v != null) { ctx.fillStyle = "#c92a2a"; ctx.beginPath(); ctx.arc(x, yOf(v), 3, 0, 7); ctx.fill(); }
    let lines;
    if (D.comp) {
      lines = [xLabelFull(D, i)];
      if (mainArr[i] != null) lines.push(D.mainLabel + "：" + (mainArr[i] > 0 ? "+" : "") + mainArr[i].toFixed(2) + "%（" + fmt(D.main[i]) + "）");
      if (subArr && subArr[i] != null) lines.push(D.subLabel + "：" + (subArr[i] > 0 ? "+" : "") + subArr[i].toFixed(2) + "%（" + fmt(D.sub[i]) + "）");
      if (mainArr[i] != null && subArr && subArr[i] != null)
        lines.push("強弱差：" + ((mainArr[i] - subArr[i]) > 0 ? "+" : "") + (mainArr[i] - subArr[i]).toFixed(2) + "%");
    } else {
      lines = [xLabelFull(D, i), D.mainLabel + "：" + fmt(D.main[i])];
      if (D.sub && D.sub[i] != null) lines.push(D.subLabel + "：" + fmt(D.sub[i]));
      if (D.fut && D.fut[i] != null) lines.push("期貨：" + fmt(D.fut[i]));
      if (D.spot && D.spot[i] != null) lines.push("現貨：" + fmt(D.spot[i]));
      if (D.isDate && ctxMap[D.t[i]] && ctxMap[D.t[i]].fiN != null) lines.push("外資淨未平倉：" + ctxMap[D.t[i]].fiN.toLocaleString());
    }
    ctx.font = "11px Consolas, monospace";
    const tw = Math.max.apply(null, lines.map(s => ctx.measureText(s).width)) + 16;
    const th = lines.length * 16 + 10;
    let bx = hist.mouse.x + 14, by = hist.mouse.y - th - 6;
    if (bx + tw > W - M.r) bx = hist.mouse.x - tw - 14;
    if (by < M.t) by = hist.mouse.y + 14;
    ctx.fillStyle = "rgba(255,255,255,.95)"; ctx.strokeStyle = "#c5cede";
    ctx.beginPath();
    if (ctx.roundRect) ctx.roundRect(bx, by, tw, th, 6); else ctx.rect(bx, by, tw, th);
    ctx.fill(); ctx.stroke();
    ctx.fillStyle = "#1a2233"; ctx.textAlign = "left"; ctx.textBaseline = "top";
    lines.forEach((s, k) => ctx.fillText(s, bx + 8, by + 6 + k * 16));
  }
}
function niceStep(raw) {
  const p = Math.pow(10, Math.floor(Math.log10(raw)));
  for (const m of [1, 2, 5, 10]) if (raw <= m * p) return m * p;
  return 10 * p;
}
function fmtAxis(v) { return Math.abs(v) >= 1000 ? (v / 1000).toFixed(1) + "k" : (Math.round(v * 10) / 10).toString(); }
function xLabel(D, i) {
  if (D.isDate) return D.t[i].slice(2);
  const d = new Date(D.t[i] * 1000);
  return d.toLocaleTimeString("zh-TW", { timeZone: "Asia/Taipei", hour12: false, hour: "2-digit", minute: "2-digit" });
}
function xLabelFull(D, i) {
  if (D.isDate) return D.t[i];
  return new Date(D.t[i] * 1000).toLocaleString("zh-TW", { timeZone: "Asia/Taipei", hour12: false });
}

// ---- 互動 ----
const ZMIN = 0.05, ZMAX = 400;
function doZoom(d) {
  hist.autoAnchor = false;
  hist.zoom = Math.max(ZMIN, Math.min(ZMAX, hist.zoom * (1 + d)));
  drawHist();
}
function evPos(e) {
  const r = cv.getBoundingClientRect();
  const t = e.touches ? e.touches[0] : e;
  return { x: t.clientX - r.left, y: t.clientY - r.top };
}
function onDown(e) {
  const m = evPos(e);
  const pt = e.touches ? e.touches[0] : e;
  if (m.y >= H - M.b && m.x > M.l && m.x < W - M.r) {
    hist.drag = { kind: "xaxis", sx: pt.clientX, z0: hist.zoom };
    cv.style.cursor = "ew-resize";
  } else if (m.x >= W - M.r && m.y > M.t && m.y < H - M.b) {
    hist.drag = { kind: "yaxis", sy: pt.clientY, z0: hist.yZoom };
    cv.style.cursor = "ns-resize";
  } else if (m.x > M.l && m.x < W - M.r) {
    hist.drag = { kind: "pan", sx: pt.clientX, sy: pt.clientY, px0: hist.panX, py0: hist.panY };
    cv.style.cursor = "grabbing";
  }
  if (hist.drag) { hist.autoAnchor = false; if (e.cancelable) e.preventDefault(); }
}
function onMove(e) {
  const pt = e.touches ? e.touches[0] : e;
  if (hist.drag) {
    if (hist.drag.kind === "xaxis") {
      const dx = pt.clientX - hist.drag.sx;
      hist.zoom = Math.max(ZMIN, Math.min(ZMAX, hist.drag.z0 * Math.exp(dx * 0.004)));
    } else if (hist.drag.kind === "yaxis") {
      const dy = pt.clientY - hist.drag.sy;
      hist.yZoom = Math.max(0.05, Math.min(20, hist.drag.z0 * Math.exp(dy * 0.005)));
    } else {
      hist.panX = hist.drag.px0 + (pt.clientX - hist.drag.sx);
      hist.panY = hist.drag.py0 + (pt.clientY - hist.drag.sy) * yRangePerPx();
    }
    hist.mouse = null;
    drawHist();
    if (e.cancelable) e.preventDefault();
    return;
  }
  hist.mouse = evPos(e);
  drawHist();
}
function yRangePerPx() {
  if (!hist.data || !hist.data.t.length) return 0.01;
  const [i0, i1] = visRange();
  const { mainArr } = viewArrays(i0, i1);
  let mn = Infinity, mx = -Infinity;
  for (let i = i0; i <= i1; i++) {
    const v = mainArr[i];
    if (v != null) { if (v < mn) mn = v; if (v > mx) mx = v; }
  }
  if (!isFinite(mn) || mx - mn < 1e-6) return 0.01;
  return (mx - mn) * 1.15 * hist.yZoom / (H - M.t - M.b);
}
function onUp() { hist.drag = null; cv.style.cursor = "crosshair"; }
cv.addEventListener("mousedown", onDown);
document.addEventListener("mousemove", e => { if (hist.drag || e.target === cv) onMove(e); });
document.addEventListener("mouseup", onUp);
cv.addEventListener("mouseleave", () => { if (!hist.drag) { hist.mouse = null; drawHist(); } });
cv.addEventListener("wheel", e => { e.preventDefault(); doZoom(e.deltaY < 0 ? 0.18 : -0.15); }, { passive: false });
cv.addEventListener("dblclick", () => { resetView(); applyAnchor(); drawHist(); });
cv.addEventListener("touchstart", onDown, { passive: false });
cv.addEventListener("touchmove", onMove, { passive: false });
cv.addEventListener("touchend", onUp);

// ---------- 市場脈絡（外資部位/PCR/換月）與評價 ----------
let ctxSeries = [], ctxMap = {}, ctxFetched = 0;
let dailyCacheJS = {};
async function loadContext() {
  if (Date.now() - ctxFetched < 600000 && ctxSeries.length) return;
  try {
    const d = await (await fetch("/api/context")).json();
    ctxSeries = d.dates || [];
    ctxMap = {};
    for (const e of ctxSeries) ctxMap[e.d] = e;
    ctxFetched = Date.now();
  } catch (e) {}
}
async function ensureDaily(p) {
  if (dailyCacheJS[p]) return dailyCacheJS[p];
  try {
    const d = await (await fetch("/api/history?product=" + p + "&res=D")).json();
    d.spread = (d.d || []).map((_, i) => (d.fut[i] != null && d.spot[i] != null) ? d.fut[i] - d.spot[i] : null);
    dailyCacheJS[p] = d;
  } catch (e) { dailyCacheJS[p] = { d: [], spread: [] }; }
  return dailyCacheJS[p];
}
function monthPctl(p, cur) {
  const d = dailyCacheJS[p];
  if (!d || !d.d || d.d.length < 100 || cur == null) return null;
  const mm = todayTW().slice(5, 7);
  const vals = [];
  for (let i = 0; i < d.d.length; i++)
    if (d.d[i].slice(5, 7) === mm && d.spread[i] != null) vals.push(d.spread[i]);
  if (vals.length < 30) return null;
  let below = 0;
  for (const v of vals) if (v < cur) below++;
  return Math.round(below / vals.length * 100);
}
function fairCost(spot, settle, todayStr) {
  if (spot == null || !settle) return null;
  const rate = (cfg && cfg.costRate != null) ? cfg.costRate : 1.5;
  const days = Math.max(0, (new Date(settle) - new Date(todayStr)) / 86400000);
  return Math.round(spot * rate / 100 * days / 365 * 100) / 100;
}
function daysToSettle(settle, todayStr) {
  if (!settle) return null;
  return Math.round((new Date(settle) - new Date(todayStr)) / 86400000);
}
function chipPlain(label, txt, tip) { return "<span class='chip'" + (tip ? " title='" + tip + "'" : "") + ">" + label + " <b>" + txt + "</b></span>"; }
function renderCtxBar(res) {
  const el = document.getElementById("ctxBar");
  if (!el) return;
  const last = ctxSeries.length ? ctxSeries[ctxSeries.length - 1] : null;
  let prev = null;
  for (let i = ctxSeries.length - 2; i >= 0; i--) {
    if (last && ctxSeries[i].fiN != null && last.fiN != null) { prev = ctxSeries[i]; break; }
  }
  const items = [];
  const netWord = v => (v > 0 ? "+" : (v < 0 ? "−" : "")) + Math.abs(v).toLocaleString() + " 口" + (v > 0 ? "（淨多單）" : (v < 0 ? "（淨空單）" : ""));
  if (last && last.fiN != null) {
    const chg = (prev && prev.fiN != null) ? last.fiN - prev.fiN : null;
    items.push("<span class='ctxitem' title='外資及陸資在台指期貨的未平倉淨部位（多單口數−空單口數）。每日收盤後由期交所公布，顯示日期為資料日'>" +
      "外資台指期淨未平倉量 <b class='" + cls(last.fiN) + "'>" + netWord(last.fiN) + "</b>" +
      (chg != null ? " <span class='" + cls(chg) + "'>較前日 " + (chg > 0 ? "+" : "−") + Math.abs(chg).toLocaleString() + "</span>" : "") +
      " <span class='dim'>" + last.d.slice(5) + "</span></span>");
    if (last.itN != null) items.push("<span class='ctxitem' title='投信（國內基金公司）在台指期貨的未平倉淨部位'>" +
      "投信淨未平倉量 <b class='" + cls(last.itN) + "'>" + netWord(last.itN) + "</b></span>");
  }
  if (last && last.pcrOI != null) {
    const mood = last.pcrOI > 110 ? "偏避險" : (last.pcrOI < 90 ? "偏樂觀" : "中性");
    items.push("<span class='ctxitem' title='選擇權賣權與買權的未平倉量比率。大於100%代表賣權（保險性質）部位較多，市場偏防禦'>" +
      "賣權買權未平倉比 <b>" + last.pcrOI + "%</b> <span class='dim'>" + mood + "</span></span>");
  }
  const tx = res && res.TX;
  if (tx && tx.near) {
    const dts = daysToSettle(tx.near.settle, todayTW());
    if (dts != null) items.push("<span class='ctxitem' title='台指期每月第三個週三到期結算。接近結算時價差會自然收斂，訊號易受換月影響'>" +
      "距台指期結算 <b>" + dts + "</b> 天" + (dts <= 2 ? " <span class='pos'>結算週</span>" : "") + "</span>");
    if (tx.near.vol != null && tx.next && tx.next.vol != null && (tx.near.vol + tx.next.vol) > 0) {
      const pr = Math.round(tx.next.vol / (tx.near.vol + tx.next.vol) * 100);
      items.push("<span class='ctxitem' title='次月合約成交量÷（近月＋次月成交量）。比例越高代表部位移往次月越多，超過五成代表換月過半'>" +
      "換月進度（次月成交量占比）<b>" + pr + "%</b></span>");
    }
  }
  if (items.length) items.unshift("<span class='dim' style='padding:5px 0'>市場脈絡｜</span>");
  el.innerHTML = items.join("");
  el.style.display = items.length ? "flex" : "none";
}
const REF_LIST = [
  ["TWS:2330:STOCK", "台積電", "台股權值最大個股（占加權指數約三成五）。價差急變時先看它：若只有台積電大漲跌，多半是權值效應而非全面性行情"],
  ["TWS:2317:STOCK", "鴻海", "權值前三大個股"],
  ["TWS:2454:STOCK", "聯發科", "權值前三大個股"],
  ["TWS:0050:STOCK", "元大台灣50", "權值股ETF。與期貨方向背離時，代表現貨買盤與期貨情緒不同步"],
  ["TWS:OTC01:INDEX", "櫃買指數", "中小型股指數。與加權指數同跌齊漲＝全面性行情；背離＝權值股獨走"],
  ["FX:USDTWD:FOREX", "美元／台幣", "台幣貶值（數字上升）通常代表外資匯出、偏空；升值代表資金流入、偏多"],
  ["GI:NDX:INDEX", "NASDAQ100", "美股科技指數，台股電子權值的主要連動對象（美股交易時段＝台指夜盤）"],
  ["GI:DJI:INDEX", "道瓊指數", "美股傳產金融指數（美股交易時段＝台指夜盤）"]
];
function renderRefBar(raw) {
  const el = document.getElementById("refBar");
  if (!el) return;
  if (!(cfg && cfg.refBarOn)) { el.style.display = "none"; return; }   // 預設隱藏，於參數設定中開啟
  const hide = new Set((cfg && cfg.refBarHide) || []);
  const cn = raw.cnyes;
  if (!cn) { el.style.display = "none"; return; }
  const items = [];
  for (const [code, name, tip] of REF_LIST) {
    if (hide.has(code)) continue;
    const q = cn[code];
    if (!q || num(q.last) == null) continue;
    let chg = num(q.chg), pct = num(q.chgPct);
    const prev = num(q.prev);
    // 個股盤中漲跌欄位偶有歸零，改以昨收自算
    if ((chg == null || (chg === 0 && pct === 0)) && prev != null && prev !== 0 && num(q.last) !== prev) {
      chg = Math.round((num(q.last) - prev) * 100) / 100;
      pct = Math.round(chg / prev * 10000) / 100;
    }
    const a = (chg == null || chg === 0) ? "" : (chg > 0 ? "▲" : "▼");
    const pctStr = pct != null ? (pct > 0 ? "+" : "") + pct.toFixed(2) + "%" : "";
    items.push("<span class='ctxitem' title='" + tip + "'>" + name + " <b>" + fmt(num(q.last)) + "</b> " +
      "<span class='" + cls(chg) + "'>" + a + pctStr + "</span></span>");
  }
  // 台指期連續（含夜盤）：盤前顯示的即是隔夜夜盤變動
  const fxc = hide.has("TXF") ? null : cn["TWF:TXF:FUTURES"];
  if (fxc && num(fxc.last) != null && num(fxc.prev) != null && num(fxc.prev) !== 0) {
    const c2 = num(fxc.last) - num(fxc.prev), p2 = c2 / num(fxc.prev) * 100;
    items.unshift("<span class='ctxitem' title='台指期連續合約最新價（含凌晨5點收盤的夜盤）。盤前時段顯示的就是夜盤相對昨日的變動，可預估今日開盤跳空方向'>" +
      "台指期(含夜盤) <b>" + fmt(num(fxc.last)) + "</b> <span class='" + cls(c2) + "'>" + (c2 > 0 ? "▲" : (c2 < 0 ? "▼" : "")) + (p2 > 0 ? "+" : "") + p2.toFixed(2) + "%</span></span>");
  }
  // 海外對照（盤前判讀用）
  const Y = raw.yahoo || {};
  const yDefs = [
    ["TSM", "台積電ADR", "台積電美國存託憑證（1股ADR＝5股普通股），美股收盤價。換算溢價＝ADR×匯率÷5 相對台積電昨收的百分比，是早盤最重要的開盤領先指標"],
    ["^SOX", "費城半導體", "美國半導體指數昨夜收盤，台股電子股的主要連動指標"],
    ["^N225", "日經225", "日本股市（09:00 開盤，與台股盤中同步）"],
    ["^KS11", "韓國KOSPI", "韓國股市（08:00 開盤，比台股早 45 分鐘，盤前可先參考）"],
    ["YM=F", "道瓊期", "CME 道瓊 E-mini 期貨（近乎24小時交易），美股大盤盤前領先指標"],
    ["ES=F", "S&P期", "CME S&P500 E-mini 期貨，美股大盤盤前領先指標"],
    ["NQ=F", "那斯達克期", "CME 那斯達克100 E-mini 期貨，科技股／電子權值盤前領先指標"],
    ["RTY=F", "羅素2000期", "CME 羅素2000 E-mini 期貨，中小型股／風險偏好指標"],
    ["^TNX", "美10年債息", "美國10年期公債殖利率（%）。殖利率升高壓抑成長股估值、通常利空科技股"],
    ["BZ=F", "Brent原油", "布蘭特原油期貨（美元/桶），通膨與能源類股風向"],
    ["BTC-USD", "比特幣", "加密貨幣龍頭，全球風險情緒指標"]
  ];
  for (const [sym, yname, tip] of yDefs) {
    if (hide.has(sym)) continue;
    const y = Y[sym];
    if (!y || y.last == null || !y.prev) continue;
    const c3 = y.last - y.prev, p3 = c3 / y.prev * 100;
    let extra = "";
    if (sym === "TSM") {
      const fxq = cn["FX:USDTWD:FOREX"], tw = cn["TWS:2330:STOCK"];
      if (fxq && num(fxq.last) && tw && num(tw.prev)) {
        const prem = (y.last * num(fxq.last) / 5 / num(tw.prev) - 1) * 100;
        extra = " <span class='" + cls(prem) + "'>換算溢價" + (prem > 0 ? "+" : "") + prem.toFixed(1) + "%</span>";
      }
    }
    items.push("<span class='ctxitem' title='" + tip + "'>" + yname + " <b>" + fmt(y.last) + "</b> <span class='" + cls(c3) + "'>" + (c3 > 0 ? "▲" : (c3 < 0 ? "▼" : "")) + (p3 > 0 ? "+" : "") + p3.toFixed(2) + "%</span>" + extra + "</span>");
  }
  for (const raw0 of ((cfg && cfg.refBarAdd) || [])) {        // 手動加入 ticker（台股數字→.TW）
    const r = (raw0 || "").trim(); if (!r) continue;
    const s = /^\d+$/.test(r) ? (r + ".TW") : r.toUpperCase();
    const y = Y[s];
    if (!y || y.last == null || !y.prev) continue;
    const c4 = y.last - y.prev, p4 = c4 / y.prev * 100;
    items.push("<span class='ctxitem' title='手動加入'>" + r.toUpperCase() + " <b>" + fmt(y.last) + "</b> <span class='" + cls(c4) + "'>" + (c4 > 0 ? "▲" : (c4 < 0 ? "▼" : "")) + (p4 > 0 ? "+" : "") + p4.toFixed(2) + "%</span></span>");
  }
  if (items.length) items.unshift("<span class='dim' style='padding:5px 0'>對照行情｜</span>");
  el.innerHTML = items.join("");
  el.style.display = items.length ? "flex" : "none";
}
async function saveRate() {
  const v = parseFloat(document.getElementById("costRate").value);
  await postCfg({ ...cfg, costRate: isNaN(v) ? 1.5 : v });
  refresh();
}

// ---------- 標的主清單（參數設定下拉可自由切換；宏觀卡僅支援 ext=Yahoo 標的）----------
const INSTRUMENTS = [
  { id: "twii", label: "加權指數(現貨)", grp: "台股", type: "spot",  prod: "TX", win: "tw" },
  { id: "otc",  label: "櫃買指數(現貨)", grp: "台股", type: "bench", code: "TWS:OTC01:INDEX", win: "tw" },
  { id: "tsmc", label: "台積電",         grp: "台股", type: "bench", code: "TWS:2330:STOCK", win: "tw" },
  { id: "hon",  label: "鴻海",           grp: "台股", type: "bench", code: "TWS:2317:STOCK", win: "tw" },
  { id: "mtk",  label: "聯發科",         grp: "台股", type: "bench", code: "TWS:2454:STOCK", win: "tw" },
  { id: "t50",  label: "元大台灣50",     grp: "台股", type: "bench", code: "TWS:0050:STOCK", win: "tw" },
  { id: "ym",   label: "道瓊 期 YM",     grp: "美股期指", type: "ext", sym: "YM=F",  dec: 0 },
  { id: "es",   label: "S&P500 期 ES",   grp: "美股期指", type: "ext", sym: "ES=F",  dec: 2 },
  { id: "nq",   label: "那斯達克 期 NQ", grp: "美股期指", type: "ext", sym: "NQ=F",  dec: 2 },
  { id: "rty",  label: "羅素2000 期 RTY", grp: "美股期指", type: "ext", sym: "RTY=F", dec: 2 },
  { id: "niy",  label: "日經225 期 NIY", grp: "亞股期指", type: "ext", sym: "NIY=F", dec: 0 },
  { id: "dji",  label: "道瓊(現貨)",     grp: "美股現貨", type: "us", code: "^DJI" },
  { id: "spx",  label: "S&P500(現貨)",   grp: "美股現貨", type: "us", code: "^GSPC" },
  { id: "ixic", label: "那斯達克(現貨)", grp: "美股現貨", type: "us", code: "^IXIC" },
  { id: "ndx",  label: "那斯達克100(現貨)", grp: "美股現貨", type: "us", code: "^NDX" },
  { id: "sox",  label: "費城半導體(現貨)", grp: "美股現貨", type: "us", code: "^SOX" },
  { id: "dax",  label: "德國DAX(現貨)",  grp: "歐股現貨", type: "ext", sym: "^GDAXI", dec: 2 },
  { id: "twd",  label: "美元/台幣",      grp: "匯率", type: "ext", sym: "TWD=X", dec: 3 },
  { id: "jpy",  label: "日幣/美金",      grp: "匯率", type: "ext", sym: "JPY=X", dec: 2 },
  { id: "dxy",  label: "美元指數 DXY",   grp: "匯率", type: "ext", sym: "DX-Y.NYB", dec: 2 },
  { id: "brent", label: "Brent原油",     grp: "商品", type: "ext", sym: "BZ=F", dec: 2 },
  { id: "wti",  label: "WTI原油",        grp: "商品", type: "ext", sym: "CL=F", dec: 2 },
  { id: "tnx",  label: "美10年債殖利率", grp: "債息/波動", type: "ext", sym: "^TNX", dec: 3 },
  { id: "vix",  label: "VIX 波動率",     grp: "債息/波動", type: "ext", sym: "^VIX", dec: 2 },
  { id: "btc",  label: "比特幣",         grp: "加密", type: "ext", sym: "BTC-USD", dec: 0 },
  { id: "n225", label: "日經225(現貨)",  grp: "亞股現貨", type: "ext", sym: "^N225", dec: 0 },
  { id: "ks11", label: "韓股KOSPI(現貨)", grp: "亞股現貨", type: "ext", sym: "^KS11", dec: 2 }
];
const INST_BY_ID = {}; INSTRUMENTS.forEach(x => INST_BY_ID[x.id] = x);
function baseInstId(base) {                 // 由原始定義反查對應主清單 id（供下拉預選）
  const m = INSTRUMENTS.find(x =>
    (x.sym && x.sym === base.sym) || (x.code && x.code === base.code) ||
    (!base.sym && !base.code && x.prod && x.prod === base.prod && x.type === base.type));
  return m ? m.id : "";
}
function effMacroDef(base, i) {             // 套用設定覆蓋（宏觀卡：僅 ext 標的）
  const sel = (cfg && Array.isArray(cfg.macroSel)) ? cfg.macroSel[i] : null;
  const inst = sel && INST_BY_ID[sel];
  if (!inst || inst.type !== "ext") return base;
  return { ...base, sym: inst.sym, name: inst.label, dec: inst.dec != null ? inst.dec : base.dec };
}
function effMiniDef(base) {                 // 套用設定覆蓋（子圖：任意型別）
  const sel = (cfg && cfg.miniSel) ? cfg.miniSel[base.k] : null;
  const inst = sel && INST_BY_ID[sel];
  if (!inst) return base;
  return { k: base.k, cat: base.cat, label: inst.label, type: inst.type,
           sym: inst.sym, code: inst.code, prod: inst.prod, win: inst.win,
           dec: inst.dec != null ? inst.dec : base.dec };
}

// ---------- 右側宏觀對照卡片（各自完整互動線圖：時間切換＋拖曳平移＋滾輪縮放＋查價線）----------
const MACRO_DEFS = [
  { k: "es",  sym: "ES=F",  name: "S&P500 期", dec: 2 },
  { k: "twd", sym: "TWD=X", name: "美元/台幣", dec: 3 },
  { k: "nq",  sym: "NQ=F",  name: "那斯達克 期", dec: 2 },
  { k: "niy", sym: "NIY=F", name: "日經225 期", dec: 0 },
  { k: "ks",  sym: "^KS11", name: "韓股 KOSPI 指數", dec: 2 }
];
const MACRO_RANGES = [
  ["1日", "1d", "5m"], ["5日", "5d", "15m"], ["1月", "1mo", "1d"],
  ["3月", "3mo", "1d"], ["6月", "6mo", "1d"], ["1年", "1y", "1d"], ["5年", "5y", "1wk"]
];
let macroCharts = {};

function makeInstChart(host, def, startOpen) {
  const st = { data: { t: [], c: [] }, prev: null, last: null, ri: 0, zoom: 1, panX: 0, mouse: null, drag: null, loaded: false, open: false };
  host.innerHTML =
    '<div class="ic-head"><span class="ic-name">' + def.name + '</span>' +
    '<span class="ic-val" id="' + def.k + '_v">—</span><span class="ic-tg" id="' + def.k + '_tg">▸</span></div>' +
    '<div class="ic-body hidden" id="' + def.k + '_b"><div class="ic-ranges" id="' + def.k + '_r"></div>' +
    '<canvas class="ic-cv" id="' + def.k + '_c"></canvas><div class="ic-hint">拖曳平移｜滾輪縮放｜移動查價｜雙擊重置</div></div>';
  const head = host.querySelector(".ic-head"), body = host.querySelector(".ic-body"), cv = host.querySelector(".ic-cv"), rbox = host.querySelector(".ic-ranges"), tg = host.querySelector(".ic-tg");
  const ctx = cv.getContext("2d");
  let W = 0, H = 0, DPR = 1;
  const M = { l: 6, r: 52, t: 8, b: 18 };
  MACRO_RANGES.forEach((r, i) => {
    const b = document.createElement("button"); b.textContent = r[0]; b.className = "ic-rbtn" + (i === st.ri ? " sel" : "");
    b.onclick = (e) => { e.stopPropagation(); st.ri = i; st.zoom = 1; st.panX = 0; markRanges(); load(true); };
    rbox.appendChild(b);
  });
  function markRanges() { Array.prototype.forEach.call(rbox.children, (b, i) => b.className = "ic-rbtn" + (i === st.ri ? " sel" : "")); }
  head.onclick = () => { st.open = !st.open; body.classList.toggle("hidden", !st.open); tg.textContent = st.open ? "▾" : "▸"; if (st.open) { size(); if (!st.loaded) load(false); else draw(); } };
  function renderVal() {
    const el = document.getElementById(def.k + "_v"); if (!el) return;
    if (st.last == null) { el.textContent = "—"; return; }
    const chg = (st.prev != null) ? st.last - st.prev : null, pct = (chg != null && st.prev) ? chg / st.prev * 100 : null;
    const c = chg == null ? "" : (chg > 0 ? "pos" : (chg < 0 ? "neg" : "")), a = chg == null ? "" : (chg > 0 ? "▲" : (chg < 0 ? "▼" : ""));
    el.innerHTML = "<b>" + fmt(st.last, def.dec) + "</b> <span class='" + c + "'>" + a + (pct != null ? (pct > 0 ? "+" : "") + pct.toFixed(2) + "%" : "") + "</span>";
  }
  async function fetchHeader() {
    try { const d = await (await fetch("/api/ext?sym=" + encodeURIComponent(def.sym) + "&range=1d&interval=5m")).json();
      st.prev = d.prev; st.last = (d.last != null) ? d.last : (d.c && d.c.length ? d.c[d.c.length - 1] : st.last); renderVal();
    } catch (e) {}
  }
  async function load(reset) {
    const r = MACRO_RANGES[st.ri];
    try {
      let d = await (await fetch("/api/ext?sym=" + encodeURIComponent(def.sym) + "&range=" + r[1] + "&interval=" + r[2])).json();
      if ((!d.t || !d.t.length) && st.ri < MACRO_RANGES.length - 1) { st.ri++; markRanges(); const r2 = MACRO_RANGES[st.ri]; d = await (await fetch("/api/ext?sym=" + encodeURIComponent(def.sym) + "&range=" + r2[1] + "&interval=" + r2[2])).json(); }
      st.data = { t: d.t || [], c: d.c || [] };
      if (st.ri === 0 && st.data.t.length) {   // 「1日」只顯示台北當日（避免 ES/NIY 等24h期貨把昨天整段帶進來）
        const today = todayTW(), t2 = [], c2 = [];
        for (let i = 0; i < st.data.t.length; i++) if (dateOfTs(st.data.t[i]) === today) { t2.push(st.data.t[i]); c2.push(st.data.c[i]); }
        if (t2.length) st.data = { t: t2, c: c2 };
      }
      if (d.prev != null) st.prev = d.prev; if (d.last != null) st.last = d.last;
      st.loaded = true; if (reset) { st.zoom = 1; st.panX = 0; } renderVal(); size(); draw();
    } catch (e) { st.data = { t: [], c: [] }; draw(); }
  }
  function size() { DPR = window.devicePixelRatio || 1; W = cv.clientWidth; H = cv.clientHeight; if (!W || !H) return; cv.width = Math.round(W * DPR); cv.height = Math.round(H * DPR); ctx.setTransform(DPR, 0, 0, DPR, 0, 0); draw(); }
  const n = () => st.data.t.length;
  function ppb() { return Math.max(0.02, (W - M.l - M.r) / Math.max(n(), 2) * st.zoom); }
  function xOf(i) { return W - M.r + st.panX - (n() - 1 - i) * ppb(); }
  function iOfX(x) { return Math.round(n() - 1 - (W - M.r + st.panX - x) / ppb()); }
  function vis() { let i0 = Math.max(0, Math.floor(iOfX(M.l)) - 1), i1 = Math.min(n() - 1, Math.ceil(iOfX(W - M.r)) + 1); if (i1 < i0) { i0 = 0; i1 = n() - 1; } return [i0, i1]; }
  function draw() {
    if (!W || !H) return; ctx.clearRect(0, 0, W, H);
    if (!n()) { ctx.fillStyle = "#bbc"; ctx.font = "11px sans-serif"; ctx.textAlign = "center"; ctx.textBaseline = "middle"; ctx.fillText("此區間暫無資料", W / 2, H / 2); return; }
    const [i0, i1] = vis(); let mn = Infinity, mx = -Infinity;
    for (let i = i0; i <= i1; i++) { const v = st.data.c[i]; if (v != null) { if (v < mn) mn = v; if (v > mx) mx = v; } }
    if (!isFinite(mn)) { mn = 0; mx = 1; } if (mx - mn < 1e-9) { mn -= 1; mx += 1; }
    const pad = (mx - mn) * 0.1; mn -= pad; mx += pad;
    const yOf = v => M.t + (mx - v) / (mx - mn) * (H - M.t - M.b);
    ctx.font = "9px Consolas,monospace"; ctx.textAlign = "left"; ctx.textBaseline = "middle";
    const step = niceStep((mx - mn) / 4);
    for (let v = Math.ceil(mn / step) * step; v <= mx; v += step) { const y = yOf(v); ctx.strokeStyle = "#eef1f6"; ctx.beginPath(); ctx.moveTo(M.l, y); ctx.lineTo(W - M.r, y); ctx.stroke(); ctx.fillStyle = "#99a"; ctx.fillText(fmt(v, def.dec), W - M.r + 4, y); }
    if (st.prev != null && st.prev >= mn && st.prev <= mx) { const y = yOf(st.prev); ctx.strokeStyle = "#c5cede"; ctx.setLineDash([3, 3]); ctx.beginPath(); ctx.moveTo(M.l, y); ctx.lineTo(W - M.r, y); ctx.stroke(); ctx.setLineDash([]); }
    ctx.textAlign = "center"; ctx.textBaseline = "top"; ctx.fillStyle = "#99a";
    const every = Math.max(1, Math.round(64 / ppb())), iv = MACRO_RANGES[st.ri][2], intraday = (iv.indexOf("m") > -1);
    for (let i = i0; i <= i1; i++) { if (i % every !== 0) continue; const x = xOf(i); if (x < M.l || x > W - M.r) continue; const dt = new Date(st.data.t[i] * 1000); const lbl = intraday ? dt.toLocaleTimeString("zh-TW", { hour12: false, hour: "2-digit", minute: "2-digit" }) : (dt.getMonth() + 1) + "/" + dt.getDate(); ctx.fillText(lbl, x, H - M.b + 4); }
    const lastV = st.data.c[i1], base = (st.prev != null) ? st.prev : st.data.c[i0];
    ctx.save(); ctx.beginPath(); ctx.rect(M.l, M.t, W - M.l - M.r, H - M.t - M.b); ctx.clip();
    ctx.strokeStyle = (lastV != null && base != null && lastV >= base) ? "#c92a2a" : "#2b8a3e"; ctx.lineWidth = 1.4; ctx.beginPath();
    let started = false; const stride = Math.max(1, Math.floor((i1 - i0) / (W * 2)));
    for (let i = i0; i <= i1; i += stride) { const v = st.data.c[i]; if (v == null) { started = false; continue; } const x = xOf(i), y = yOf(v); if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y); }
    ctx.stroke(); ctx.restore();
    if (st.mouse && st.mouse.x > M.l && st.mouse.x < W - M.r) {
      const i = Math.max(i0, Math.min(i1, iOfX(st.mouse.x))), v = st.data.c[i];
      if (v != null) {
        const x = xOf(i), y = yOf(v);
        ctx.strokeStyle = "#bbc"; ctx.setLineDash([3, 3]); ctx.beginPath(); ctx.moveTo(x, M.t); ctx.lineTo(x, H - M.b); ctx.stroke(); ctx.setLineDash([]);
        ctx.fillStyle = "#1f4e8c"; ctx.beginPath(); ctx.arc(x, y, 2.5, 0, 7); ctx.fill();
        const dt = new Date(st.data.t[i] * 1000), tip = dt.toLocaleString("zh-TW", { hour12: false, month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }) + "  " + fmt(v, def.dec);
        ctx.font = "10px Consolas,monospace"; const tw = ctx.measureText(tip).width + 10; let bx = x + 8; if (bx + tw > W - M.r) bx = x - tw - 8;
        ctx.fillStyle = "rgba(255,255,255,.95)"; ctx.strokeStyle = "#c5cede"; ctx.fillRect(bx, M.t + 2, tw, 16); ctx.strokeRect(bx, M.t + 2, tw, 16);
        ctx.fillStyle = "#1a2233"; ctx.textAlign = "left"; ctx.textBaseline = "middle"; ctx.fillText(tip, bx + 5, M.t + 10);
      }
    }
  }
  function pos(e) { const r = cv.getBoundingClientRect(), t = e.touches ? e.touches[0] : e; return { x: t.clientX - r.left, y: t.clientY - r.top }; }
  cv.addEventListener("mousedown", e => { st.drag = { sx: e.clientX, p0: st.panX }; });
  document.addEventListener("mousemove", e => { if (st.drag) { st.panX = st.drag.p0 + (e.clientX - st.drag.sx); st.mouse = null; draw(); } else if (e.target === cv) { st.mouse = pos(e); draw(); } });
  document.addEventListener("mouseup", () => { st.drag = null; });
  cv.addEventListener("mouseleave", () => { if (!st.drag) { st.mouse = null; draw(); } });
  cv.addEventListener("wheel", e => { e.preventDefault(); st.zoom = Math.max(0.3, Math.min(50, st.zoom * (e.deltaY < 0 ? 1.15 : 0.87))); draw(); }, { passive: false });
  cv.addEventListener("dblclick", () => { st.zoom = 1; st.panX = 0; draw(); });
  cv.addEventListener("touchstart", e => { st.drag = { sx: e.touches[0].clientX, p0: st.panX }; }, { passive: true });
  cv.addEventListener("touchmove", e => { if (st.drag) { st.panX = st.drag.p0 + (e.touches[0].clientX - st.drag.sx); draw(); } }, { passive: true });
  cv.addEventListener("touchend", () => { st.drag = null; });
  if (startOpen) { st.open = true; body.classList.remove("hidden"); tg.textContent = "▾"; setTimeout(() => { if (!st.loaded) load(false); else { size(); draw(); } }, 30); }
  return { fetchHeader, refresh: () => { fetchHeader(); if (st.open) load(false); }, onResize: () => { if (st.open) size(); } };
}
function buildMacro() {
  const list = document.getElementById("macroCards"); if (!list || Object.keys(macroCharts).length) return;
  MACRO_DEFS.forEach((base, i) => { const def = effMacroDef(base, i); const d = document.createElement("div"); d.className = "card"; list.appendChild(d); macroCharts[base.k] = makeInstChart(d, def, true); macroCharts[base.k].fetchHeader(); });
}
function refreshMacroHeaders() { for (const k in macroCharts) macroCharts[k].refresh(); }
window.addEventListener("resize", () => { for (const k in macroCharts) macroCharts[k].onResize(); });

// ---------- 盤中子圖表（台灣＝當天開盤起；美股四大指數＋自訂 ticker 獨立時段） ----------
const MINI_BASE = [
  { k: "twii",  label: "加權指數（現貨）", type: "spot",   prod: "TX",  win: "tw",  cat: "live" },
  { k: "otc",   label: "櫃買指數（現貨）", type: "bench",  code: "TWS:OTC01:INDEX", win: "tw", cat: "live" },
  { k: "tsmc",  label: "台積電",           type: "bench",  code: "TWS:2330:STOCK", win: "tw", cat: "live" },
  { k: "fym",   label: "道瓊 期",          type: "ext",    sym: "YM=F",   cat: "live" },
  { k: "fes",   label: "S&P500 期",        type: "ext",    sym: "ES=F",   cat: "live" },
  { k: "fnq",   label: "那斯達克 期",      type: "ext",    sym: "NQ=F",   cat: "live" },
  { k: "frty",  label: "羅素2000 期",      type: "ext",    sym: "RTY=F",  cat: "live" },
  { k: "fbtc",  label: "比特幣 BTC",       type: "ext",    sym: "BTC-USD", cat: "live" },
  { k: "dji",   label: "道瓊（前夜收盤）", type: "us",     code: "^DJI",  cat: "overnight" },
  { k: "spx",   label: "S&P（前夜收盤）",  type: "us",     code: "^GSPC", cat: "overnight" },
  { k: "ixic",  label: "那指（前夜收盤）", type: "us",     code: "^IXIC", cat: "overnight" },
  { k: "ndx",   label: "那指100（前夜收盤）", type: "us",  code: "^NDX",  cat: "overnight" },
  { k: "sox",   label: "費半（前夜收盤）", type: "us",     code: "^SOX",  cat: "overnight" },
  { k: "dax",   label: "德國DAX（前夜收盤）", type: "ext", sym: "^GDAXI", cat: "overnight" },
  { k: "tnx",   label: "美10年債殖利率",   type: "ext",    sym: "^TNX",   cat: "overnight" },
  { k: "vix",   label: "VIX 波動率",       type: "ext",    sym: "^VIX",   cat: "overnight" }
];
const MINI_CATS = [["live", "盤中即時連動（台股盤中同步跳動）"], ["overnight", "前夜歐美收盤（白天靜止不動）"], ["custom", "自訂"]];
// 依設定組出完整子圖定義（基礎 ＋ 最多5個自訂 ticker）
function miniDefs() {
  const defs = MINI_BASE.map(effMiniDef);
  const tk = (cfg && Array.isArray(cfg.miniTickers)) ? cfg.miniTickers : [];
  tk.slice(0, 8).forEach((raw, idx) => {
    raw = (raw || "").trim();
    if (!raw) return;
    const isTW = /^\d{3,6}[A-Za-z]?$/.test(raw);
    defs.push({
      k: "ck" + idx, label: raw.toUpperCase(),
      type: "ticker", q: raw, us: !isTW, win: isTW ? "tw" : null, cat: "custom"
    });
  });
  return defs;
}
let miniDefsCur = MINI_BASE.slice();
let miniData = {}, miniBuilt = false;

// 各子圖時間窗
function miniWinOf(def, d) {
  if (def.type === "ext") {
    if (!d || !d.t.length) return null;
    const tEnd = d.t[d.t.length - 1];
    if (def.cat === "usf") {   // 美股指數期貨(近24h交易)：從今早6點(台北)起，不含前日收盤
      const base = new Date(tEnd * 1000);
      const tw = new Date(base.toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
      const off = tw.getTime() - base.getTime();
      const s = new Date(tw); s.setHours(6, 0, 0, 0);
      let t0 = Math.floor((s.getTime() - off) / 1000);
      if (t0 < d.t[0] || t0 >= tEnd) t0 = d.t[0];   // 今晨6點尚無資料則退回全部
      return [t0, tEnd];
    }
    return [d.t[0], tEnd];   // 其餘(DXY/債息/VIX/日幣等)依資料自身涵蓋區間
  }
  if (def.type === "us" || def.us) {
    if (d && d.t.length) return [d.t[0], d.t[d.t.length - 1]];   // 含盤前盤後：依實測延長時段(盤前4amET~盤後8pmET)
    return null;
  }
  // 台灣：以資料最後一筆的日期為「當天」（盤前自動顯示前一交易日）
  const base = (d && d.t.length) ? new Date(d.t[d.t.length - 1] * 1000) : new Date();
  const tw = new Date(base.toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
  const off = tw.getTime() - base.getTime();
  const s = new Date(tw);
  let span;
  if (def.win === "fx") { s.setHours(9, 0, 0, 0); span = 7 * 3600; }      // 台北匯市 09:00–16:00
  else { s.setHours(8, 45, 0, 0); span = 5 * 3600; }                       // 08:45–13:45
  const t0 = Math.floor((s.getTime() - off) / 1000);
  return [t0, t0 + span];
}

function buildMiniGrid() {
  if (miniBuilt) return;
  const host = document.getElementById("miniGrid");
  if (!host) return;
  host.innerHTML = "";
  const byCat = {};
  for (const d of miniDefsCur) { const c = d.cat || "custom"; (byCat[c] = byCat[c] || []).push(d); }
  for (const [ck, clabel] of MINI_CATS) {
    const list = byCat[ck];
    if (!list || !list.length) continue;
    const sec = document.createElement("div"); sec.className = "minicat";
    const hd = document.createElement("div"); hd.className = "minicat-h"; hd.textContent = clabel;
    sec.appendChild(hd);
    const grid = document.createElement("div"); grid.className = "minigrid";
    for (const d of list) {
      const div = document.createElement("div");
      div.className = "mini";
      div.innerHTML = "<div class='mt'><span id='mt_" + d.k + "'>" + d.label + "</span><span class='mv' id='mv_" + d.k + "'>—</span></div><canvas id='mc_" + d.k + "'></canvas>";
      grid.appendChild(div);
      const cvm = div.querySelector("canvas");
      cvm.addEventListener("mousemove", e => miniHover(d.k, e));
      cvm.addEventListener("mouseleave", () => { drawMini(d.k); updateMiniValue(d.k, null); });
    }
    sec.appendChild(grid);
    host.appendChild(sec);
  }
  miniBuilt = true;
}

async function loadMinis() {
  if (!cfg) await loadCfgFromServer();
  miniDefsCur = miniDefs();
  buildMiniGrid();
  const cacheH = {}, benches = {}, tickers = {}, exts = {};
  let usidx = {};
  const tasks = [];
  for (const p of ["TX", "TE", "TF", "XIF"])
    tasks.push(fetch("/api/history?product=" + p + "&res=5").then(r => r.json()).then(d => { cacheH[p] = d; }).catch(() => {}));
  for (const d of miniDefsCur) {
    if (d.type === "bench")
      tasks.push(fetch("/api/bench?code=" + encodeURIComponent(d.code) + "&res=5").then(r => r.json()).then(x => { benches[d.code] = x; }).catch(() => {}));
    else if (d.type === "ticker")
      tasks.push(fetch("/api/ticker?q=" + encodeURIComponent(d.q)).then(r => r.json()).then(x => { tickers[d.q] = x; }).catch(() => {}));
    else if (d.type === "ext")
      tasks.push(fetch("/api/ext?sym=" + encodeURIComponent(d.sym) + "&range=1d&interval=5m").then(r => r.json()).then(x => { exts[d.sym] = x; }).catch(() => {}));
  }
  tasks.push(fetch("/api/usidx").then(r => r.json()).then(x => { usidx = x || {}; }).catch(() => {}));
  await Promise.all(tasks);
  for (const def of miniDefsCur) {
    const t = [], v = [];
    let prev = null, sess = null;
    if (def.type === "spread") {
      const h = cacheH[def.prod];
      if (h && h.t) for (let i = 0; i < h.t.length; i++)
        if (h.fut[i] != null && h.spot[i] != null) { t.push(h.t[i]); v.push(Math.round((h.fut[i] - h.spot[i]) * 100) / 100); }
    } else if (def.type === "spot") {
      const h = cacheH[def.prod];
      if (h && h.t) for (let i = 0; i < h.t.length; i++)
        if (h.spot[i] != null) { t.push(h.t[i]); v.push(h.spot[i]); }
      // 漲跌幅基準＝前一交易日收盤（cnyes 指數昨收，field 21），非當日開盤
      const qi = lastRaw && lastRaw.cnyes ? lastRaw.cnyes[CNYES_IDX[def.prod]] : null;
      if (qi) prev = num(qi.prev);
    } else if (def.type === "bench") {
      const b = benches[def.code];
      if (b && b.t) for (let i = 0; i < b.t.length; i++)
        if (b.c[i] != null) { t.push(b.t[i]); v.push(b.c[i]); }
      // 漲跌幅基準＝前一交易日收盤（cnyes 昨收，field 21），非當日開盤
      const qb = lastRaw && lastRaw.cnyes ? lastRaw.cnyes[def.code] : null;
      if (qb) prev = num(qb.prev);
    } else if (def.type === "ticker") {
      const x = tickers[def.q];
      if (x && x.t) for (let i = 0; i < x.t.length; i++)
        if (x.c[i] != null) { t.push(x.t[i]); v.push(x.c[i]); }
      if (x) { prev = x.prev; sess = x.sess || null; }
      // 數字代號難辨識：改用資料源回傳的公司名＋正確市場名顯示
      if (x && (x.name || x.mkt)) {
        def.label = (x.name || def.q.toUpperCase()) + (x.mkt ? "（" + x.mkt + "）" : "");
        const lab = document.getElementById("mt_" + def.k);
        if (lab) lab.textContent = def.label;
      }
    } else if (def.type === "ext") {
      const x = exts[def.sym];
      if (x && x.t) {
        const tdy = todayTW();
        for (let i = 0; i < x.t.length; i++)
          if (x.c[i] != null && dateOfTs(x.t[i]) === tdy) { t.push(x.t[i]); v.push(x.c[i]); }
        if (!t.length) for (let i = 0; i < x.t.length; i++)   // 當日尚無資料則退回全部
          if (x.c[i] != null) { t.push(x.t[i]); v.push(x.c[i]); }
      }
      if (x) { prev = x.prev; sess = x.sess || null; }
    } else {
      const u = usidx[def.code];
      if (u && u.t) for (let i = 0; i < u.t.length; i++)
        if (u.c[i] != null) { t.push(u.t[i]); v.push(u.c[i]); }
      if (u) { prev = u.prev; sess = u.sess || null; }
    }
    const data = { t, v, isSpread: def.type === "spread", def, prev, sess };
    data.win = miniWinOf(def, data);
    miniData[def.k] = data;
    drawMini(def.k);
    updateMiniValue(def.k, null);
  }
}

function drawMini(k, hi) {
  const cv2 = document.getElementById("mc_" + k);
  const d = miniData[k];
  if (!cv2) return;
  const dpr = window.devicePixelRatio || 1;
  const w = cv2.clientWidth, h = cv2.clientHeight;
  if (!w || !h) return;
  if (cv2.width !== Math.round(w * dpr)) { cv2.width = Math.round(w * dpr); cv2.height = Math.round(h * dpr); }
  const c = cv2.getContext("2d");
  c.setTransform(dpr, 0, 0, dpr, 0, 0);
  c.clearRect(0, 0, w, h);
  if (!d || !d.t.length || !d.win) {
    c.fillStyle = "#bbc"; c.font = "11px sans-serif"; c.textAlign = "center"; c.textBaseline = "middle";
    c.fillText("暫無資料", w / 2, h / 2);
    return;
  }
  const t0 = d.win[0], t1 = d.win[1];
  // 盤前/盤後陰影＋規則時段開收分隔線（US 個股/指數；最背層）
  if (d.sess && d.def.type !== "ext") {
    const xc = tt => Math.max(0, Math.min(w, (tt - t0) / (t1 - t0) * w));
    c.fillStyle = "rgba(120,130,150,.09)";
    for (const seg of [d.sess.pre, d.sess.post]) {
      if (!seg) continue; const x1 = xc(seg[0]), x2 = xc(seg[1]);
      if (x2 - x1 > 0.5) c.fillRect(x1, 2, x2 - x1, h - 16);
    }
    if (d.sess.reg) {
      c.strokeStyle = "#cdd5e0"; c.setLineDash([3, 3]);
      for (const b of d.sess.reg) { const x = xc(b); if (x > 0.5 && x < w - 0.5) { c.beginPath(); c.moveTo(x, 2); c.lineTo(x, h - 14); c.stroke(); } }
      c.setLineDash([]);
    }
  }
  // 軸線與刻度
  c.strokeStyle = "#dde3ec"; c.beginPath(); c.moveTo(0, h - 14); c.lineTo(w, h - 14); c.stroke();
  c.font = "9.5px sans-serif"; c.textBaseline = "bottom";
  const wide = w >= 380, narrow = w < 190;   // narrow＝手機兩欄窄卡，刻度減量避免重疊
  const tks = [];
  if (d.def.type === "ext") {
    const nSegE = narrow ? 2 : 4;
    for (let k2 = 0; k2 <= nSegE; k2++) tks.push(t0 + k2 * (t1 - t0) / nSegE);
  } else if (d.def.type === "us" || d.def.us) {
    const span = t1 - t0, nSeg = narrow ? 2 : (span > 8 * 3600 ? 5 : 4);   // 含盤前盤後(較長)→多一格
    for (let k2 = 0; k2 <= nSeg; k2++) tks.push(t0 + k2 * span / nSeg);
  } else if (d.def.win === "fx") {
    for (const k2 of (wide ? [0, 1, 2, 3, 4, 5, 6, 7] : (narrow ? [0, 3, 6] : [0, 2, 4, 6]))) tks.push(t0 + k2 * 3600);
  } else {
    // 08:45起：刻度取整點 09:00–13:00
    for (const k2 of (wide ? [0, 1, 2, 3, 4] : [0, 2, 4])) tks.push(t0 + 900 + k2 * 3600);
  }
  for (const tt of tks) {
    if (tt < t0 || tt > t1) continue;
    const x = (tt - t0) / (t1 - t0) * w;
    c.strokeStyle = "#f0f3f8"; c.beginPath(); c.moveTo(x, 2); c.lineTo(x, h - 14); c.stroke();
    c.fillStyle = "#99a";
    c.textAlign = x < 20 ? "left" : (x > w - 20 ? "right" : "center");
    c.fillText(new Date(tt * 1000).toLocaleTimeString("zh-TW", { timeZone: "Asia/Taipei", hour12: false, hour: "2-digit", minute: "2-digit" }), x, h - 1);
  }
  c.textAlign = "center";
  const pts = [];
  for (let i = 0; i < d.t.length; i++) if (d.t[i] >= t0 && d.t[i] <= t1) pts.push([d.t[i], d.v[i]]);
  if (!pts.length) {
    c.fillStyle = "#bbc"; c.font = "11px sans-serif"; c.textBaseline = "middle";
    c.fillText("此時段無資料", w / 2, (h - 14) / 2);
    return;
  }
  let mn = Infinity, mx = -Infinity;
  for (const pv of pts) { if (pv[1] < mn) mn = pv[1]; if (pv[1] > mx) mx = pv[1]; }
  if (d.isSpread) { mn = Math.min(mn, 0); mx = Math.max(mx, 0); }
  if (mx - mn < 1e-9) { mn -= 1; mx += 1; }
  const pad = (mx - mn) * 0.12;
  mn -= pad; mx += pad;
  const xOf2 = tt => (tt - t0) / (t1 - t0) * w;
  const yOf2 = vv => 4 + (mx - vv) / (mx - mn) * (h - 22);
  const base = d.isSpread ? 0 : (d.prev != null ? d.prev : pts[0][1]);
  if (base >= mn && base <= mx) {
    c.strokeStyle = "#c5cede"; c.setLineDash([3, 3]);
    c.beginPath(); c.moveTo(0, yOf2(base)); c.lineTo(w, yOf2(base)); c.stroke(); c.setLineDash([]);
  }
  const lastV = pts[pts.length - 1][1];
  c.strokeStyle = lastV >= base ? "#c92a2a" : "#2b8a3e"; c.lineWidth = 1.4;
  c.beginPath();
  let st = false, prevT = 0;
  for (const pv of pts) {
    const x = xOf2(pv[0]), y = yOf2(pv[1]);
    if (!st || pv[0] - prevT > 1800) { c.moveTo(x, y); st = true; } else c.lineTo(x, y);
    prevT = pv[0];
  }
  c.stroke();
  if (hi != null && d.t[hi] >= t0 && d.t[hi] <= t1) {
    c.fillStyle = "#1f4e8c";
    c.beginPath(); c.arc(xOf2(d.t[hi]), yOf2(d.v[hi]), 3, 0, 7); c.fill();
  }
}

function updateMiniValue(k, hi) {
  const el = document.getElementById("mv_" + k);
  const d = miniData[k];
  if (!el) return;
  if (!d || !d.t.length || !d.win) { el.textContent = "—"; return; }
  if (hi != null) {
    el.innerHTML = "<span class='dim'>" + new Date(d.t[hi] * 1000).toLocaleTimeString("zh-TW", { timeZone: "Asia/Taipei", hour12: false, hour: "2-digit", minute: "2-digit" }) + "</span> <b>" + fmt(d.v[hi]) + "</b>";
    return;
  }
  const t0 = d.win[0], t1 = d.win[1];
  let firstI = -1, lastI = -1;
  for (let i = 0; i < d.t.length; i++) {
    if (d.t[i] < t0 || d.t[i] > t1) continue;
    if (firstI < 0) firstI = i;
    lastI = i;
  }
  if (lastI < 0) { el.textContent = "—"; return; }
  const lastV = d.v[lastI];
  const ref = d.isSpread ? d.v[firstI] : (d.prev != null ? d.prev : d.v[firstI]);
  const chg = lastV - ref;
  const pct = (!d.isSpread && ref) ? chg / ref * 100 : null;
  el.innerHTML = "<b>" + fmt(lastV) + "</b> <span class='" + cls(chg) + "'>" +
    (chg > 0 ? "▲" : (chg < 0 ? "▼" : "")) +
    (d.isSpread ? fmt(Math.abs(chg)) : (pct != null ? Math.abs(pct).toFixed(2) + "%" : "")) + "</span>";
}

function miniHover(k, e) {
  const d = miniData[k];
  const cv2 = document.getElementById("mc_" + k);
  if (!d || !d.t.length || !d.win || !cv2) return;
  const r = cv2.getBoundingClientRect();
  const t0 = d.win[0], t1 = d.win[1];
  const tt = t0 + (e.clientX - r.left) / r.width * (t1 - t0);
  let best = -1, bd = Infinity;
  for (let i = 0; i < d.t.length; i++) {
    if (d.t[i] < t0 || d.t[i] > t1) continue;
    const dd = Math.abs(d.t[i] - tt);
    if (dd < bd) { bd = dd; best = i; }
  }
  if (best >= 0) { drawMini(k, best); updateMiniValue(k, best); }
}
window.addEventListener("resize", () => { for (const d of miniDefsCur) drawMini(d.k); });

// ---------- 設定面板 ----------
let paramAuthed = false;
function fillParamPanel() {
  if (!cfg) return;
  document.getElementById("cfgText").value = JSON.stringify({ seedDate: cfg.seedDate, seedRemaining: cfg.seedRemaining, daily: cfg.daily }, null, 1);
  document.getElementById("twnSpot").value = cfg.twnSpot || "";
  document.getElementById("costRate").value = (cfg.costRate != null ? cfg.costRate : 1.5);
  const tk = (cfg && Array.isArray(cfg.miniTickers)) ? cfg.miniTickers : [];
  for (let i = 0; i < 8; i++) document.getElementById("tk" + i).value = tk[i] || "";
  buildSlotCfg();
  buildRefBarCfg();
}
// 參數設定入口（密碼 0168）
async function openParams() {
  const el = document.getElementById("cfgPanel");
  if (!el.classList.contains("hidden")) { el.classList.add("hidden"); return; }
  if (!paramAuthed) {
    const pw = prompt("請輸入參數設定密碼：");
    if (pw == null) return;
    if (pw !== "0168") { alert("密碼錯誤"); return; }
    paramAuthed = true;
  }
  if (!cfg) await loadCfgFromServer();
  fillParamPanel();
  el.classList.remove("hidden");
}
function togglePanel(id) {
  const el = document.getElementById(id);
  el.classList.toggle("hidden");
  if (!el.classList.contains("hidden") && cfg) fillParamPanel();
}
async function saveTickers() {
  const arr = [];
  for (let i = 0; i < 8; i++) arr.push(document.getElementById("tk" + i).value.trim());
  await postCfg({ ...cfg, miniTickers: arr });
  document.getElementById("tkMsg").textContent = "已儲存 ✓";
  setTimeout(() => document.getElementById("tkMsg").textContent = "", 2500);
  miniBuilt = false;                       // 代號變更：重建子圖表格
  await loadMinis();
}
function instOptions(selId, extOnly) {
  let html = "", grp = "";
  for (const x of INSTRUMENTS) {
    if (extOnly && x.type !== "ext") continue;
    if (x.grp !== grp) { if (grp) html += "</optgroup>"; html += "<optgroup label='" + x.grp + "'>"; grp = x.grp; }
    html += "<option value='" + x.id + "'" + (x.id === selId ? " selected" : "") + ">" + x.label + "</option>";
  }
  if (grp) html += "</optgroup>";
  return html;
}
function buildSlotCfg() {
  const host = document.getElementById("slotCfg"); if (!host) return;
  let h = "<div style='font-weight:600;margin:4px 0'>上方宏觀卡片（5）｜僅國際標的</div><div class='row' style='flex-wrap:wrap;gap:6px'>";
  MACRO_DEFS.forEach((base, i) => {
    const cur = (cfg && cfg.macroSel && cfg.macroSel[i]) || baseInstId(base);
    h += "<select data-mk='" + i + "' style='width:155px'>" + instOptions(cur, true) + "</select>";
  });
  h += "</div>";
  for (const [ck, clabel] of MINI_CATS) {
    if (ck === "custom") continue;
    const slots = MINI_BASE.filter(b => (b.cat || "custom") === ck);
    if (!slots.length) continue;
    h += "<div style='font-weight:600;margin:8px 0 4px'>" + clabel + "</div><div class='row' style='flex-wrap:wrap;gap:6px'>";
    for (const base of slots) {
      const cur = (cfg && cfg.miniSel && cfg.miniSel[base.k]) || baseInstId(base);
      h += "<select data-bk='" + base.k + "' style='width:155px'>" + instOptions(cur, false) + "</select>";
    }
    h += "</div>";
  }
  host.innerHTML = h;
}
function rebuildMacro() {
  const list = document.getElementById("macroCards");
  if (list) list.innerHTML = "";
  macroCharts = {};
  buildMacro();
}
async function saveSlots() {
  const macroSel = [];
  document.querySelectorAll("#slotCfg select[data-mk]").forEach(s => { macroSel[+s.getAttribute("data-mk")] = s.value; });
  const miniSel = {};
  document.querySelectorAll("#slotCfg select[data-bk]").forEach(s => { miniSel[s.getAttribute("data-bk")] = s.value; });
  await postCfg({ ...cfg, macroSel, miniSel });
  document.getElementById("slotMsg").textContent = "已儲存 ✓ 重建中…";
  setTimeout(() => document.getElementById("slotMsg").textContent = "", 2500);
  rebuildMacro();
  miniBuilt = false; await loadMinis();
}
async function resetSlots() {
  await postCfg({ ...cfg, macroSel: null, miniSel: null });
  buildSlotCfg();
  rebuildMacro();
  miniBuilt = false; await loadMinis();
  document.getElementById("slotMsg").textContent = "已回復預設 ✓";
  setTimeout(() => document.getElementById("slotMsg").textContent = "", 2500);
}
const REFBAR_STD = [
  ["TXF", "台指期(含夜盤)"],
  ["TWS:2330:STOCK", "台積電"], ["TWS:2317:STOCK", "鴻海"], ["TWS:2454:STOCK", "聯發科"],
  ["TWS:0050:STOCK", "元大台灣50"], ["TWS:OTC01:INDEX", "櫃買指數"], ["FX:USDTWD:FOREX", "美元/台幣"],
  ["GI:NDX:INDEX", "NASDAQ100"], ["GI:DJI:INDEX", "道瓊指數"],
  ["TSM", "台積電ADR"], ["^SOX", "費城半導體"], ["^N225", "日經225"], ["^KS11", "韓國KOSPI"],
  ["YM=F", "道瓊期"], ["ES=F", "S&P期"], ["NQ=F", "那斯達克期"], ["RTY=F", "羅素2000期"],
  ["^TNX", "美10年債息"], ["BZ=F", "Brent原油"], ["BTC-USD", "比特幣"]
];
function buildRefBarCfg() {
  const on = document.getElementById("rbOn"); if (!on) return;
  on.checked = !!(cfg && cfg.refBarOn);
  const hide = new Set((cfg && cfg.refBarHide) || []);
  let h = "";
  for (const [k, label] of REFBAR_STD)
    h += "<label style='white-space:nowrap'><input type='checkbox' data-rb='" + k + "'" + (hide.has(k) ? "" : " checked") + "> " + label + "</label>";
  document.getElementById("rbStd").innerHTML = h;
  document.getElementById("rbAdd").value = ((cfg && cfg.refBarAdd) || []).join(", ");
}
async function saveRefBar() {
  const refBarOn = document.getElementById("rbOn").checked;
  const refBarHide = [];
  document.querySelectorAll("#rbStd input[data-rb]").forEach(c => { if (!c.checked) refBarHide.push(c.getAttribute("data-rb")); });
  const refBarAdd = document.getElementById("rbAdd").value.split(/[,\s]+/).map(s => s.trim()).filter(Boolean).slice(0, 12);
  await postCfg({ ...cfg, refBarOn, refBarHide, refBarAdd });
  document.getElementById("rbMsg").textContent = "已儲存 ✓";
  setTimeout(() => document.getElementById("rbMsg").textContent = "", 2500);
  refresh();
}
async function saveCfg() {
  try {
    const v = JSON.parse(document.getElementById("cfgText").value);
    if (!v.seedDate || !v.seedRemaining || !Array.isArray(v.daily)) throw new Error("缺少 seedDate / seedRemaining / daily");
    await postCfg({ ...cfg, seedDate: v.seedDate, seedRemaining: v.seedRemaining, daily: v.daily });
    document.getElementById("cfgMsg").textContent = "已儲存 ✓";
    setTimeout(() => document.getElementById("cfgMsg").textContent = "", 2500);
    refresh();
  } catch (e) { document.getElementById("cfgMsg").textContent = "格式錯誤：" + e.message; }
}
async function resetCfg() {
  await postCfg({ reset: true });
  fillParamPanel();   // 還原後重建整個面板(含成本利率、ticker、子圖下拉、對照行情),避免殘留舊值
  refresh();
}
async function saveTwnSpot() {
  const v = parseFloat(document.getElementById("twnSpot").value);
  const val = isNaN(v) ? null : v;
  const taiex = (lastResult && lastResult.TX) ? lastResult.TX.spot : null;
  // 輸入今日現貨值即以「值÷當前 TAIEX」重新校準比例；之後自動隨 TAIEX 推估到當日
  const ratio = (val != null && taiex) ? Math.round(val / taiex * 1e6) / 1e6 : (cfg ? cfg.twnSpotRatio : null);
  const note = (val != null && ratio && taiex)
    ? "校準 " + todayTW() + "（TAIEX " + taiex + "×" + ratio + "）"
    : "手動輸入 " + todayTW();
  await postCfg({ ...cfg, twnSpot: val, twnSpotRatio: ratio, twnSpotNote: note });
  refresh();
}

// ---------- 自動更新 ----------
function toggleAuto() {
  autoOn = !autoOn;
  const b = document.getElementById("btnAuto");
  b.textContent = "自動更新：" + (autoOn ? "開" : "關");
  b.className = autoOn ? "on" : "";
  schedule();
}
function schedule() {
  if (timer) clearInterval(timer);
  if (autoOn) timer = setInterval(() => { refresh(); }, 60000);   // 背景分頁也持續更新(不再因 document.hidden 暫停)
}

// ---------- 圖表左右寬度可拖曳調整 ----------
function setupChartSplit() {
  const split = document.getElementById("chartSplit");
  const row = split ? split.closest(".chartrow") : null;
  if (!split || !row) return;
  let dragging = false;
  const onMove = (e) => {
    if (!dragging) return;
    const pt = e.touches ? e.touches[0] : e;
    const rect = row.getBoundingClientRect();
    const w = Math.max(420, Math.min(rect.width - 360, pt.clientX - rect.left));
    row.style.setProperty("--leftw", w + "px");
    sizeCanvas();
    if (e.cancelable) e.preventDefault();
  };
  const stop = () => { if (dragging) { dragging = false; document.body.style.userSelect = ""; sizeCanvas(); } };
  split.addEventListener("mousedown", (e) => { dragging = true; document.body.style.userSelect = "none"; e.preventDefault(); });
  split.addEventListener("touchstart", () => { dragging = true; }, { passive: true });
  document.addEventListener("mousemove", onMove);
  document.addEventListener("touchmove", onMove, { passive: false });
  document.addEventListener("mouseup", stop);
  document.addEventListener("touchend", stop);
}

// ---------- 初始化 ----------
document.getElementById("btnAuto").className = "on";
buildHistTabs();
sizeCanvas();
setupChartSplit();
schedule();
// 宏觀卡片必須等 cfg 載入後才建立，否則 effMacroDef 讀到 cfg=null 會退回預設值，
// 造成 macroSel 覆蓋每次重整都失效（例如第三張變回那斯達克期）
(async () => { await loadCfgFromServer(); buildMacro(); await refresh(); await loadHist(); })();
startStream();
</script>
</body>
</html>"""


# ---------------------------------------------------------------- HTTP 伺服器
class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _query(self):
        if "?" not in self.path:
            return {}
        out = {}
        for kv in self.path.split("?", 1)[1].split("&"):
            if "=" in kv:
                k, v = kv.split("=", 1)
                out[k] = v
        return out

    def do_GET(self):
        path = self.path.split("?")[0]
        try:
            if path in ("/", "/index.html"):
                self._send(200, HTML, "text/html; charset=utf-8")
            elif path == "/api/quotes":
                self._send(200, json.dumps(fetch_all(), ensure_ascii=False))
            elif path == "/api/config":
                self._send(200, json.dumps(load_cfg(), ensure_ascii=False))
            elif path == "/api/usidx":
                self._send(200, json.dumps(fetch_usidx(), ensure_ascii=False,
                                           separators=(",", ":")))
            elif path == "/api/bench":
                q = self._query()
                code = urllib.parse.unquote(q.get("code", ""))
                bres = "5" if q.get("res") == "5" else "D"
                if code not in BENCH_ALLOWED:
                    self._send(400, '{"error":"code"}')
                    return
                self._send(200, json.dumps(get_bench(code, bres), ensure_ascii=False,
                                           separators=(",", ":")))
            elif path == "/api/ticker":
                q = self._query()
                raw = urllib.parse.unquote(q.get("q", "")).strip()
                if not raw or not re.fullmatch(r"[A-Za-z0-9.\-^]{1,12}", raw):
                    self._send(400, '{"error":"q"}')
                    return
                self._send(200, json.dumps(get_ticker(raw), ensure_ascii=False,
                                           separators=(",", ":")))
            elif path == "/api/ext":
                q = self._query()
                sym = urllib.parse.unquote(q.get("sym", ""))
                rng = q.get("range", "1d")
                iv = q.get("interval", "5m")
                if sym not in EXT_ALLOWED or rng not in EXT_RANGES or iv not in EXT_INTERVALS:
                    self._send(400, '{"error":"param"}')
                    return
                self._send(200, json.dumps(get_ext(sym, rng, iv), ensure_ascii=False,
                                           separators=(",", ":")))
            elif path == "/api/meta":
                with _snap_lock:
                    n = len(_snaps)
                    last = _snaps[-1]["t"] if _snaps else 0
                self._send(200, json.dumps({"boot": BOOT_ID, "port": PORT,
                                            "snapCount": n, "lastSnapT": last,
                                            "now": int(time.time())},
                                           ensure_ascii=False, separators=(",", ":")))
            elif path == "/api/stream":
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Connection", "keep-alive")
                self.send_header("X-Accel-Buffering", "no")
                self.end_headers()
                q = queue.Queue(maxsize=2000)
                with _sse_lock:
                    _sse_subs.add(q)
                try:
                    self.wfile.write(b": connected\n\n")
                    self.wfile.flush()
                    while True:
                        try:
                            payload = q.get(timeout=20)
                        except queue.Empty:
                            payload = b": ping\n\n"   # keepalive
                        self.wfile.write(payload)
                        self.wfile.flush()
                except Exception:
                    pass
                finally:
                    with _sse_lock:
                        _sse_subs.discard(q)
                return
            elif path == "/api/context":
                refresh_inst()
                self._send(200, json.dumps(get_context(), ensure_ascii=False,
                                           separators=(",", ":")))
            elif path == "/api/snapshots":
                q = self._query()
                after = int(q.get("after", "0") or 0)
                self._send(200, json.dumps({"snaps": get_snaps(after)},
                                           ensure_ascii=False, separators=(",", ":")))
            elif path == "/api/history":
                q = self._query()
                p = q.get("product", "TX")
                if p not in ("TX", "TE", "TF", "XIF"):
                    self._send(400, '{"error":"product"}')
                    return
                if q.get("res") == "5":
                    self._send(200, json.dumps(intraday_5m(p), ensure_ascii=False,
                                               separators=(",", ":")))
                else:
                    data = refresh_daily()
                    self._send(200, json.dumps(data.get(p, {}), ensure_ascii=False,
                                               separators=(",", ":")))
            elif path == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
            else:
                self._send(404, '{"error":"not found"}')
        except Exception as e:
            self._send(500, json.dumps({"error": str(e)[:200]}))

    def do_POST(self):
        path = self.path.split("?")[0]
        try:
            if path == "/api/config":
                n = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(n).decode("utf-8")) if n else {}
                if body.get("reset"):
                    cfg = json.loads(json.dumps(DEFAULT_CFG))
                else:
                    if not (body.get("seedDate") and body.get("seedRemaining")
                            and isinstance(body.get("daily"), list)):
                        self._send(400, '{"error":"缺少 seedDate / seedRemaining / daily"}')
                        return
                    cfg = {**load_cfg(), **body}   # 與既有設定合併，避免部分欄位 POST 時清掉 miniSel/macroSel 等
                save_cfg(cfg)
                self._send(200, json.dumps(cfg, ensure_ascii=False))
            else:
                self._send(404, '{"error":"not found"}')
        except Exception as e:
            self._send(500, json.dumps({"error": str(e)[:200]}))

    def log_message(self, fmt, *args):
        msg = (args[0] if args else "")
        if "/api/quotes" not in msg and "/api/snapshots" not in msg:
            sys.stderr.write("[%s] %s\n" % (self.address_string(), fmt % args))


def _background_init():
    """啟動：載入累積資料 → 增量補抓 → 每 30 分鐘自動累積"""
    load_snaps()
    load_daily_cache()
    load_intra_cache()
    load_inst_cache()
    load_bench_cache()
    with _daily_lock:
        n0 = sum(len(v.get("d", [])) for v in _daily["data"].values())
    with _intra_lock:
        n5 = sum(len(v.get("t", [])) for v in _intra["data"].values())
    print("已載入累積資料：日線 %d 筆、5分線 %d 根、快照 %d 筆" % (n0, n5, len(_snaps)))
    while True:
        try:
            data = refresh_daily()          # 有需要才打 API，否則直接用累積檔
            n = sum(len(v.get("d", [])) for v in data.values())
            if n != n0:
                print("日線累積更新：共 %d 筆" % n)
                n0 = n
            for p in ["TX", "TE", "TF", "XIF"]:
                intraday_5m(p)              # 同樣按需增量累積
            refresh_inst()                  # 外資/PCR/OI（1小時間隔內不重打）
            with _bench_lock:
                bkeys = [c for c, v in _bench["data"].items() if "5" in v]
            for c in bkeys:
                get_bench(c, "5")           # 持續累積已看過的對標5分線
        except Exception as e:
            print("背景累積異常：%s" % str(e)[:100])
        time.sleep(1800)


def _snapshot_loop():
    """伺服器端背景每約1分鐘記一筆快照（不依賴前端開頁），盤中現貨/價差才會持續累積、5分線才能即時補現貨"""
    while True:
        interval = SNAP_MIN_INTERVAL
        try:
            if _in_session():
                fetch_all()          # 內部會 record_snapshot；報價快取過期才真的抓
            else:
                interval = 120
        except Exception:
            pass
        time.sleep(interval)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    try:
        server = ThreadingHTTPServer((HOST, PORT), Handler)
    except OSError:
        print("埠號 %d 已被占用：可能已有另一個監控程式在執行。" % PORT)
        print("請先關閉舊的視窗，或改用其他埠號啟動：python 台指期價差監控.py %d" % (PORT + 1))
        return
    ips = lan_ips()
    print("=" * 58)
    print("台指期正逆價差即時監控 v2 已啟動")
    print("本機開啟：    http://127.0.0.1:%d/" % PORT)
    if ips:
        for ip in ips:
            print("區網開啟：    http://%s:%d/" % (ip, PORT))
    else:
        print("區網開啟：    （未偵測到區網 IP，請以 ipconfig 確認）")
    print("-" * 58)
    print("若區網其他裝置無法連線：請允許 Windows 防火牆對 Python 的")
    print("「私人網路」存取，或以系統管理員執行：")
    print('  netsh advfirewall firewall add rule name="SpreadMon" dir=in action=allow protocol=TCP localport=%d' % PORT)
    print("-" * 58)
    print("資料檔目錄：  %s" % BASE)
    print("停止伺服器：  Ctrl + C")
    print("=" * 58)
    threading.Thread(target=_background_init, daemon=True).start()
    threading.Thread(target=_snapshot_loop, daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止。")


if __name__ == "__main__":
    main()
