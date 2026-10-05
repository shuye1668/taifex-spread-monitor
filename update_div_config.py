# -*- coding: utf-8 -*-
r"""
除息點數自動更新（2026-10-02 新增）
================================
原本「除息資料設定」要人工照券商報告（富邦「除息影響點數分析」）貼 JSON，6/29 之後沒人更新 →
10 月近月合約還以為有 212 點股息沒除，還原價差整整高估約 200 點（頁面上也一直掛著「除息資料過期」警示）。

改成每天用證交所官方公開資料自己算：
  1. 已公告：openapi exchangeReport/TWT48U_ALL（除權除息預告表，含每股現金股利）。
  2. 尚未公告：用去年同期實際除息（www.twse.com.tw rwd/zh/exRight/TWT49U）推估日期與金額
     （日期＝去年日期＋1 年、金額＝該股最近一次現金股利）。同一檔 45 天內已有公告就以公告為準。
  3. 換算指數點數：點數 = 現金股利 × 發行股數 ÷ 類股總市值 × 指數
     （發行股數／產業別：opendata/t187ap03_L；收盤價：STOCK_DAY_ALL；指數：MI_INDEX）。
     TX＝發行量加權（排除 TDR）、TE＝電子工業類（產業 24–31）、TF＝金融保險類（17）、
     XIF＝未含金融電子、TWN＝TX 點數 × twnSpotRatio（富台/加權 換算比，沿用設定裡的校準值）。
  4. 寫入方式與網頁「除息資料設定 → 儲存」相同：POST http://127.0.0.1:8701/api/config
     （伺服器沒開就直接合併寫 spread_config.json）。seedDate＝今天，daily＝之後每天的除息點數，
     seedRemaining＝各合約月份（TX 第三個週三、TWN 倒數第二個營業日結算）前還沒除的點數合計。

已公告的部分（通常涵蓋未來約一個月，近月合約）是精確值；更遠月份含推估，僅供參考。
排程：工作排程器 Spread_DivUpdate 每天 08:20（開盤前）＋ 登入時。
用法：python update_div_config.py [--dry-run]
"""
import sys, os, json, ssl, time, datetime as dt, argparse, urllib.request
from pathlib import Path

# Python 3.13+ 預設 VERIFY_X509_STRICT：證交所部分伺服器的憑證缺 Subject Key Identifier 會被拒
# （同一網址時好時壞，看分到哪台）。仍完整驗證憑證鏈與主機名，只關掉這個嚴格旗標。
_SSL = ssl.create_default_context()
_SSL.verify_flags &= ~getattr(ssl, "VERIFY_X509_STRICT", 0)

BASE = Path(__file__).resolve().parent
CFG_FILE = BASE / "spread_config.json"
LOG = BASE / "_logs" / "div_update.log"
H = {"User-Agent": "Mozilla/5.0"}
ELEC = {"24", "25", "26", "27", "28", "29", "30", "31"}
FIN = {"17"}
TDR = {"91"}


def log(msg):
    line = f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    try:
        print(line)
    except Exception:
        pass
    try:
        LOG.parent.mkdir(exist_ok=True)
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def get_json(url, tries=3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=H), timeout=40, context=_SSL) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            if i == tries - 1:
                raise
            time.sleep(3 * (i + 1))


def num(v):
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None


def roc(s):
    """'1151008' 或 '114年12月11日' → date"""
    s = str(s).strip()
    if "年" in s:
        y, rest = s.split("年"); m, d = rest.replace("日", "").split("月")
        return dt.date(int(y) + 1911, int(m), int(d))
    return dt.date(int(s[:-4]) + 1911, int(s[-4:-2]), int(s[-2:]))


def third_wed(y, m):
    d = dt.date(y, m, 1)
    d += dt.timedelta(days=(2 - d.weekday()) % 7)
    return d + dt.timedelta(days=14)


def second_last_bday(y, m):
    d = dt.date(y + (m == 12), m % 12 + 1, 1) - dt.timedelta(days=1)
    n = 0
    while True:
        if d.weekday() < 5:
            n += 1
            if n == 2:
                return d
        d -= dt.timedelta(days=1)


def weekday_on_or_after(d):
    while d.weekday() >= 5:
        d += dt.timedelta(days=1)
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    today = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()

    basic = get_json("https://openapi.twse.com.tw/v1/opendata/t187ap03_L")
    price = {x["Code"]: num(x["ClosingPrice"]) for x in get_json("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL")}
    idx = {x["指數"]: num(x["收盤指數"]) for x in get_json("https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX")}
    levels = {"TX": idx.get("發行量加權股價指數"), "TE": idx.get("電子工業類指數"),
              "TF": idx.get("金融保險類指數"), "XIF": idx.get("未含金融電子指數")}
    if not all(levels.values()):
        raise RuntimeError(f"指數抓不到：{levels}")

    stocks, cap = {}, {"TX": 0.0, "TE": 0.0, "TF": 0.0, "XIF": 0.0}
    for x in basic:
        code, ind = x["公司代號"].strip(), x["產業別"].strip()
        sh, px = num(x.get("已發行普通股數或TDR原股發行股數")), price.get(x["公司代號"].strip())
        if not sh or not px or ind in TDR:
            continue
        grp = ["TX"] + (["TE"] if ind in ELEC else ["TF"] if ind in FIN else ["XIF"])
        stocks[code] = (sh, grp)
        for g in grp:
            cap[g] += sh * px

    def points(code, cash):
        sh, grp = stocks[code]
        return {g: cash * sh / cap[g] * levels[g] for g in grp}

    # 1) 已公告
    events = {}   # code -> list[(date, cash, src)]
    ann_until = today
    for x in get_json("https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL"):
        code, cash = x["Code"].strip(), num(x.get("CashDividend"))
        if code not in stocks or "息" not in x.get("Exdividend", "") or not cash:
            continue
        d = roc(x["Date"])
        if d > today:
            events.setdefault(code, []).append((d, cash, "公告"))
            ann_until = max(ann_until, d)

    # 基準日＝第一筆盤中快照那天：歷史圖（每分鐘／每5分鐘還原價差）也用這份設定算各日的未除息點數，
    # 所以 daily 要從那天起、含「已實際除息」的點數，不能只放未來（否則過去的還原價差會被算錯）。
    seed_date = today
    try:
        with open(BASE / "spread_snapshots.jsonl", encoding="utf-8") as f:
            seed_date = dt.datetime.fromtimestamp(json.loads(f.readline())["t"], dt.timezone(dt.timedelta(hours=8))).date()
    except Exception:
        pass
    seed_date = min(seed_date, today)

    # 2) 已實際除息（基準日之後～今天）＋ 去年同期推估（尚未公告者）：同一個查詢
    start = min(seed_date, today - dt.timedelta(days=365))
    hist = get_json("https://www.twse.com.tw/rwd/zh/exRight/TWT49U?startDate=%s&endDate=%s&response=json"
                    % (start.strftime("%Y%m%d"), today.strftime("%Y%m%d")))
    past = {}
    n_done = 0
    cache_f = BASE / "_logs" / "div_detail_cache.json"   # 「權息」的現金部分（查過就記下來，過去的除權息不會變）
    try:
        detail_cache = json.loads(cache_f.read_text(encoding="utf-8"))
    except Exception:
        detail_cache = {}
    for row in hist.get("data", []):
        code, kind = row[1].strip(), row[6]
        if code not in stocks or "息" not in kind:
            continue
        d0 = roc(row[0])
        if kind == "息":
            cash = num(row[5])
        else:
            # 「權息」的「權值+息值」含股票股利 → 查明細取現金股利（每股配發現金股利）
            key = f"{code},{d0:%Y%m%d}"
            if key not in detail_cache:
                try:
                    dj = get_json("https://www.twse.com.tw/rwd/zh/exRight/TWT49UDetail?STK_NO=%s&T1=%s&response=json"
                                  % (code, d0.strftime("%Y%m%d")))
                    txt = (dj.get("data") or [[None, None, ""]])[0][2] or ""
                    detail_cache[key] = num(txt.split("元")[0]) or 0.0
                except Exception as e:
                    log(f"  權息明細 {key} 查不到（{type(e).__name__}），這筆不計")
                    detail_cache[key] = None
                time.sleep(0.6)                        # 證交所網站有頻率限制
            cash = detail_cache.get(key)
        if not cash:
            continue
        past.setdefault(code, []).append((d0, cash))
        if seed_date < d0 <= today:                    # 基準日之後已實際除息 → 歷史圖要用
            events.setdefault(code, []).append((d0, cash, "實際"))
            n_done += 1
    try:
        cache_f.parent.mkdir(exist_ok=True)
        cache_f.write_text(json.dumps({k: v for k, v in detail_cache.items() if v is not None}, ensure_ascii=False),
                           encoding="utf-8")
    except Exception:
        pass
    n_proj = 0
    for code, lst in past.items():
        lst.sort()
        latest_cash = lst[-1][1]
        for d0, _ in lst:
            if d0 <= today - dt.timedelta(days=365):
                continue
            d = weekday_on_or_after(d0 + dt.timedelta(days=365))
            if d <= ann_until:                        # 公告已涵蓋的期間：以公告為準，不推估
                continue
            if any(abs((e[0] - d).days) <= 45 for e in events.get(code, [])):
                continue
            events.setdefault(code, []).append((d, latest_cash, "推估"))
            n_proj += 1

    # 3) 每日點數
    daily = {}
    for code, lst in events.items():
        for d, cash, src in lst:
            p = points(code, cash)
            row = daily.setdefault(d, {"TX": 0.0, "TE": 0.0, "TF": 0.0, "XIF": 0.0})
            for g, v in p.items():
                row[g] += v
    cfg_now = {}
    try:
        cfg_now = json.loads(CFG_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    ratio = cfg_now.get("twnSpotRatio") or 0.085591
    daily_rows = [[d.isoformat(), round(v["TX"], 2), round(v["TE"], 3), round(v["TF"], 3), round(v["XIF"], 2),
                   round(v["TX"] * ratio, 3)] for d, v in sorted(daily.items())]

    # 4) 各合約月份「基準日當天起」尚未除息點數（從基準日的月份到 13 個月後）
    seed = {}
    y, m = seed_date.year, seed_date.month
    n_months = (today.year - y) * 12 + (today.month - m) + 14
    for k in range(n_months):
        yy, mm = y + (m - 1 + k) // 12, (m - 1 + k) % 12 + 1
        s_tx, s_twn = third_wed(yy, mm), second_last_bday(yy, mm)
        if s_tx <= seed_date and s_twn <= seed_date:
            continue
        tot = [0.0] * 5
        for r in daily_rows:
            d = dt.date.fromisoformat(r[0])
            if d <= seed_date:
                continue
            if d <= s_tx:
                for i in range(4):
                    tot[i] += r[i + 1]
            if d <= s_twn:
                tot[4] += r[5]
        seed[f"{yy}-{mm:02d}"] = [round(t, 2) for t in tot]

    body = {"seedDate": seed_date.isoformat(), "seedRemaining": seed, "daily": daily_rows,
            "divUpdated": today.isoformat(),           # 網頁的「除息資料過期」警示改看這個（seedDate 現在是歷史起點）
            "divSource": f"證交所 TWT49U 實際除息（{seed_date.isoformat()} 起）＋ TWT48U 已公告（至 {ann_until.isoformat()}）"
                         f"＋ 去年同期推估；{dt.datetime.now():%Y-%m-%d %H:%M} 自動更新（update_div_config.py）",
            "divAnnouncedUntil": ann_until.isoformat()}
    # 今天看近月：今天之後到近月結算前還沒除的點數
    ny, nm = today.year, today.month
    if today > third_wed(ny, nm):
        ny, nm = ny + (nm == 12), nm % 12 + 1
    near_ym, near_settle = f"{ny}-{nm:02d}", third_wed(ny, nm)
    near_left = sum(r[1] for r in daily_rows if today < dt.date.fromisoformat(r[0]) <= near_settle)
    log(f"基準日 {seed_date}；實際 {n_done} 筆、已公告 {sum(1 for l in events.values() for e in l if e[2] == '公告')} 筆"
        f"（至 {ann_until}）、推估 {n_proj} 筆；近月 {near_ym} TX 尚未除息 {near_left:.2f} 點；共 {len(daily_rows)} 個除息日")
    if a.dry_run:
        if os.environ.get("DIV_DUMP"):              # 檢查用：整份設定寫到指定檔
            Path(os.environ["DIV_DUMP"]).write_text(json.dumps(body, ensure_ascii=False, indent=1), encoding="utf-8")
        print(json.dumps({k: body[k] for k in ("seedDate", "seedRemaining", "divSource")}, ensure_ascii=False, indent=1))
        print("daily 前 8 筆：", daily_rows[:8])
        return 0
    try:
        req = urllib.request.Request("http://127.0.0.1:8701/api/config", data=json.dumps(body).encode("utf-8"),
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=20) as r:
            r.read()
        log("已寫入（經 8701 /api/config）")
    except Exception as e:
        cfg_now.update(body)
        tmp = CFG_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(cfg_now, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp, CFG_FILE)
        log(f"8701 沒回應（{type(e).__name__}），已直接寫 spread_config.json")
    return 0


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    try:
        sys.exit(main())
    except Exception as e:
        log(f"✗ 更新失敗（保留原設定）：{type(e).__name__}: {e}")
        sys.exit(1)
