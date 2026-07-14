#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pages_publish.py — GitHub Pages「延遲快照版」自動發布器
================================================================
與運作中的 台指期價差監控.py 完全解耦：本腳本只透過本機 HTTP API
（127.0.0.1:8701）讀取資料，不 import、不修改主程式狀態。

用法：
  python pages_publish.py               # 單次循環：匯出→(滿15分延遲)→發布→push（給工作排程器每5分鐘呼叫）
  python pages_publish.py --build-site  # 從主程式抽取前端 HTML、注入靜態 shim，產生/更新 pages_site/
  python pages_publish.py --status      # 顯示目前狀態
  python pages_publish.py --loop        # 常駐模式（除錯用，每 5 分鐘一輪）

發布策略（法遵取向）：
  * 日盤（週一至五 08:25–14:10）：每 5 分鐘發布一次「至少 15 分鐘前」的快照。
  * 夜盤（15:00–翌日 05:10）：至少間隔 25 分鐘發布一次（同樣 ≥15 分延遲）。
  * 收盤時段/週末：每小時至多一次（市場未開，資料本身已靜態）。
資料一律推到公開 repo 的 data 分支，以「單一 commit --amend + force push」
覆寫，repo 不隨時間膨脹。
"""

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent
MAIN_SCRIPT = BASE / "台指期價差監控.py"
SERVER = os.environ.get("SPREAD_SERVER", "http://127.0.0.1:8701")

SITE_DIR = BASE / "pages_site"      # 公開 repo main 分支（靜態殼頁）
DATA_DIR = BASE / "pages_data"      # 公開 repo data 分支（延遲資料，孤兒分支）
BUF_DIR = BASE / "pages_buffer"     # 延遲緩衝區（本機暫存，不進任何 repo）
STATE_FILE = BASE / "pages_state.json"
LOG_FILE = BASE / "pages_publish.log"
LOCK_FILE = BASE / "pages_publish.lock"

DELAY_SEC = 900                     # 公開資料最低延遲（15 分鐘）
SNAP_WINDOW_SEC = 48 * 3600         # 公開快照只保留最近 48 小時
BUFFER_KEEP_SEC = 2 * 3600          # 緩衝區保留 2 小時
NIGHT_PUBLISH_GAP = 1500            # 夜盤發布間隔（25 分）
CLOSED_PUBLISH_GAP = 3300           # 收盤時段發布間隔（55 分）

TZ_TW = timezone(timedelta(hours=8))

# ---- 與前端 INSTRUMENTS / MACRO_DEFS 對應的匯出目錄（來源：主程式前端定義） ----
EXT_ALLOWED = ["ES=F", "NQ=F", "YM=F", "RTY=F", "NKD=F", "NIY=F", "^KS11", "^KS200",
               "^N225", "^DJI", "^GSPC", "^IXIC", "^SOX", "^GDAXI",
               "TWD=X", "JPY=X", "DX-Y.NYB", "^TNX", "^VIX", "BTC-USD", "BZ=F", "CL=F"]
ID2SYM = {"ym": "YM=F", "es": "ES=F", "nq": "NQ=F", "rty": "RTY=F", "niy": "NIY=F",
          "dax": "^GDAXI", "twd": "TWD=X", "jpy": "JPY=X", "dxy": "DX-Y.NYB",
          "brent": "BZ=F", "wti": "CL=F", "tnx": "^TNX", "vix": "^VIX",
          "btc": "BTC-USD", "n225": "^N225", "ks11": "^KS11"}
MACRO_DEF_SYMS = ["ES=F", "TWD=X", "NQ=F", "NIY=F", "^KS11"]   # 前端 MACRO_DEFS 預設
MACRO_RANGES = [("1d", "5m"), ("5d", "15m"), ("1mo", "1d"), ("3mo", "1d"),
                ("6mo", "1d"), ("1y", "1d"), ("5y", "1wk")]
BENCH_CODES = ["TWS:OTC01:INDEX", "TWS:2330:STOCK", "TWS:2317:STOCK",
               "TWS:2454:STOCK", "TWS:0050:STOCK"]
PRODUCTS = ["TX", "TE", "TF", "XIF"]


# ---------------------------------------------------------------- 基礎工具
def log(msg):
    line = "[%s] %s\n" % (datetime.now(TZ_TW).strftime("%m-%d %H:%M:%S"), msg)
    try:
        if LOG_FILE.exists() and LOG_FILE.stat().st_size > 512 * 1024:
            LOG_FILE.replace(LOG_FILE.with_suffix(".log.1"))
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    try:
        sys.stdout.write(line)
    except Exception:
        pass


def load_state():
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"lastPublishTs": 0, "lastBufferTs": 0}


def save_state(st):
    STATE_FILE.write_text(json.dumps(st, ensure_ascii=False, indent=1), encoding="utf-8")


def acquire_lock():
    try:
        if LOCK_FILE.exists():
            if time.time() - LOCK_FILE.stat().st_mtime < 600:
                return False
            LOCK_FILE.unlink()
        LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
        return True
    except Exception:
        return False


def release_lock():
    try:
        LOCK_FILE.unlink()
    except Exception:
        pass


def safe_name(s):
    return re.sub(r"[^A-Za-z0-9.\-]", "_", str(s))


def http_get(path, timeout=25):
    req = urllib.request.Request(SERVER + path, headers={"User-Agent": "pages-exporter"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def git(cwd, *args, timeout=180):
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0", GCM_INTERACTIVE="never")
    return subprocess.run(["git", "-C", str(cwd)] + list(args),
                          capture_output=True, text=True, encoding="utf-8",
                          errors="replace", timeout=timeout, env=env)


def ensure_repo(path, branch):
    path.mkdir(exist_ok=True)
    if not (path / ".git").exists():
        git(path, "init", "-b", branch)
        git(path, "config", "user.name", "taifex-pages-bot")
        git(path, "config", "user.email", "taifex-pages-bot@users.noreply.github.com")
        log("初始化 git repo：%s（分支 %s）" % (path.name, branch))


def commit_and_push(path, branch, msg, amend):
    if not git(path, "status", "--porcelain").stdout.strip():
        return "nochange"
    git(path, "add", "-A")
    has_head = git(path, "rev-parse", "--verify", "HEAD").returncode == 0
    if has_head and amend:
        r = git(path, "commit", "--amend", "-m", msg)
    else:
        r = git(path, "commit", "-m", msg)
    if r.returncode != 0:
        return "commitfail:" + (r.stderr or r.stdout)[-200:]
    if git(path, "remote", "get-url", "origin").returncode != 0:
        return "local-only"
    p = git(path, "push", "--force", "-u", "origin", branch)
    return "pushed" if p.returncode == 0 else "pushfail:" + (p.stderr or "")[-200:]


# ---------------------------------------------------------------- 時段判斷
def session_kind(dt=None):
    dt = dt or datetime.now(TZ_TW)
    wd, hm = dt.weekday(), dt.hour * 100 + dt.minute
    if wd <= 4 and 825 <= hm <= 1410:
        return "day"
    if (wd <= 4 and hm >= 1455) or (wd in (1, 2, 3, 4, 5) and hm <= 510):
        return "night"
    return "closed"


# ---------------------------------------------------------------- 匯出
def export_targets():
    """組出「檔名 → API 路徑」對照。ext 組合依目前設定的宏觀卡標的展開全部時間區間。"""
    targets = {
        "config.json": "/api/config",
        "quotes.json": "/api/quotes",
        "usidx.json": "/api/usidx",
        "context.json": "/api/context",
        "meta_raw.json": "/api/meta",
        "snaps.json": "/api/snapshots?after=%d" % int(time.time() - SNAP_WINDOW_SEC),
    }
    for p in PRODUCTS:
        targets["hist_%s_D.json" % p] = "/api/history?product=%s&res=D" % p
        targets["hist_%s_5.json" % p] = "/api/history?product=%s&res=5" % p

    try:
        cfg = json.loads(http_get("/api/config"))
    except Exception:
        cfg = {}

    macro_syms = []
    sel = cfg.get("macroSel") or []
    for i, base_sym in enumerate(MACRO_DEF_SYMS):
        sid = sel[i] if i < len(sel) else None
        macro_syms.append(ID2SYM.get(sid, base_sym))

    combos = set((s, "1d", "5m") for s in EXT_ALLOWED)
    for s in macro_syms:
        for rng, iv in MACRO_RANGES:
            combos.add((s, rng, iv))
    for sym, rng, iv in sorted(combos):
        targets["ext_%s_%s_%s.json" % (safe_name(sym), rng, iv)] = \
            "/api/ext?sym=%s&range=%s&interval=%s" % (urllib.parse.quote(sym, safe=""), rng, iv)

    for code in BENCH_CODES:
        targets["bench_%s_5.json" % safe_name(code)] = \
            "/api/bench?code=%s&res=5" % urllib.parse.quote(code, safe="")

    for q in (cfg.get("miniTickers") or [])[:8]:
        q = (q or "").strip()
        if q:
            targets["ticker_%s.json" % safe_name(q)] = "/api/ticker?q=" + urllib.parse.quote(q, safe="")
    return targets


def export_cycle():
    """抓一輪本機 API，寫進 pages_buffer/<epoch>/（延遲緩衝）。回傳緩衝 ts 或 None。"""
    ts = int(time.time())
    tmp = BUF_DIR / ("_tmp_%d" % ts)
    final = BUF_DIR / str(ts)
    BUF_DIR.mkdir(exist_ok=True)
    tmp.mkdir(exist_ok=True)
    ok, fail = 0, 0
    for name, path in export_targets().items():
        try:
            body = http_get(path)
            json.loads(body)                      # 確認是合法 JSON 才收
            (tmp / name).write_bytes(body)
            ok += 1
        except Exception as e:
            fail += 1
            if name in ("quotes.json", "config.json"):
                log("匯出中止：核心檔 %s 失敗（%s）" % (name, str(e)[:80]))
                shutil.rmtree(tmp, ignore_errors=True)
                return None
    (tmp / "manifest.json").write_text(
        json.dumps({"ts": ts, "ok": ok, "fail": fail}), encoding="utf-8")
    tmp.replace(final)
    log("匯出完成：buffer/%d（成功 %d、失敗 %d）" % (ts, ok, fail))
    return ts


def prune_buffers():
    if not BUF_DIR.exists():
        return
    cutoff = time.time() - BUFFER_KEEP_SEC
    for d in BUF_DIR.iterdir():
        try:
            ts = int(d.name) if d.name.isdigit() else int(d.stat().st_mtime)
            if ts < cutoff:
                shutil.rmtree(d, ignore_errors=True)
        except Exception:
            pass


# ---------------------------------------------------------------- 發布
def latest_buffer(max_ts):
    """回傳 ts <= max_ts 的最新完整緩衝目錄（ts, path），沒有則 None。"""
    if not BUF_DIR.exists():
        return None
    best = None
    for d in BUF_DIR.iterdir():
        if d.name.isdigit() and (d / "manifest.json").exists():
            ts = int(d.name)
            if ts <= max_ts and (best is None or ts > best[0]):
                best = (ts, d)
    return best


def publish(buf_ts, buf_dir, kind):
    ensure_repo(DATA_DIR, "data")
    # 清掉舊資料檔（保留 .git 與 README）
    for f in DATA_DIR.glob("*.json"):
        f.unlink()
    for f in buf_dir.glob("*.json"):
        if f.name in ("manifest.json", "meta_raw.json"):
            continue
        shutil.copy2(f, DATA_DIR / f.name)
    # 合成 meta.json（前端診斷列與延遲橫幅都讀這裡）
    raw = {}
    try:
        raw = json.loads((buf_dir / "meta_raw.json").read_text(encoding="utf-8"))
    except Exception:
        pass
    now = int(time.time())
    meta = {"boot": "PAGES·" + str(raw.get("boot", "")), "port": 0,
            "snapCount": raw.get("snapCount", 0), "lastSnapT": raw.get("lastSnapT", 0),
            "now": now, "dataAsOf": buf_ts, "delayMin": DELAY_SEC // 60,
            "session": kind,
            "publishedAt": datetime.now(TZ_TW).strftime("%Y-%m-%d %H:%M:%S")}
    (DATA_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    if not (DATA_DIR / "README.md").exists():
        (DATA_DIR / "README.md").write_text(
            "# data 分支\n\n台指期價差監控「延遲快照版」資料檔（延遲 ≥15 分鐘）。"
            "由家中主機每 5 分鐘以單一 commit 覆寫（amend + force push），不保留歷史。\n"
            "資料僅供參考，非即時報價，請勿作為交易依據或轉散布。\n", encoding="utf-8")
    res = commit_and_push(DATA_DIR, "data", "data snapshot (delayed)", amend=True)
    log("發布：dataAsOf=%s → %s" % (datetime.fromtimestamp(buf_ts, TZ_TW).strftime("%H:%M:%S"), res))
    return res


def run_cycle():
    kind = session_kind()
    st = load_state()
    now = int(time.time())
    since_pub = now - int(st.get("lastPublishTs", 0))

    # 收盤時段：低頻維護（每 ~55 分一次，含匯出+立即發布——市場未開、資料靜態）
    if kind == "closed":
        if since_pub < CLOSED_PUBLISH_GAP:
            return
        try:
            json.loads(http_get("/api/meta", timeout=8))
        except Exception:
            log("略過：主監控伺服器未回應（%s）" % SERVER)
            return
        ts = export_cycle()
        if ts:
            publish(ts, BUF_DIR / str(ts), kind)
            st.update(lastPublishTs=now, lastBufferTs=ts)
            save_state(st)
        prune_buffers()
        return

    # 盤中（日盤/夜盤）：每輪匯出進緩衝；發布必須滿 15 分鐘延遲
    try:
        json.loads(http_get("/api/meta", timeout=8))
    except Exception:
        log("略過：主監控伺服器未回應（%s）" % SERVER)
        return
    export_cycle()
    if kind == "night" and since_pub < NIGHT_PUBLISH_GAP:
        prune_buffers()
        return
    cand = latest_buffer(now - DELAY_SEC)
    if cand and cand[0] != int(st.get("lastBufferTs", 0)):
        publish(cand[0], cand[1], kind)
        st.update(lastPublishTs=now, lastBufferTs=cand[0])
        save_state(st)
    prune_buffers()


# ---------------------------------------------------------------- 靜態站建置
SHIM_JS = r"""
/* == GitHub Pages 靜態資料轉接層（由 pages_publish.py --build-site 自動注入，請勿手改） == */
(function () {
  "use strict";
  var RAW_BASE = (function () {
    if (window.SPREAD_DATA_BASE) return window.SPREAD_DATA_BASE;
    var h = location.hostname;
    if (/\.github\.io$/i.test(h)) {
      var owner = h.split(".")[0];
      var seg = location.pathname.split("/").filter(Boolean);
      return "https://raw.githubusercontent.com/" + owner + "/" + (seg[0] || "") + "/data/";
    }
    return "data/";
  })();
  var OVERLAY_KEY = "spreadPagesCfgOverlay";
  var _fetch = window.fetch.bind(window);
  function bust() { return "?u=" + Math.floor(Date.now() / 60000); }
  function safeName(s) { return String(s).replace(/[^A-Za-z0-9.\-]/g, "_"); }
  function dataFetch(name) { return _fetch(RAW_BASE + name + bust(), { cache: "no-store" }); }
  function jsonResp(obj, status) {
    return new Response(JSON.stringify(obj), { status: status || 200,
      headers: { "Content-Type": "application/json; charset=utf-8" } });
  }
  function parseQuery(url) {
    var out = {}, i = url.indexOf("?");
    if (i < 0) return out;
    url.slice(i + 1).split("&").forEach(function (kv) {
      var j = kv.indexOf("=");
      if (j > -1) out[kv.slice(0, j)] = decodeURIComponent(kv.slice(j + 1));
    });
    return out;
  }
  function overlay() {
    try { return JSON.parse(localStorage.getItem(OVERLAY_KEY) || "{}"); }
    catch (e) { return {}; }
  }
  var snapsCache = { t: 0, d: null };
  async function getSnaps() {
    if (snapsCache.d && Date.now() - snapsCache.t < 55000) return snapsCache.d;
    var d = await (await dataFetch("snaps.json")).json();
    snapsCache = { t: Date.now(), d: d };
    return d;
  }
  var lastMeta = null;
  async function handleApi(url, init) {
    var path = url.split("?")[0], q = parseQuery(url);
    var method = ((init && init.method) || "GET").toUpperCase();
    if (path === "/api/config") {
      if (method === "POST") {
        var body = {};
        try { body = JSON.parse(init.body || "{}"); } catch (e) {}
        if (body.reset) localStorage.removeItem(OVERLAY_KEY);
        else localStorage.setItem(OVERLAY_KEY, JSON.stringify(Object.assign(overlay(), body)));
      }
      var base = {};
      try { base = await (await dataFetch("config.json")).json(); } catch (e) {}
      return jsonResp(Object.assign({}, base, overlay()));
    }
    if (path === "/api/quotes") return dataFetch("quotes.json");
    if (path === "/api/usidx") return dataFetch("usidx.json");
    if (path === "/api/context") return dataFetch("context.json");
    if (path === "/api/meta") {
      try {
        var m = await (await dataFetch("meta.json")).json();
        lastMeta = m; updateBanner();
        return jsonResp(m);
      } catch (e) { return jsonResp({ error: "meta" }, 404); }
    }
    if (path === "/api/snapshots") {
      try {
        var after = parseInt(q.after || "0", 10) || 0;
        var d = await getSnaps();
        return jsonResp({ snaps: ((d && d.snaps) || []).filter(function (s) { return s.t > after; }) });
      } catch (e) { return jsonResp({ snaps: [] }); }
    }
    if (path === "/api/history")
      return dataFetch("hist_" + safeName(q.product || "TX") + "_" + (q.res === "5" ? "5" : "D") + ".json");
    if (path === "/api/ext")
      return dataFetch("ext_" + safeName(q.sym || "") + "_" + (q.range || "1d") + "_" + (q.interval || "5m") + ".json");
    if (path === "/api/bench")
      return dataFetch("bench_" + safeName(q.code || "") + "_" + (q.res === "5" ? "5" : "D") + ".json");
    if (path === "/api/ticker")
      return dataFetch("ticker_" + safeName(q.q || "") + ".json");
    return jsonResp({ error: "static" }, 404);
  }
  window.fetch = function (input, init) {
    var url = (typeof input === "string") ? input : ((input && input.url) || "");
    if (url.indexOf("/api/") === 0) return handleApi(url, init);
    return _fetch(input, init);
  };
  var _ES = window.EventSource;
  window.EventSource = function (url) {
    if (String(url).indexOf("/api/stream") === 0)
      return { close: function () {}, addEventListener: function () {},
               onmessage: null, onerror: null, readyState: 2 };
    return _ES ? new _ES(url) : { close: function () {} };
  };
  function fmtTW(ts) {
    try {
      return new Intl.DateTimeFormat("zh-TW", { timeZone: "Asia/Taipei", hour12: false,
        month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }).format(new Date(ts * 1000));
    } catch (e) { return ""; }
  }
  function updateBanner() {
    if (!document.body) return;
    var el = document.getElementById("pagesDelayBanner");
    if (!el) {
      el = document.createElement("div");
      el.id = "pagesDelayBanner";
      el.style.cssText = "position:fixed;left:0;right:0;bottom:0;z-index:99999;" +
        "background:#1f4e8c;color:#fff;font:12px/1.7 -apple-system,'Microsoft JhengHei',sans-serif;" +
        "text-align:center;padding:3px 10px;opacity:.94;";
      document.body.appendChild(el);
    }
    var m = lastMeta, asOf = (m && (m.dataAsOf || m.now)) || 0;
    var stale = asOf && (Date.now() / 1000 - asOf) > 2400;
    el.style.background = stale ? "#b54708" : "#1f4e8c";
    el.textContent = "GitHub Pages 延遲快照版｜資料時間 " + (asOf ? fmtTW(asOf) : "—") +
      "（延遲 ≥" + ((m && m.delayMin) || 15) + " 分鐘" + (stale ? "，更新滯後" : "") +
      "）｜非即時報價，僅供參考，不構成投資建議";
    document.body.style.paddingBottom = (el.offsetHeight + 6) + "px";
  }
  document.addEventListener("DOMContentLoaded", function () {
    document.title += "（延遲快照版）";
    updateBanner();
    setInterval(updateBanner, 30000);
  });
})();
"""

SITE_README = """# 台指期價差監控 — 延遲快照版（GitHub Pages）

這是個人用途的台指期正逆價差監控儀表板之**延遲快照版**：

- 資料由家中主機每 5 分鐘匯出、**延遲至少 15 分鐘**後發布到本 repo 的 `data` 分支。
- 本頁為純靜態網頁，無即時報價、無伺服器。
- **免責聲明**：所有數據僅供個人參考，非即時報價，不構成任何投資建議；
  請勿轉散布本頁資料。原始資料版權屬各資料來源所有。

`index.html` 由私有 repo 的 `pages_publish.py --build-site` 自動產生，請勿手動編輯。
"""


def build_site():
    src = MAIN_SCRIPT.read_text(encoding="utf-8")
    m = re.search(r'HTML\s*=\s*r?"""(.*?)"""', src, re.S)
    if not m or not m.group(1).lstrip().startswith("<!DOCTYPE"):
        log("建站失敗：抓不到主程式的 HTML 區塊")
        return False
    html = m.group(1)
    inject = ('<head>\n<meta name="robots" content="noindex">\n'
              "<script>" + SHIM_JS + "</script>")
    if "<head>" not in html:
        log("建站失敗：HTML 內找不到 <head>")
        return False
    html = html.replace("<head>", inject, 1)
    ensure_repo(SITE_DIR, "main")
    (SITE_DIR / "index.html").write_text(html, encoding="utf-8")
    (SITE_DIR / ".nojekyll").write_text("", encoding="utf-8")
    (SITE_DIR / "README.md").write_text(SITE_README, encoding="utf-8")
    res = commit_and_push(SITE_DIR, "main", "site: build from 台指期價差監控.py", amend=False)
    log("建站完成：pages_site/index.html（%d bytes）→ %s" % ((SITE_DIR / "index.html").stat().st_size, res))
    return True


# ---------------------------------------------------------------- 入口
def status():
    st = load_state()
    print("狀態檔：", json.dumps(st, ensure_ascii=False))
    print("時段：", session_kind())
    for d, b in ((SITE_DIR, "main"), (DATA_DIR, "data")):
        r = git(d, "remote", "get-url", "origin") if (d / ".git").exists() else None
        print("%s → remote: %s" % (d.name, (r.stdout.strip() if r and r.returncode == 0 else "（未設定）")))
    if BUF_DIR.exists():
        bufs = sorted(int(x.name) for x in BUF_DIR.iterdir() if x.name.isdigit())
        print("緩衝區：%d 份" % len(bufs),
              ("最新 " + datetime.fromtimestamp(bufs[-1], TZ_TW).strftime("%H:%M:%S")) if bufs else "")
    if LOG_FILE.exists():
        print("--- 最近日誌 ---")
        for line in LOG_FILE.read_text(encoding="utf-8").splitlines()[-8:]:
            print(line)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    arg = sys.argv[1] if len(sys.argv) > 1 else ""
    if arg == "--build-site":
        build_site()
        return
    if arg == "--status":
        status()
        return
    if arg == "--loop":
        while True:
            if acquire_lock():
                try:
                    run_cycle()
                finally:
                    release_lock()
            time.sleep(300)
    if not acquire_lock():
        log("略過：另一個發布程序仍在執行")
        return
    try:
        run_cycle()
    finally:
        release_lock()


if __name__ == "__main__":
    main()
