# 台指期價差監控 — 上 GitHub 公開 Pages 可行性評估與完整規劃

> 撰寫日期：2026-07-13。本文與 `GitHub上線部署規劃.md`（私有 repo ＋ Tailscale Funnel 即時方案）互補，不取代它。
> 結論先講：**可行，但 GitHub Pages 只能做「延遲快照版」公開儀表板（總延遲約 5–10 分鐘），無法做即時、無法做伺服器端密碼**。即時版仍應走原本的 Tailscale 方案；Pages 定位為「任何人（或持密碼者）隨開即看的延遲版」。

---

## 1. 現況盤點（評估依據）

`台指期價差監控.py` 共 3,314 行：

| 區段 | 行數 | 內容 |
|---|---|---|
| 後端 | 1–1005 | 抓取期交所 MIS、SGX、cnyes、Yahoo；快取與持久化（daily / intraday / inst / bench / snapshots）；HTTP server |
| 前端 | 1006–3078 | 內嵌 HTML/CSS/JS 儀表板 |
| 路由 | 3078–3314 | `do_GET` / `do_POST` 分發 |

前端依賴 10 個 API ＋ 1 條即時流：
`/api/quotes`（15 秒輪詢報價）、`/api/stream`（SSE 即時推播）、`/api/snapshots`（盤中 55 秒快照）、`/api/history`、`/api/context`、`/api/meta`、`/api/usidx`、`/api/ext`、`/api/bench`、`/api/ticker`、`/api/config`（GET＋POST 儲存設定）。

資料檔現況：`spread_snapshots.jsonl` 2.7 MB（4,935 筆、持續增長）、`spread_daily.json` 138 KB、`spread_intraday.json` 195 KB、`spread_bench.json` 87 KB、`spread_inst.json` 4 KB、`spread_config.json`（個人設定，含自選 ticker 與除息種子）。

---

## 2. 可行性總評

### 2.1 GitHub Pages 的三個本質限制

1. **純靜態託管、無後端**：不能跑 Python、不能有伺服器端密碼驗證、不能開 SSE。所有 `/api/*` 都必須改成「讀靜態 JSON 檔」。
2. **瀏覽器無法直連資料源**：期交所 MIS、cnyes、Yahoo 的 API 都沒開 CORS 給第三方網域，Pages 上的 JS 直接 fetch 會被瀏覽器擋掉。改用 GitHub Actions 在雲端排程抓也不可靠——期交所 MIS 與 cnyes 常封鎖海外雲端 IP（原規劃文件第 0 節已確認過此限制），且 Actions 排程實務延遲 10–20 分鐘。**結論：資料仍必須由家中台灣 PC 產生，再推上 GitHub。**
3. **公開 = 全世界可看**：
   - 免費版 GitHub Pages 網站一律公開（私有 Pages 需 Enterprise）；免費個人帳號還要求 **repo 本身也是 public** 才能開 Pages（私有 repo 開 Pages 需 GitHub Pro，約 US$4/月，且網站照樣公開）。
   - 現在前端的「密碼 0168」只是 JS 判斷，在公開 repo / 公開網頁上等於沒有。
   - **法遵是最大風險**：即時報價的公開轉散布受期交所資訊使用授權與各資料源條款約束。公開版務必做到：(a) 只放**延遲 ≥15 分鐘**的資料；(b) 以**衍生計算值**（價差、基差、年化貼水等）為主體而非原始報價流；(c) 加免責聲明。若不想承擔任何公開散布疑慮，選「加密內容」方案（見 3-D）。

### 2.2 功能搬移可行性一覽

| 功能 | 上 Pages | 說明 |
|---|---|---|
| 儀表板 UI（版面/圖表/互動） | ✅ 可 | 把內嵌 HTML 拆成獨立 `index.html` |
| 價差/基差歷史、日線、5 分線 | ✅ 可 | 匯出為靜態 JSON，5–10 分鐘更新 |
| 盤中快照曲線 | ✅ 可（延遲） | 匯出當日修剪版快照 |
| 三大法人／外資未平倉 | ✅ 可 | 本來就是日更資料 |
| 15 秒即時報價、SSE 推播 | ❌ 不可 | 靜態託管做不到；即時需求回到 Tailscale 方案 |
| 參數設定儲存（POST /api/config） | ⚠️ 改造 | 靜態版改存 localStorage，僅影響本機顯示 |
| 自訂 ticker 即查（/api/ticker） | ❌ 不可 | 需要後端代理；靜態版只能顯示預先匯出的清單 |
| 密碼保護 | ⚠️ 選配 | 無伺服器端驗證；可用整頁 AES 加密（StatiCrypt 式）達到「託管公開、內容私有」 |

---

## 3. 架構選項比較

| 選項 | 做法 | 延遲 | 可靠度 | 法遵風險 | 建議 |
|---|---|---|---|---|---|
| **A. 家中 PC 推送延遲資料（推薦）** | 家中 PC 每 5 分鐘匯出 JSON → push 到公開 repo 的 `data` 分支 → Pages 殼頁用 raw.githubusercontent.com 讀 | 5–10 分 | 高（資料源在台灣本機，已驗證可抓） | 中（用延遲＋衍生值＋免責聲明壓低） | ✅ 主方案 |
| B. GitHub Actions 雲端排程抓 | Actions cron 每 5 分抓資料源 commit | 10–25 分 | **低**（海外 IP 被期交所/cnyes 擋；cron 不準時） | 同上 | ❌ 不採用 |
| C. Pages 殼＋即時走家中 Tunnel | 靜態殼頁公開，輸入密碼後 JS 改連 Tailscale Funnel 的家中 API（需後端加 CORS＋Basic Auth） | 即時 | 高 | 低（即時資料僅持密碼者可見） | ⭕ 進階混合，第二階段再做 |
| D. 內容加密（公開託管、私有內容） | 匯出的 JSON 以 AES-GCM（密碼派生金鑰）加密再 push；前端用 WebCrypto 解密，密碼記在 localStorage | 5–10 分 | 高 | **最低**（公開網路上只有密文） | ⭕ 若不願公開任何行情，A 的加密變體 |

**推薦路線：先做 A（必要時套 D 的加密層），穩定後視需求加 C 的「密碼切即時」混合模式。**

### 推薦架構圖（A，含 D 選配）

```
 家中 Windows PC（既有，持續在跑監控程式）
   ├─ 台指期價差監控.py ──(既有抓取/累積邏輯不動)
   ├─ [新增] exporter：每 5 分鐘把 API 資料寫成 docs-data/*.json（延遲版、修剪版）
   │          └─ (選配 D) AES-GCM 加密後才落地
   └─ [新增] pusher：git commit --amend + push --force 到公開 repo 的 data 分支
                    │
                    ▼
 GitHub 公開 repo（taifex-spread-pages）
   ├─ main 分支：index.html（拆出的前端，靜態模式）→ GitHub Pages 建置
   └─ data 分支：quotes.json / daily.json / intraday.json / snaps_today.json / inst.json
                    │（raw.githubusercontent.com，CDN 快取約 5 分鐘，CORS 開放）
                    ▼
 任何電腦/手機 → https://<帳號>.github.io/taifex-spread-pages/
   └─ JS 每 1–5 分鐘輪詢 raw JSON → 渲染（選配 D：先要求輸入密碼解密）
```

要點：**資料放 `data` 分支、不放 Pages 建置分支**——每 5 分鐘一次 push 若觸發 Pages 重建，會撞上 GitHub「每小時約 10 次建置」的軟限制；改走 raw.githubusercontent.com 讀資料就完全不觸發重建，殼頁只有改版時才重建。

---

## 4. 完整實施規劃

### Phase 0 — 決策點（開工前必須拍板）

| # | 決策 | 選項 | 影響 |
|---|---|---|---|
| 0-1 | 公開內容範圍 | 全部延遲資料公開 ／ 只公開衍生指標 ／ 內容加密（D） | 法遵風險等級 |
| 0-2 | 原始碼是否公開 | 公開（單一 repo 全開）／ 不公開（雙 repo：私有 code repo ＋ 公開 pages repo 只放建置產物與資料） | 免費帳號下 Pages repo 必為 public，雙 repo 可保住程式碼隱私 |
| 0-3 | 更新頻率 | 盤中每 5 分（建議）／ 每 15 分（法遵最穩） | push 次數、延遲 |

> 建議組合：**雙 repo ＋ 延遲 15 分標示 ＋ 衍生指標為主體 ＋ 免責聲明**；若仍有疑慮直接上 D 加密。

### Phase 1 — 程式重構（家中 PC，約 1 個工作天）

1. **拆前端**：把 1006–3078 行的 `HTML` 字串抽成獨立 `web/index.html`；Python 端改為讀檔回傳（本機即時版行為不變）。這步同時讓日後前端維護不必動 Python。
2. **資料層 adapter**：前端 JS 加一個 `DATA_MODE`（`live` / `static`）：
   - `live`：維持現狀（`/api/*` ＋ SSE）。
   - `static`：`/api/quotes → data/quotes.json`、`/api/snapshots → data/snaps_today.json`…；停用 SSE 與 `/api/ticker` 即查；`/api/config` 的 POST 改為寫 localStorage；輪詢間隔放寬到 60–300 秒；畫面加「延遲資料，更新於 HH:MM」浮水印。
3. **新增 exporter**（約 150–250 行）：重用既有的 `get_context` / cache 讀取函式，產出：
   - `quotes.json`（**延遲 15 分**的最後報價與衍生值）
   - `daily.json` / `intraday.json`（既有快取直接修剪匯出，近 N 日）
   - `snaps_today.json`（僅當日、可再降採樣到 5 分粒度，控制在數百 KB 內）
   - `inst.json`、`meta.json`（含資料時間戳與免責聲明文字）
   - 選配 D：全部經 AES-GCM（PBKDF2 派生）加密成 `.enc`。
4. **順手修安全債**（無論走哪條路都該做）：`HOST = "0.0.0.0"` 改成 `os.environ.get("SPREAD_HOST", "127.0.0.1")`（原規劃 2.4 節，一直沒做）。

### Phase 2 — Repo 佈局與 Pages 開通（約 0.5 天）

1. 建 **私有 repo**（原始碼，照原規劃第 3 節，`.gitignore` 排除所有 `spread_*.json*`）。
2. 建 **公開 repo** `taifex-spread-pages`：
   - `main`：`index.html`（static 模式建置產物）、`README`、免責聲明。
   - `data`：孤兒分支（`git checkout --orphan data`），只放匯出 JSON。
3. Settings → Pages → Deploy from branch → `main`，確認 `https://<帳號>.github.io/taifex-spread-pages/` 可開。

### Phase 3 — 自動推送（約 0.5 天）

1. **pusher 腳本**（PowerShell 或併入監控程式的背景執行緒）：盤中（日盤 08:45–13:45、夜盤 15:00–05:00）每 5 分鐘：
   ```powershell
   # 在 data 分支工作樹：覆寫 JSON 後
   git add -A
   git commit --amend -m "data" --no-edit   # 永遠只有一個 commit
   git push --force origin data              # repo 不隨時間膨脹
   ```
   > `--amend ＋ --force` 是關鍵：每 5 分鐘一筆正常 commit 一年會累積數萬 commit、repo 破 GB；孤兒分支單 commit 覆寫則永遠只有一份資料的大小。
2. 認證用 fine-grained PAT（只授權該公開 repo 的 contents:write），存 Windows 認證管理員。
3. 掛進既有的開機常駐機制（NSSM 服務或工作排程器，沿用原規劃第 5 節）。

### Phase 4 — 驗收（約 0.5 天）

1. 手機 4G 開 Pages 網址 → 看得到儀表板、資料時間戳在 15 分鐘內滾動。
2. 盤中連續觀察 1 小時：raw JSON 的 CDN 更新延遲應在 5 分鐘內。
3. 拔網路測試：pusher 失敗要靜默重試、不影響本機監控主功能。
4. 檢查公開 repo：確認沒有 `spread_config.json`、完整 snapshots、任何個人資料。
5. （選配 D）無密碼開頁 → 只見輸入框；輸入密碼 → 正常渲染；查 repo 內 `.enc` 確為密文。

### Phase 5 — 選配強化（之後再說）

- **C 混合模式**：後端加 CORS header（僅允許 Pages 網域）＋ Basic Auth（原規劃階段一），殼頁輸入密碼後切 `live` 連 Funnel 網址 → 同一個公開頁，持密碼者看即時、其他人看延遲。
- 自訂網域 ＋ Cloudflare（可加 Access 做真正的存取控制）。
- 歷史資料頁（從 snapshots 產生週/月統計，純靜態、日更即可）。

**總工時估計：約 2–2.5 個工作天**（不含 Phase 5）。成本 US$0（雙 repo 免費方案即可）。

---

## 5. 限制與風險清單

| 風險/限制 | 等級 | 對策 |
|---|---|---|
| 即時報價公開轉散布觸及期交所/資料源授權條款 | **高** | 延遲 ≥15 分、衍生指標為主、免責聲明；最穩是 D 加密 |
| Pages 免費版網站必公開、無伺服器端密碼 | 高 | 定位為公開延遲版；私密需求走 D 或原 Tailscale 方案 |
| raw.githubusercontent CDN 快取約 5 分鐘 | 中 | 更新頻率本來就 ≥5 分，可接受；前端顯示資料時間戳 |
| 每 5 分 push 若打到 Pages 建置分支會撞 10 次/小時建置限制 | 中 | 資料走獨立 `data` 分支＋raw 讀取，不觸發重建 |
| 頻繁 commit 使 repo 膨脹（軟限 1GB） | 中 | 孤兒分支單 commit `--amend ＋ --force` |
| `spread_config.json`、完整 snapshots 屬個人資料 | 中 | 永不進公開 repo；exporter 只輸出修剪後公開子集 |
| 前端密碼 0168 在公開頁面/repo 形同虛設 | 中 | 靜態版直接移除該機制；保護需求改 D |
| Yahoo 端 ext 行情（美股期指等）條款同樣限制轉散布 | 中 | 公開版可考慮不放 ext 區塊，或只放漲跌幅不放價位 |
| 家中 PC 斷電/斷網 → 資料停更 | 低 | 頁面顯示「最後更新時間」自動變紅提示；主監控不受影響 |
| Pages 流量軟限 100GB/月 | 低 | 個人使用量級遠低於此 |

---

## 6. 一頁待辦清單（2026-07-13 實作狀態）

1. [x] Phase 0 決策：雙 repo／延遲 15 分／每 5 分鐘更新／衍生指標＋免責聲明
2. [x] 靜態前端：改為 `pages_publish.py --build-site` 從主程式自動抽取 HTML 並注入 fetch/SSE 攔截 shim（主程式**零改動**，比拆檔更安全，且前端改版後重跑即同步）
3. [x] shim：`/api/*`→靜態 JSON 對映、SSE 停用、設定改存 localStorage、底部延遲橫幅、noindex
4. [x] exporter＋delayer＋pusher 三合一 `pages_publish.py`（15 分延遲緩衝、amend+force 單 commit、時段分級頻率）
5. [x] `HOST` 改為環境變數 `SPREAD_HOST` 可覆寫（預設維持 `0.0.0.0` 不影響既有區網使用）
6. [x] 本地 git repo 三份就緒：code（main）、pages_site（main）、pages_data（data）
7. [x] 工作排程器 `SpreadPagesPublish` 每 5 分鐘背景執行（pythonw 無視窗）
8. [x] 本地端到端驗收通過（靜態頁完整渲染、橫幅/延遲時間/快照數正確）
9. [ ] **唯一剩餘人工步驟**：執行 `setup_github.ps1` 完成 GitHub 登入 → 自動建 repo、推送、開通 Pages
10. [ ] 視需求排 Phase 5（密碼切即時、自訂網域、歷史頁）
