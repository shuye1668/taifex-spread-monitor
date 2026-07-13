# 台指期價差監控 — 上 GitHub ＋ 任何電腦可開、永遠在線 完整規劃

> 目標：原始碼放上 **私有 GitHub**；程式 **永遠在線**、從 **任何電腦** 用 **一組共用密碼** 即可開啟。
> 已選方案：**家裡台灣 PC 執行 ＋ 通道(Tunnel)對外 ＋ 伺服器端單一密碼 ＋ 私有 repo**。
> 成本：**US$0**（用 Tailscale Funnel，免網域）。若你已有網域，可改用 Cloudflare Tunnel（見附錄 B）。

---

## 0. 為什麼這個架構最適合你

- **無地理封鎖**：期交所 MIS、cnyes 等資料源常擋海外雲端 IP。程式跑在你台灣家中的 PC，抓得到資料。
- **資料持續累積**：`spread_snapshots.jsonl`、`spread_daily.json` 等需要程式長時間在盤中持續記錄。家用 PC 常開最單純，檔案就留在本機、不會因雲端重啟而消失。
- **真免費**：Tailscale 個人版免費、給你一個固定 `https://<機器名>.<tailnet>.ts.net` 網址，附自動 HTTPS。
- **GitHub 只當原始碼備份與版本管理**，不放任何資料檔與密碼。

### 架構圖

```
 任何電腦/手機 (輸入共用密碼)
        │  https://taifex.<你的>.ts.net
        ▼
 Tailscale Funnel（免費，自動 HTTPS，固定網址）
        │  轉發到本機 127.0.0.1:8701
        ▼
 家中 Windows PC（永遠開機、防睡眠）
   ├─ python 台指期價差監控.py   ← 監聽 127.0.0.1:8701，伺服器端密碼驗證
   └─ 資料檔 spread_*.json / .jsonl（本機持續累積）

 GitHub（私有 repo）← git push 原始碼；資料檔/密碼以 .gitignore 排除
```

---

## 1. 先決條件（要先準備的帳號／工具）

| 項目 | 用途 | 費用 |
|---|---|---|
| GitHub 帳號 | 放私有原始碼 | 免費 |
| Git for Windows | 在 PC 上 push/pull | 免費 |
| Tailscale 帳號 ＋ Windows 版 | 對外通道（固定網址＋HTTPS） | 免費 |
| Python 3.8+（你已在用） | 執行程式 | 免費 |
| NSSM（或內建工作排程器） | 讓程式開機自動跑、崩潰自動重啟 | 免費 |

> PC 防睡眠你已透過 `Set-NoSleep-PowerPlan`（XlsbUpdater 那套）永久關掉，這點已具備。

---

## 2. 階段一：改程式碼，加「伺服器端密碼」（必做）

目前 `0168` 只是前端 JS 判斷，任何人改網址或看原始碼就能繞過——放上網路前**必須**改成伺服器端驗證。
做法：用 **HTTP Basic Auth**，密碼從 **環境變數** 讀取（不寫死、不上傳 GitHub）。

### 2.1 在檔案最上方 import 區加入

```python
import os, base64
```

### 2.2 在 `class Handler` 內加一個驗證方法

```python
    # ---- 伺服器端存取密碼（環境變數 SPREAD_PW；未設定＝不啟用，維持純內網）----
    def _authed(self):
        pw_need = os.environ.get("SPREAD_PW", "")
        if not pw_need:
            return True  # 沒設密碼＝LAN 模式，照舊
        h = self.headers.get("Authorization", "")
        if h.startswith("Basic "):
            try:
                dec = base64.b64decode(h[6:]).decode("utf-8")
                got = dec.split(":", 1)[1] if ":" in dec else dec
                if got == pw_need:
                    return True
            except Exception:
                pass
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="Spread Monitor"')
        self.send_header("Content-Length", "0")
        self.end_headers()
        return False
```

### 2.3 在 `do_GET` 與 `do_POST` 的最前面各加一行

```python
    def do_GET(self):
        if not self._authed():
            return
        path = self.path.split("?")[0]
        ...

    def do_POST(self):
        if not self._authed():
            return
        path = self.path.split("?")[0]
        ...
```

> 行為：瀏覽器第一次開會跳出帳密視窗，**帳號隨便打、密碼填你設定的那組**即可。透過 Tunnel 一律走 HTTPS，密碼不會以明文在網路上傳。
> 想要更漂亮的「登入頁＋記住我」可改用 Cookie/Session（附錄 C），但 Basic Auth 最簡單夠用。

### 2.4 （建議）只綁本機

通道是把外網轉發到 `127.0.0.1`，所以本機綁定即可、更安全。把：

```python
HOST = "0.0.0.0"
```

改成可由環境變數控制、預設本機：

```python
HOST = os.environ.get("SPREAD_HOST", "127.0.0.1")
```

> 仍想在家用區網直接連，臨時把環境變數設成 `0.0.0.0` 即可，平時維持 `127.0.0.1`。

---

## 3. 階段二：放上私有 GitHub

### 3.1 建立 `.gitignore`（與程式同目錄）

```
# 累積資料（屬個人資料，不上傳）
spread_daily.json
spread_intraday.json
spread_inst.json
spread_bench.json
spread_snapshots.jsonl
spread_config.json
# 快取
__pycache__/
*.pyc
# 規劃文件可自行決定是否上傳
```

> 密碼放環境變數、不在檔案裡，所以原始碼可安全上傳。`spread_config.json`（含你的除息設定與自訂 ticker）建議排除，屬個人資料。

### 3.2 建立 README（簡述用途、啟動方式、環境變數）

至少寫明：`SPREAD_PW`（存取密碼）、`SPREAD_HOST`、`PORT` 的用法，以及 `python 台指期價差監控.py` 啟動。

### 3.3 初始化並推送

在程式所在資料夾開 PowerShell：

```powershell
cd C:\台指期監控
git init
git add 台指期價差監控.py .gitignore README.md GitHub上線部署規劃.md
git commit -m "init: 台指期價差監控（伺服器端密碼版）"
# 到 GitHub 網站建立一個 Private repo（例如 taifex-spread-monitor），複製其網址後：
git branch -M main
git remote add origin https://github.com/<你的帳號>/taifex-spread-monitor.git
git push -u origin main
```

> 第一次 push 會要求登入；建議用 GitHub 桌面版或 Personal Access Token。

---

## 4. 階段三：對外開放（Tailscale Funnel，免費、免網域、固定網址）

### 4.1 安裝與登入

1. 到 tailscale.com 註冊（可用 Google/GitHub 登入），下載 **Windows 版**安裝。
2. 安裝後登入，PC 會出現在你的 tailnet，並得到一個機器名，例如 `taifex-pc`。
3. 你的 tailnet 會有一個網域，例如 `tailXXXX.ts.net`。

### 4.2 開啟 Funnel（把本機 8701 對全世界開）

以系統管理員開 PowerShell：

```powershell
# 先確認程式在跑、且監聽 127.0.0.1:8701
tailscale funnel 8701
```

成功後會顯示一個固定公開網址，例如：

```
https://taifex-pc.tailXXXX.ts.net/
```

這就是「**任何電腦／手機輸入這個網址 → 跳密碼 → 進入監控**」的入口。
> Funnel 設定是持久的；之後開機只要 Tailscale 服務在跑、程式在跑，網址就持續可用。
> 若管理台要求啟用 Funnel 權限，依畫面指示在 Tailscale Admin 的 ACL 開啟一次即可。

---

## 5. 階段四：永遠在線、自動啟動、崩潰自動重啟

要「永遠可用」需三者都常駐：**① 監控程式 ② Tailscale 服務 ③ PC 不睡眠**。

### 5.1 監控程式 → 註冊成 Windows 服務（推薦用 NSSM）

NSSM 可開機自動啟動、崩潰自動重啟、背景無視窗：

```powershell
# 安裝 nssm 後：
nssm install SpreadMonitor "C:\Windows\py.exe" "C:\台指期監控\台指期價差監控.py"
nssm set SpreadMonitor AppDirectory "C:\台指期監控"
# 設定存取密碼與綁定（服務環境變數）
nssm set SpreadMonitor AppEnvironmentExtra SPREAD_PW=你的共用密碼 SPREAD_HOST=127.0.0.1
nssm set SpreadMonitor Start SERVICE_AUTO_START
nssm set SpreadMonitor AppExit Default Restart
nssm start SpreadMonitor
```

> 替代法（不裝 NSSM）：用「工作排程器」建立一個「電腦啟動時」「不論使用者登入與否」執行 `pythonw.exe 台指期價差監控.py` 的任務，並在「設定」勾選失敗自動重新啟動。環境變數可用系統環境變數設定 `SPREAD_PW`。

### 5.2 Tailscale → 本來就是 Windows 服務

Tailscale 安裝後即以服務常駐、開機自動連線；Funnel 設定持久。不需額外動作。

### 5.3 PC 不睡眠

已用 `Set-NoSleep-PowerPlan` 永久關閉睡眠/休眠（沿用你現有設定）。確認螢幕可關、但系統不睡。

### 5.4 驗收

重開機 → 不登入桌面等 1–2 分鐘 → 用手機 4G（不連家裡 Wi-Fi）開 `https://taifex-pc.tailXXXX.ts.net/` → 跳密碼 → 看到即時報價且每分鐘快照持續累積，即代表「永遠在線」成立。

---

## 6. 日後更新流程（改程式 → 上版 → 生效）

```powershell
# 在 PC 上改完程式、測試 OK 後
cd C:\台指期監控
git add -A
git commit -m "feat: 說明這次改了什麼"
git push
# 讓服務載入新版
nssm restart SpreadMonitor
```

> 在別台電腦改的話：那台 `git push` → 家裡 PC `git pull` → `nssm restart SpreadMonitor`。

---

## 7. 安全與注意事項

- **務必先完成階段一**（伺服器端密碼）再開 Funnel；否則等於把你的監控對全世界裸奔。
- 密碼只放 **環境變數 / 服務設定**，不要寫進程式、不要 commit。想換密碼：改 `SPREAD_PW` → 重啟服務。
- Basic Auth 在 HTTPS 下安全（Funnel 自帶 HTTPS）；不要用純 http 對外。
- 想更強：用 Tailscale 的「只給登入裝置」模式（不開 Funnel，改 Serve＋裝置授權），就完全不對公開網路曝光——但這樣別人要先加入你的 tailnet 才連得到，與「任何電腦可開」取捨一下。
- GitHub 設 **Private**；即使如此也不要把資料檔／密碼放進去。
- 法遵：本工具僅供你個人參考，資料源各有使用條款，請勿公開轉散布。

---

## 8. 一頁待辦清單

1. [ ] 程式加伺服器端密碼（階段一 2.1–2.4）
2. [ ] 建 `.gitignore` ＋ README
3. [ ] GitHub 建 Private repo、`git push`
4. [ ] 裝 Tailscale、登入、`tailscale funnel 8701`、記下公開網址
5. [ ] 用 NSSM（或工作排程器）把程式設成開機自動＋崩潰重啟，並帶入 `SPREAD_PW`
6. [ ] 確認 PC 不睡眠
7. [ ] 用手機行動網路驗收公開網址＋密碼
8. [ ] 記下「更新流程」（第 6 節）備用

---

## 附錄 A：成本

全部 US$0：Tailscale 個人版免費、GitHub 私有 repo 免費、NSSM 免費、Python 免費。唯一前提是家中 PC 電費與保持開機。

## 附錄 B：改用 Cloudflare Tunnel（若你已有網域）

1. 把網域加入 Cloudflare（免費方案）。
2. 裝 `cloudflared`，`cloudflared tunnel login` → `cloudflared tunnel create taifex`。
3. 設定 `config.yml` 把 `taifex.你的網域` 指到 `http://127.0.0.1:8701`，`cloudflared tunnel route dns taifex taifex.你的網域`。
4. `cloudflared service install` 設成開機常駐服務。
5. 自訂網址、效能佳；缺點是需要一個網域（約 US$1–10/年），所以非 0 元。

## 附錄 C：把 Basic Auth 換成「登入頁＋Cookie」（選配，UX 較好）

概念：新增 `/login` 收 POST 密碼，正確就 `Set-Cookie: sid=<隨機>; HttpOnly; Secure`，伺服器記住有效 session；`_authed()` 改查 Cookie，未登入則導到登入頁。比 Basic Auth 多一點程式碼，但能「記住我」、可登出。需要的話我可以直接幫你改。
```
