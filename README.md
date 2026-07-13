# 台指期價差監控

單檔 Python LAN 伺服器：台指期（TX/TE/TF/XIF）＋富台期（TWN）正逆價差即時監控，
含除息還原、歷史累積、盤中快照、宏觀對照卡。無外部套件依賴（Python 3.8+）。

## 啟動

```powershell
python 台指期價差監控.py        # 預設埠 8701
```

環境變數：
- `SPREAD_HOST` — 綁定位址（預設 `0.0.0.0`；上通道時建議設 `127.0.0.1`）

## GitHub Pages 延遲快照版

`pages_publish.py` 會把儀表板發布成公開的靜態延遲版（延遲 ≥15 分鐘、每 5 分鐘更新）：

| 指令 | 用途 |
|---|---|
| `python pages_publish.py --build-site` | 從主程式抽取前端、注入靜態 shim → `pages_site/` |
| `python pages_publish.py` | 單次「匯出→延遲→發布→push」循環（工作排程器每 5 分鐘呼叫） |
| `python pages_publish.py --status` | 檢視發布狀態與日誌 |

架構與完整規劃見 `GitHub公開Pages規劃.md`；即時版（Tailscale Funnel）規劃見 `GitHub上線部署規劃.md`。

### 首次上線（一次性）

```powershell
powershell -ExecutionPolicy Bypass -File setup_github.ps1
```

腳本會引導完成 GitHub 登入（唯一需要人工的步驟），然後自動建立
私有程式碼 repo ＋ 公開 Pages repo、推送、開通 GitHub Pages。

## 免責聲明

本工具僅供個人參考，不構成投資建議。各資料來源版權屬原提供者，
公開版僅發布延遲資料且不得轉散布。
