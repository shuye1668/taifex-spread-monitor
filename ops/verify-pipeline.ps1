<#
.SYNOPSIS
  台指期價差監控「發布鏈」唯讀健檢：一眼看出資料為何沒更新。
.DESCRIPTION
  只讀取狀態、不更動任何系統設定或檔案。檢查三件事並印出 GREEN / RED：
    1) 主程式伺服器 127.0.0.1:8701 是否在跑（/api/meta）
    2) pages_publish 最後一次成功發布距今多久（讀 pages_state.json 的 lastPublishTs）
    3) 本機 data 工作樹 pages_data/meta.json 的資料時間戳
  並印出 pages_publish.log 最後 8 行。與 `python pages_publish.py --status` 互補。
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File ops\verify-pipeline.ps1
.NOTES
  相容 Windows PowerShell 5.1；預期本檔位於 <repo>\ops\ 下，repo 根目錄＝上一層。
#>
[CmdletBinding()]
param(
  [int]$Port = 8701,
  [int]$StaleMinutes = 120
)

$ErrorActionPreference = 'Continue'
$Root = Split-Path -Parent $PSScriptRoot   # repo 根目錄（ops 的上一層）
$epoch = (Get-Date '1970-01-01T00:00:00Z').ToUniversalTime()
$now = [int]((Get-Date).ToUniversalTime() - $epoch).TotalSeconds
$red = $false

function To-LocalStr([int]$sec) { $epoch.AddSeconds($sec).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss') }

Write-Host '== 台指期發布鏈健檢（唯讀）==' -ForegroundColor Cyan
Write-Host ("repo 根目錄：{0}" -f $Root)
Write-Host ''

# 1) 主程式伺服器
try {
  $r = Invoke-WebRequest -Uri ("http://127.0.0.1:{0}/api/meta" -f $Port) -TimeoutSec 8 -UseBasicParsing
  if ($r.StatusCode -eq 200) {
    Write-Host ("[OK ] 主程式伺服器 127.0.0.1:{0} 有回應" -f $Port) -ForegroundColor Green
  } else {
    Write-Host ("[!! ] 主程式回應非 200：{0}" -f $r.StatusCode) -ForegroundColor Yellow
    $red = $true
  }
} catch {
  Write-Host ("[RED] 主程式伺服器 127.0.0.1:{0} 沒回應 — 台指期價差監控.py 沒在跑或當掉" -f $Port) -ForegroundColor Red
  $red = $true
}

# 2) 最後一次成功發布
$stateFile = Join-Path $Root 'pages_state.json'
if (Test-Path $stateFile) {
  try {
    $st = Get-Content $stateFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $lp = [int]$st.lastPublishTs
    if ($lp -gt 0) {
      $ageMin = [math]::Round(($now - $lp) / 60)
      if ($ageMin -gt $StaleMinutes) {
        Write-Host ("[RED] 最後成功發布：{0}（約 {1} 分鐘前，超過 {2} 分門檻）" -f (To-LocalStr $lp), $ageMin, $StaleMinutes) -ForegroundColor Red
        $red = $true
      } else {
        Write-Host ("[OK ] 最後成功發布：{0}（約 {1} 分鐘前）" -f (To-LocalStr $lp), $ageMin) -ForegroundColor Green
      }
    } else {
      Write-Host '[!! ] pages_state.json 尚無 lastPublishTs（可能從未成功發布過）' -ForegroundColor Yellow
    }
  } catch {
    Write-Host ("[!! ] 讀取 pages_state.json 失敗：{0}" -f $_.Exception.Message) -ForegroundColor Yellow
  }
} else {
  Write-Host ("[!! ] 找不到 {0}（發布器可能尚未跑過）" -f $stateFile) -ForegroundColor Yellow
}

# 3) 本機 data 工作樹 meta.json
$metaFile = Join-Path $Root 'pages_data\meta.json'
if (Test-Path $metaFile) {
  try {
    $m = Get-Content $metaFile -Raw -Encoding UTF8 | ConvertFrom-Json
    Write-Host ("[i  ] 本機 data/meta.json：publishedAt={0}，session={1}" -f $m.publishedAt, $m.session)
  } catch { }
}

# 日誌尾巴
$logFile = Join-Path $Root 'pages_publish.log'
if (Test-Path $logFile) {
  Write-Host ''
  Write-Host '--- pages_publish.log 最後 8 行 ---' -ForegroundColor Cyan
  Get-Content $logFile -Tail 8
}

Write-Host ''
if ($red) {
  Write-Host '結論：RED — 發布鏈異常，請依上面紅字對症處理（見 ops\可靠度強化.md ③ 復原 SOP）。' -ForegroundColor Red
  exit 1
} else {
  Write-Host '結論：GREEN — 發布鏈正常。' -ForegroundColor Green
  exit 0
}
