# ============================================================
# setup_github.ps1 — GitHub Pages 延遲快照版 首次上線（一次性）
# 用法：powershell -ExecutionPolicy Bypass -File setup_github.ps1
# 唯一需要人工的步驟：gh auth login 的瀏覽器授權（約 30 秒）。
# ============================================================
param(
    [string]$CodeRepo  = "taifex-spread-monitor",   # 私有：原始碼
    [string]$PagesRepo = "taifex-spread-pages"      # 公開：Pages 網站 + data 分支
)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Refresh-Path {
    $env:Path = [Environment]::GetEnvironmentVariable("Path", "Machine") + ";" +
                [Environment]::GetEnvironmentVariable("Path", "User")
}

# ---- 1) GitHub CLI ----
Refresh-Path
if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    Write-Host "安裝 GitHub CLI…"
    winget install --id GitHub.cli -e --silent --accept-source-agreements --accept-package-agreements --scope user
    Refresh-Path
}
gh auth status 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "=== 需要登入 GitHub（唯一人工步驟）：請依畫面指示在瀏覽器完成授權 ===" -ForegroundColor Yellow
    gh auth login --hostname github.com --git-protocol https --web
}
gh auth setup-git   # 讓 git push 走 gh 的憑證（排程器背景推送也適用）
$owner = (gh api user -q .login).Trim()
if (-not $owner) { throw "無法取得 GitHub 帳號名稱" }
Write-Host "GitHub 帳號：$owner" -ForegroundColor Green

# ---- 2) 私有 code repo ----
git rev-parse --is-inside-work-tree 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) { throw "此資料夾尚未 git init（pages_publish 部署流程應已建好，請先確認）" }
git remote get-url origin 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "建立私有 repo：$CodeRepo"
    gh repo create $CodeRepo --private --source . --remote origin --push
} else {
    git push -u origin main
}

# ---- 3) 公開 pages repo（main = 網站殼頁）----
Push-Location pages_site
git remote get-url origin 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "建立公開 repo：$PagesRepo"
    gh repo create $PagesRepo --public --source . --remote origin --push
} else {
    git push -u origin main
}
Pop-Location

# ---- 4) data 分支（延遲資料）----
Push-Location pages_data
git remote get-url origin 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) { git remote add origin "https://github.com/$owner/$PagesRepo.git" }
git push --force -u origin data
Pop-Location

# ---- 5) 開通 GitHub Pages（main 分支根目錄）----
$null = gh api -X POST "repos/$owner/$PagesRepo/pages" -f "source[branch]=main" -f "source[path]=/" 2>$null
if ($LASTEXITCODE -ne 0) {
    # 已開通過會 409，改用 PUT 確保指向 main
    $null = gh api -X PUT "repos/$owner/$PagesRepo/pages" -f "source[branch]=main" -f "source[path]=/" 2>$null
}

Write-Host ""
Write-Host "=== 完成 ===" -ForegroundColor Green
Write-Host ("網站網址：  https://{0}.github.io/{1}/" -f $owner, $PagesRepo)
Write-Host ("資料分支：  https://github.com/{0}/{1}/tree/data" -f $owner, $PagesRepo)
Write-Host "Pages 首次建置約需 1–3 分鐘；資料由排程器每 5 分鐘自動推送（延遲 15 分）。"
Write-Host "驗收：手機用行動網路開上面網址，底部應有藍色『延遲快照版』橫幅且資料時間持續更新。"
