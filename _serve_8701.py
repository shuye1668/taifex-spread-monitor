# -*- coding: utf-8 -*-
"""台指期價差監控.py 的排程啟動器（2026-09-30 搬機時新增）。

為什麼需要這一層：
    台指期價差監控.py 原本是「人在終端機用 python 手動跑」的（見其檔頭用法說明），
    所以第 3367 行的 log_message 直接寫 sys.stderr、第 3417 行呼叫 sys.stdout.reconfigure()。
    在 pythonw.exe 之下 sys.stdout / sys.stderr 都是 None：
      - 每個 HTTP 請求進 log_message → AttributeError → 連線被關掉、不回任何內容
        （實測症狀：8701 有 LISTEN，但 /api/meta 一律 RemoteDisconnected，
         於是 pages_publish.py 每分鐘記「略過：主圖服務尚未就緒」靜默空轉）
      - 啟動時 reconfigure 也會炸
    這支啟動器在 exec 主程式前先把 stdout/stderr 指到 log 檔，主程式就不必改一個字。
    作法與本機 DataCacheSync\\snapshot_cache.py 的 _setup_streams() 同一個套路。

用法（工作排程器）：
    pythonw.exe "C:\\台指期監控\\_serve_8701.py"        # 預設埠 8701
    pythonw.exe "C:\\台指期監控\\_serve_8701.py" 8702   # 指定埠（會轉給主程式的 sys.argv[1]）
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "台指期價差監控.py")
LOG_DIR = os.path.join(HERE, "_logs")
LOG_FILE = os.path.join(LOG_DIR, "spread_server.log")
LOG_MAX = 5_000_000


def setup_streams():
    """把 stdout/stderr 指到 log 檔；line buffering 讓運維訊息不會卡在緩衝區。"""
    os.makedirs(LOG_DIR, exist_ok=True)
    try:
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX:
            bak = LOG_FILE + ".1"
            if os.path.exists(bak):
                os.remove(bak)
            os.replace(LOG_FILE, bak)
    except Exception:
        pass
    f = open(LOG_FILE, "a", buffering=1, encoding="utf-8", errors="replace")
    sys.stdout = f
    sys.stderr = f


def main():
    setup_streams()
    # 主程式用 sys.argv[1] 當埠號；把本啟動器收到的參數原樣傳下去。
    sys.argv = [TARGET] + sys.argv[1:]
    # 工作目錄要是專案資料夾（spread_*.json 都是相對路徑讀寫）。
    os.chdir(HERE)
    with open(TARGET, encoding="utf-8", errors="replace") as fh:
        code = compile(fh.read(), TARGET, "exec")
    exec(code, {"__name__": "__main__", "__file__": TARGET})


if __name__ == "__main__":
    main()
