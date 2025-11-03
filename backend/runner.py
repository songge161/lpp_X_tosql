import subprocess, threading

def run_import(log_callback=None):
    """
    启动 version3.main() 或 update_table_list()
    通过独立子线程执行，实时输出日志
    """
    def _worker():
        proc = subprocess.Popen(
            ["python", "version3.py"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        for line in proc.stdout:
            if log_callback:
                log_callback(line.rstrip())
        proc.wait()
    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return t
