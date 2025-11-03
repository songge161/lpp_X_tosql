from pathlib import Path
import time

LOG_PATH = Path("import_log.txt")

def tail_log(last_size=0):
    """读取日志增量"""
    if not LOG_PATH.exists():
        return "", 0
    text = LOG_PATH.read_text(encoding="utf-8")
    new_text = text[last_size:]
    return new_text, len(text)
