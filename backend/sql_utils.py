# backend/sql_utils.py
import re
from pathlib import Path

INSERT_RE = re.compile(
    r"insert\s+into\s+public\.\"?(?P<table>[\w\u4e00-\u9fa5]+)\"?\s*\((?P<cols>[^)]*)\)\s*values",
    re.IGNORECASE
)
DDL_RE = re.compile(
    r"create\s+table\s+(?:public\.)?\"?(?P<table>[\w\u4e00-\u9fa5]+)\"?",
    re.IGNORECASE
)

def safe_read_sql(p: Path) -> str:
    for enc in ("utf-8","gbk","utf-8-sig"):
        try: return p.read_text(encoding=enc)
        except Exception: pass
    return p.read_text(encoding="utf-8", errors="ignore")

def discover_columns(sql_file: Path):
    text = safe_read_sql(sql_file)
    m = INSERT_RE.search(text)
    if not m: return []
    cols = [c.strip().strip('"') for c in m.group("cols").split(",")]
    return cols

def try_rename_from_sql(file_path: Path) -> Path:
    text = safe_read_sql(file_path)
    m = DDL_RE.search(text) or INSERT_RE.search(text)
    if not m: return file_path
    inner_table = m.group("table").strip()
    new_path = file_path.parent / f"{inner_table}.sql"
    if new_path.name == file_path.name: return file_path
    if new_path.exists(): return new_path
    file_path.rename(new_path)
    return new_path

def bulk_fix_names(sql_dir: Path):
    changed = []
    for f in sql_dir.glob("*.sql"):
        newp = try_rename_from_sql(f)
        if newp.name != f.name:
            changed.append((f.name, newp.name))
    return changed
