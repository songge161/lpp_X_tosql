# backend/source_fields.py
import re
from pathlib import Path
from typing import List

SQL_DIR = Path("./source/sql")

INSERT_RE = re.compile(
    r"insert\s+into\s+public\.\"?(?P<table>[\w\u4e00-\u9fa5]+)\"?\s*\((?P<cols>[^)]*)\)",
    re.IGNORECASE
)
DDL_RE = re.compile(
    r"create\s+table\s+(?:public\.)?\"?(?P<table>[\w\u4e00-\u9fa5]+)\"?\s*\((?P<cols>.*?)\)",
    re.IGNORECASE | re.DOTALL
)

def _safe_read(p: Path) -> str:
    for enc in ("utf-8", "gbk", "utf-8-sig"):
        try:
            return p.read_text(encoding=enc)
        except Exception:
            continue
    return p.read_text(encoding="utf-8", errors="ignore")

def detect_sql_path(table: str) -> Path:
    return SQL_DIR / f"{table}.sql"

def detect_source_fields(table: str) -> List[str]:
    """优先从 INSERT 解析列；没有 INSERT 再回退从 DDL 粗略提取列名。"""
    p = detect_sql_path(table)
    if not p.exists():
        return []
    txt = _safe_read(p)

    m = INSERT_RE.search(txt)
    if m:
        cols = [c.strip().strip('"') for c in m.group("cols").split(",")]
        return [c for c in cols if c]

    m = DDL_RE.search(txt)
    if m:
        cols_blob = m.group("cols")
        # 简单切分列定义，取每行第一个 token 作为列名（忽略约束）
        fields = []
        for line in cols_blob.split(","):
            line = line.strip()
            if not line or line.lower().startswith(("primary", "unique", "key", "constraint", "index")):
                continue
            name = re.split(r"\s+", line)[0].strip('"`')
            if name:
                fields.append(name)
        return fields
    return []