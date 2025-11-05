# backend/source_fields.py
import re
from pathlib import Path
from typing import List,Dict, Optional
from functools import lru_cache
SQL_DIR = Path("./source/sql")
# Postgres 常见两种备注写法：
# 1) COMMENT ON COLUMN public."table"."col" IS '备注';
COMMENT_ON_COLUMN_RE = re.compile(
    r'comment\s+on\s+column\s+(?:public\.)?"?(?P<table>[\w\u4e00-\u9fa5]+)"?\."?(?P<col>[\w\u4e00-\u9fa5]+)"?\s+is\s+[\'"](?P<cmt>.*?)[\'"];?',
    re.IGNORECASE | re.DOTALL
)
# 2) COMMENT ON TABLE public."table" IS '备注';
COMMENT_ON_TABLE_RE = re.compile(
    r'comment\s+on\s+table\s+(?:public\.)?"?(?P<table>[\w\u4e00-\u9fa5]+)"?\s+is\s+[\'"](?P<cmt>.*?)[\'"];?',
    re.IGNORECASE | re.DOTALL
)
# 3) MySQL/方言：在 CREATE TABLE 列定义里携带 COMMENT '备注'
#    例: "id" bigint COMMENT '基金唯一键',
INLINE_COL_CMT_RE = re.compile(
    r'"?(?P<col>[\w\u4e00-\u9fa5]+)"?\s+[^\n,]+?\s+comment\s+[\'"](?P<cmt>.*?)[\'"]',
    re.IGNORECASE
)
INSERT_RE = re.compile(
    r"insert\s+into\s+public\.\"?(?P<table>[\w\u4e00-\u9fa5]+)\"?\s*\((?P<cols>[^)]*)\)",
    re.IGNORECASE
)
DDL_RE = re.compile(
    r"create\s+table\s+(?:public\.)?\"?(?P<table>[\w\u4e00-\u9fa5]+)\"?\s*\((?P<cols>.*?)\)",
    re.IGNORECASE | re.DOTALL
)

@lru_cache(maxsize=128)
def detect_field_comments(table: str) -> Dict[str, str]:
    """
    返回 {字段名: 备注}。
    解析优先级：
    1) COMMENT ON COLUMN 语句
    2) CREATE TABLE 内联 COMMENT 语法（若存在）
    """
    p = detect_sql_path(table)
    if not p.exists():
        return {}
    txt = _safe_read(p)

    out: Dict[str, str] = {}

    # 1) COMMENT ON COLUMN
    for m in COMMENT_ON_COLUMN_RE.finditer(txt):
        if m.group("table") == table:
            out[m.group("col")] = m.group("cmt").strip()

    # 2) CREATE TABLE 内的 inline COMMENT
    #    先定位本表的 CREATE 段，避免误匹配其它表
    mddl = DDL_RE.search(txt)
    if mddl and mddl.group("table") == table:
        cols_blob = mddl.group("cols")
        for m in INLINE_COL_CMT_RE.finditer(cols_blob):
            col = m.group("col")
            cmt = m.group("cmt").strip()
            # 优先保留 COMMENT ON COLUMN，inline 作为补充
            out.setdefault(col, cmt)

    return out

@lru_cache(maxsize=128)
def detect_table_title(table: str) -> str:
    """
    返回表的人类可读名称（来自 COMMENT ON TABLE '备注'），没有则返回空串。
    """
    p = detect_sql_path(table)
    if not p.exists():
        return ""
    txt = _safe_read(p)

    for m in COMMENT_ON_TABLE_RE.finditer(txt):
        if m.group("table") == table:
            return m.group("cmt").strip() or ""
    return ""
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