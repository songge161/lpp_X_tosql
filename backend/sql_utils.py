# backend/sql_utils.py
import re
from pathlib import Path
from typing import Dict, Any, Optional

# 现有：SQL 文件解析工具

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

# ========================
# 运行时数据库配置与工具
# ========================
_RUNTIME_DB_KIND: str = "mysql"  # mysql | pg
_RUNTIME_CFG: Dict[str, Any] = {
    "host": "127.0.0.1",
    "port": 3307,
    "user": "im",
    "password": "root",
    "database": "im",
    "charset": "utf8mb4",
    "autocommit": False,
}
_RUNTIME_SCHEMA: Optional[str] = None  # 仅 PG 使用的空间(schema)

def update_runtime_db(kind: str, cfg: Dict[str, Any]):
    """更新当前数据库类型与连接参数。kind: 'mysql' 或 'pg'。cfg 为连接配置，可包含 'schema'。"""
    global _RUNTIME_DB_KIND, _RUNTIME_CFG, _RUNTIME_SCHEMA
    k = (kind or "").strip().lower()
    if k not in ("mysql", "pg"):
        k = "mysql"
    _RUNTIME_DB_KIND = k
    # 仅拷贝我们关注的字段，避免不必要的污染
    keys = ["host", "port", "user", "password", "database", "charset", "autocommit", "schema"]
    _RUNTIME_CFG = {kk: cfg.get(kk) for kk in keys if kk in cfg}
    _RUNTIME_SCHEMA = _RUNTIME_CFG.get("schema")

def current_cfg() -> Dict[str, Any]:
    return dict(_RUNTIME_CFG)

def is_pg() -> bool:
    return _RUNTIME_DB_KIND == "pg"

_SCHEMA_SAFE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _apply_pg_search_path(conn):
    """如果设置了 schema，则在 PG 连接上应用 search_path。"""
    schema = _RUNTIME_SCHEMA
    if not schema:
        return
    if not _SCHEMA_SAFE_RE.match(schema):
        raise ValueError(f"Invalid schema name: {schema}")
    cur = conn.cursor()
    try:
        cur.execute(f"SET search_path TO {schema}")
    finally:
        cur.close()

def get_conn():
    """根据运行时配置返回数据库连接。MySQL 使用 pymysql，PostgreSQL 使用 psycopg2。"""
    if is_pg():
        import psycopg2
        conn = psycopg2.connect(
            host=_RUNTIME_CFG.get("host"),
            port=_RUNTIME_CFG.get("port"),
            user=_RUNTIME_CFG.get("user"),
            password=_RUNTIME_CFG.get("password"),
            dbname=_RUNTIME_CFG.get("database"),
        )
        _apply_pg_search_path(conn)
        return conn
    else:
        import pymysql
        return pymysql.connect(
            host=_RUNTIME_CFG.get("host"),
            port=_RUNTIME_CFG.get("port"),
            user=_RUNTIME_CFG.get("user"),
            password=_RUNTIME_CFG.get("password"),
            database=_RUNTIME_CFG.get("database"),
            charset=_RUNTIME_CFG.get("charset", "utf8mb4"),
            autocommit=_RUNTIME_CFG.get("autocommit", False),
        )

def json_equals_clause(json_col: str, key: str) -> str:
    """返回比较 data JSON 指定键等于占位符的 SQL 片段。
    - MySQL: JSON_UNQUOTE(JSON_EXTRACT(data, '$.key'))=%s
    - PG:    data->>'key'=%s
    """
    k = str(key).strip()
    if is_pg():
        return f"{json_col}->>'{k}'=%s"
    else:
        return f"JSON_UNQUOTE(JSON_EXTRACT({json_col}, '$.{k}'))=%s"
