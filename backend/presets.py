# backend/presets.py
# -*- coding: utf-8 -*-
import sqlite3
from typing import List, Dict, Optional
from backend.db import DB_PATH


def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_presets_db():
    """确保预设表存在，并向后兼容增加列。"""
    with _conn() as conn:
        # 预设表：完整连接信息 + schema + sid
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS db_presets (
                name TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                host TEXT,
                port INTEGER,
                user TEXT,
                password TEXT,
                database TEXT NOT NULL,
                charset TEXT,
                autocommit INTEGER,
                schema TEXT,
                sid TEXT
            )
            """
        )
        # 兼容旧版本：如缺列则补齐
        try:
            cur = conn.execute("PRAGMA table_info(db_presets)")
            cols = [r[1] for r in cur.fetchall()]
            def _add(col_name: str, col_def: str):
                if col_name not in cols:
                    conn.execute(f"ALTER TABLE db_presets ADD COLUMN {col_def}")
            _add("host", "host TEXT")
            _add("port", "port INTEGER")
            _add("user", "user TEXT")
            _add("password", "password TEXT")
            _add("charset", "charset TEXT")
            _add("autocommit", "autocommit INTEGER")
            _add("schema", "schema TEXT")
            _add("sid", "sid TEXT")
        except Exception:
            pass

        # 记录最近一次应用的运行时配置（用于刷新后自动恢复）
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_state (
                id INTEGER PRIMARY KEY CHECK(id=1),
                kind TEXT,
                host TEXT,
                port INTEGER,
                user TEXT,
                password TEXT,
                database TEXT,
                charset TEXT,
                autocommit INTEGER,
                schema TEXT,
                sid TEXT
            )
            """
        )


def list_presets() -> List[Dict]:
    """列出所有已保存的库+空间预设（含完整连接信息与 sid）。"""
    with _conn() as conn:
        cur = conn.execute(
            """
            SELECT name, kind, host, port, user, password,
                   database, charset, autocommit, schema, sid
            FROM db_presets ORDER BY name
            """
        )
        rows = [dict(row) for row in cur.fetchall()]
        for r in rows:
            if r.get("autocommit") is not None:
                r["autocommit"] = bool(r["autocommit"])  # 规范化
        return rows


def save_preset(
    name: str,
    kind: str,
    host: Optional[str],
    port: Optional[int],
    user: Optional[str],
    password: Optional[str],
    database: str,
    charset: Optional[str] = None,
    autocommit: Optional[bool] = None,
    schema: Optional[str] = None,
    sid: Optional[str] = None,
) -> None:
    """保存或更新一个预设（完整连接信息 + schema + sid）。"""
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO db_presets(
                name, kind, host, port, user, password,
                database, charset, autocommit, schema, sid
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                kind=excluded.kind,
                host=excluded.host,
                port=excluded.port,
                user=excluded.user,
                password=excluded.password,
                database=excluded.database,
                charset=excluded.charset,
                autocommit=excluded.autocommit,
                schema=excluded.schema,
                sid=excluded.sid
            """,
            (
                name,
                kind,
                host,
                int(port or 0) if port is not None else None,
                user,
                password,
                database,
                charset,
                1 if autocommit else 0 if autocommit is not None else None,
                schema,
                sid,
            )
        )


def delete_preset(name: str) -> None:
    """删除一个预设。"""
    with _conn() as conn:
        conn.execute("DELETE FROM db_presets WHERE name = ?", (name,))


def get_last_runtime() -> Optional[Dict]:
    """读取最近一次应用的运行时配置，用于刷新后恢复。"""
    with _conn() as conn:
        cur = conn.execute("SELECT * FROM app_state WHERE id=1")
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        # 规范化 autocommit
        if d.get("autocommit") is not None:
            d["autocommit"] = bool(d["autocommit"])
        return d


def save_last_runtime(kind: str, cfg: Dict, sid: str) -> None:
    """保存最近一次应用的运行时配置（含 SID）。"""
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO app_state(
                id, kind, host, port, user, password, database, charset, autocommit, schema, sid
            ) VALUES (
                1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(id) DO UPDATE SET
                kind=excluded.kind,
                host=excluded.host,
                port=excluded.port,
                user=excluded.user,
                password=excluded.password,
                database=excluded.database,
                charset=excluded.charset,
                autocommit=excluded.autocommit,
                schema=excluded.schema,
                sid=excluded.sid
            """,
            (
                kind,
                cfg.get("host"),
                int(cfg.get("port") or 0) if cfg.get("port") is not None else None,
                cfg.get("user"),
                cfg.get("password"),
                cfg.get("database"),
                cfg.get("charset"),
                1 if cfg.get("autocommit") else 0,
                cfg.get("schema"),
                sid,
            )
        )