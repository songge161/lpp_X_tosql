# backend/db.py
# -*- coding: utf-8 -*-
import sqlite3
from pathlib import Path
from typing import List, Dict, Any
import time

DB_PATH = Path("mapping_config.db")
SQL_DIR = Path("./source/sql")


# ========== 基础连接 ==========
def _conn():
    return sqlite3.connect(DB_PATH)



# ========== 初始化 ==========
def init_db():
    conn = _conn()
    cur = conn.cursor()
    # --- 主表 table_map ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS table_map (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_table TEXT UNIQUE,
        target_entity TEXT DEFAULT '',
        priority INTEGER DEFAULT 0,
        disabled INTEGER DEFAULT 0,
        description TEXT DEFAULT '',
        py_script TEXT DEFAULT ''
    );
    """)

    # --- 字段映射表 field_map ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS field_map (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name TEXT,
        source_field TEXT,
        target_paths TEXT,
        rule TEXT DEFAULT '',
        enabled INTEGER DEFAULT 1,
        order_idx INTEGER DEFAULT 0,
        last_updated INTEGER DEFAULT 0,
        UNIQUE(table_name, source_field)
    );
    """)
    conn.commit()
    conn.close()


# ========== 初始化同步 ==========
def init_from_sql():
    """从 source/sql 初始化 table_map"""
    init_db()
    conn = _conn()
    cur = conn.cursor()
    for p in SQL_DIR.glob("*.sql"):
        tname = p.stem
        cur.execute("""
        INSERT OR IGNORE INTO table_map (source_table,target_entity,priority,disabled,description)
        VALUES (?,?,?,?,?)
        """, (tname, "", 0, 0, ""))
    conn.commit()
    conn.close()


# ========== 表映射 ==========
def list_tables(include_disabled=False):
    init_from_sql()
    conn = _conn()
    cur = conn.cursor()
    sql = "SELECT source_table,target_entity,priority,disabled,description FROM table_map"
    if not include_disabled:
        sql += " WHERE disabled=0"
    sql += " ORDER BY priority DESC, source_table ASC"
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return [(p.stem, "", 0, 0, "") for p in SQL_DIR.glob("*.sql")]
    return rows


def list_mapped_tables() -> List[Dict[str, Any]]:
    """仅返回已设置 target_entity 且未禁用的表"""
    init_from_sql()
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
      SELECT source_table, target_entity, priority, disabled, description
      FROM table_map
      WHERE disabled=0 AND target_entity <> ''
      ORDER BY priority DESC, source_table ASC
    """)
    rows = [{
        "source_table": r[0],
        "target_entity": r[1],
        "priority": int(r[2]),
        "disabled": int(r[3]),
        "description": r[4] or ""
    } for r in cur.fetchall()]
    conn.close()
    return rows


def save_table_mapping(source_table: str, target_entity: str, priority: int = 0, desc: str = ""):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO table_map (source_table,target_entity,priority,description)
    VALUES (?,?,?,?)
    ON CONFLICT(source_table) DO UPDATE SET
      target_entity=excluded.target_entity,
      priority=excluded.priority,
      description=excluded.description
    """, (source_table, target_entity or "", int(priority or 0), desc or ""))
    conn.commit()
    conn.close()


def soft_delete_table(source_table: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE table_map SET disabled=1 WHERE source_table=?", (source_table,))
    conn.commit()
    conn.close()


def restore_table(source_table: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE table_map SET disabled=0 WHERE source_table=?", (source_table,))
    conn.commit()
    conn.close()


def get_target_entity(source_table: str) -> str:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT target_entity FROM table_map WHERE source_table=?", (source_table,))
    row = cur.fetchone()
    conn.close()
    return (row[0] or "") if row else ""


def get_priority(source_table: str) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT priority FROM table_map WHERE source_table=?", (source_table,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else 0


# ========== 字段映射 ==========
def get_field_mappings(table_name: str) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
      SELECT id, source_field, target_paths, rule, enabled, order_idx
      FROM field_map
      WHERE table_name=?
      ORDER BY order_idx ASC, id ASC
    """, (table_name,))
    rows = [
        {
            "id": r[0],
            "source_field": r[1],
            "target_paths": r[2],
            "rule": r[3],
            "enabled": int(r[4]),
            "order_idx": int(r[5])
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


def delete_field_mapping(table_name: str, source_field: str):
    """删除指定源字段"""
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM field_map WHERE table_name=? AND source_field=?", (table_name, source_field))
    conn.commit()
    conn.close()


def upsert_field_mapping(table_name: str, source_field: str, target_paths: str, rule: str, enabled: int = 1, order_idx: int = 0):
    """更安全的 upsert，不会无意删除其他字段"""
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO field_map (table_name,source_field,target_paths,rule,enabled,order_idx,last_updated)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(table_name, source_field) DO UPDATE SET
            target_paths=excluded.target_paths,
            rule=excluded.rule,
            enabled=excluded.enabled,
            order_idx=excluded.order_idx,
            last_updated=excluded.last_updated
    """, (table_name, source_field or "", target_paths or "", rule or "", int(enabled), int(order_idx), int(time.time())))
    conn.commit()
    conn.close()

def update_field_mapping(table_name: str, source_field: str, target_paths: str, rule: str):
    """更新单个字段"""
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
      UPDATE field_map SET target_paths=?, rule=?, last_updated=?
      WHERE table_name=? AND source_field=?
    """, (target_paths or "", rule or "", int(time.time()), table_name, source_field))
    if cur.rowcount == 0:
        upsert_field_mapping(table_name, source_field, target_paths, rule)
    conn.commit()
    conn.close()


def update_many_field_mappings(table_name: str, data: List[Dict[str, str]]):
    """批量更新所有字段"""
    conn = _conn()
    cur = conn.cursor()
    for m in data:
        cur.execute("""
          UPDATE field_map SET target_paths=?, rule=?, last_updated=?
          WHERE table_name=? AND source_field=?
        """, (m["target_paths"], m["rule"], int(time.time()), table_name, m["source_field"]))
        if cur.rowcount == 0:
            upsert_field_mapping(table_name, m["source_field"], m["target_paths"], m["rule"])
    conn.commit()
    conn.close()


# ========== 导入导出 ==========
def export_all() -> Dict[str, Any]:
    data = {"tables": [], "fields": {}}
    rows = list_tables(include_disabled=True)
    data["tables"] = [
        {"source_table": r[0], "target_entity": r[1], "priority": r[2], "disabled": r[3], "description": r[4]}
        for r in rows
    ]
    for r in rows:
        t = r[0]
        data["fields"][t] = get_field_mappings(t)
    return data


def import_all(obj: Dict[str, Any]):
    """安全的全量导入（使用单连接写入，避免嵌套锁）"""
    conn = _conn()
    cur = conn.cursor()
    conn.execute("BEGIN IMMEDIATE")  # 防止其他写事务进入
    try:
        # 清空旧配置
        cur.execute("DELETE FROM table_map")
        cur.execute("DELETE FROM field_map")

        # 写 table_map
        for t in obj.get("tables", []):
            cur.execute("""
            INSERT INTO table_map (source_table,target_entity,priority,disabled,description)
            VALUES (?,?,?,?,?)
            """, (
                t["source_table"],
                t.get("target_entity", ""),
                int(t.get("priority", 0)),
                int(t.get("disabled", 0)),
                t.get("description", "")
            ))

        # 写 field_map
        for tbl, arr in obj.get("fields", {}).items():
            for f in arr:
                cur.execute("""
                INSERT INTO field_map (table_name,source_field,target_paths,rule,enabled,order_idx,last_updated)
                VALUES (?,?,?,?,?,?,?)
                """, (
                    tbl,
                    f.get("source_field", ""),
                    f.get("target_paths", ""),
                    f.get("rule", ""),
                    int(f.get("enabled", 1)),
                    int(f.get("order_idx", 0)),
                    int(time.time())
                ))

        conn.commit()
        print("[import_all] 全量导入完成")
    except Exception as e:
        conn.rollback()
        print("[import_all error]", e)
        raise
    finally:
        conn.close()



# ========== 脚本存取 ==========
def get_table_script(source_table: str) -> str:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT py_script FROM table_map WHERE source_table=?", (source_table,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row and row[0] else ""


def save_table_script(source_table: str, py_script: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE table_map SET py_script=? WHERE source_table=?", (py_script, source_table))
    if cur.rowcount == 0:
        cur.execute("INSERT INTO table_map (source_table, py_script) VALUES (?, ?)", (source_table, py_script))
    conn.commit()
    conn.close()
