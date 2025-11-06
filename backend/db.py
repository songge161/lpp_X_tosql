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
        source_table TEXT,
        target_entity TEXT DEFAULT '',
        priority INTEGER DEFAULT 0,
        disabled INTEGER DEFAULT 0,
        description TEXT DEFAULT '',
        py_script TEXT DEFAULT '',
        UNIQUE(source_table, target_entity)
    );
    """)

    # --- 字段映射表 field_map ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS field_map (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name TEXT,
        target_entity TEXT DEFAULT '',  -- ✅ 新增：映射目标
        source_field TEXT,
        target_paths TEXT,
        rule TEXT DEFAULT '',
        enabled INTEGER DEFAULT 1,
        order_idx INTEGER DEFAULT 0,
        last_updated INTEGER DEFAULT 0,
        UNIQUE(table_name, target_entity, source_field)
    );
    """)
    # ✅ 自动修复旧表缺少 target_entity 的情况
    try:
        cur.execute("PRAGMA table_info(field_map);")
        cols = [r[1] for r in cur.fetchall()]
        if "target_entity" not in cols:
            cur.execute("ALTER TABLE field_map ADD COLUMN target_entity TEXT DEFAULT '';")
            conn.commit()
            print("[init_db] 已为 field_map 增加 target_entity 列。")
    except Exception as e:
        print("[init_db] 检查列失败:", e)
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

def list_table_targets(table_name: str) -> List[str]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
      SELECT DISTINCT target_entity
      FROM field_map
      WHERE table_name=? AND target_entity IS NOT NULL AND target_entity<>''
      ORDER BY target_entity
    """, (table_name,))
    res = [r[0] for r in cur.fetchall()]
    conn.close()
    return res
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
    ON CONFLICT(source_table, target_entity) DO UPDATE SET
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


def get_priority(source_table: str, target_entity: str = None) -> int:
    conn = _conn()
    cur = conn.cursor()
    if target_entity:
        cur.execute(
            "SELECT priority FROM table_map WHERE source_table=? AND target_entity=?",
            (source_table, target_entity)
        )
    else:
        cur.execute("SELECT priority FROM table_map WHERE source_table=?", (source_table,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row and row[0] is not None else 0


# ========== 字段映射 ==========
def get_field_mappings(table_name: str, target_entity: str = None) -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    if target_entity:
        cur.execute("""
          SELECT id, source_field, target_paths, rule, enabled, order_idx, target_entity
          FROM field_map
          WHERE table_name=? AND target_entity=?
          ORDER BY order_idx ASC, id ASC
        """, (table_name, target_entity))
    else:
        cur.execute("""
          SELECT id, source_field, target_paths, rule, enabled, order_idx, target_entity
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
            "order_idx": int(r[5]),
            "target_entity": r[6] or ""
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


def delete_field_mapping(table_name: str, source_field: str, target_entity: str = ""):
    """删除指定源字段"""
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM field_map WHERE table_name=? AND source_field=? AND target_entity=?", (table_name, source_field, target_entity or ""))
    conn.commit()
    conn.close()


def upsert_field_mapping(
    table_name: str,
    source_field: str,
    target_paths: str,
    rule: str,
    enabled: int = 1,
    order_idx: int = 0,
    target_entity: str = "",
):
    """
    更安全的 upsert，支持多映射目标（fund / ssmjj / ...）
    若该 table + target_entity 在 table_map 中不存在，则自动创建。
    """
    conn = _conn()
    cur = conn.cursor()

    # ✅ 1. 确保 table_map 存在该 (source_table, target_entity) 组合
    cur.execute("""
        SELECT COUNT(*) FROM table_map WHERE source_table=? AND target_entity=?;
    """, (table_name, target_entity or ""))
    n = cur.fetchone()[0] or 0
    if n == 0:
        cur.execute("""
            INSERT INTO table_map (source_table, target_entity, priority, disabled, description, py_script)
            VALUES (?, ?, 0, 0, '', '');
        """, (table_name, target_entity or ""))
        print(f"[upsert_field_mapping] 已新建表配置: {table_name} ({target_entity})")

    # ✅ 2. 插入或更新字段映射（按 table+entity+field 唯一）
    cur.execute("""
        INSERT INTO field_map (table_name, target_entity, source_field, target_paths, rule, enabled, order_idx, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(table_name, target_entity, source_field)
        DO UPDATE SET
            target_paths=excluded.target_paths,
            rule=excluded.rule,
            enabled=excluded.enabled,
            order_idx=excluded.order_idx,
            last_updated=excluded.last_updated;
    """, (
        table_name,
        target_entity or "",
        source_field or "",
        target_paths or "",
        rule or "",
        int(enabled),
        int(order_idx),
        int(time.time()),
    ))
    conn.commit()
    conn.close()


def update_field_mapping(table_name: str, source_field: str, target_paths: str, rule: str, target_entity: str = ""):
    """更新单个字段"""
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
      UPDATE field_map SET target_paths=?, rule=?, last_updated=?
      WHERE table_name=? AND source_field=? AND target_entity=?
    """, (target_paths or "", rule or "", int(time.time()), table_name, source_field, target_entity or ""))
    if cur.rowcount == 0:
        upsert_field_mapping(table_name, source_field, target_paths, rule, target_entity=target_entity or "")
    conn.commit()
    conn.close()


def update_many_field_mappings(table_name: str, data: List[Dict[str, str]], target_entity: str = ""):
    """批量更新所有字段"""
    conn = _conn()
    cur = conn.cursor()
    for m in data:
        cur.execute("""
          UPDATE field_map SET target_paths=?, rule=?, last_updated=?
          WHERE table_name=? AND source_field=? AND target_entity=?
        """, (m["target_paths"], m["rule"], int(time.time()), table_name, m["source_field"], target_entity or ""))
        if cur.rowcount == 0:
            upsert_field_mapping(table_name, m["source_field"], m["target_paths"], m["rule"], target_entity=target_entity or "")
    conn.commit()
    conn.close()
def delete_table_mapping(source_table: str, target_entity: str):
    """删除表及其映射目标"""
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM table_map WHERE source_table=? AND target_entity=?", (source_table, target_entity))
    cur.execute("DELETE FROM field_map WHERE table_name=? AND target_entity=?", (source_table, target_entity))
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
def get_table_script(source_table: str, target_entity: str = None) -> str:
    conn = _conn()
    cur = conn.cursor()
    if target_entity:
        cur.execute(
            "SELECT py_script FROM table_map WHERE source_table=? AND target_entity=?",
            (source_table, target_entity)
        )
    else:
        # 兼容旧用法：未指定 entity 时，返回该表的一个脚本（按 priority 优先）
        cur.execute(
            "SELECT py_script FROM table_map WHERE source_table=? ORDER BY priority DESC LIMIT 1",
            (source_table,)
        )
    row = cur.fetchone()
    conn.close()
    return row[0] if row and row[0] else ""


def save_table_script(source_table: str, py_script: str, target_entity: str = None) -> bool:
    conn = _conn()
    cur = conn.cursor()
    if target_entity:
        cur.execute(
            "UPDATE table_map SET py_script=? WHERE source_table=? AND target_entity=?",
            (py_script, source_table, target_entity)
        )
        updated = cur.rowcount > 0
        # 详情页不负责创建新目标，这里不插入新行，保持“新增只在多映射中心”策略
        conn.commit()
        conn.close()
        return updated
    else:
        # 兼容旧用法：无 entity 时更新该表下所有行（不建议用）
        cur.execute("UPDATE table_map SET py_script=? WHERE source_table=?", (py_script, source_table))
        conn.commit()
        conn.close()
        return cur.rowcount > 0


def rename_table_target_entity(source_table: str, old_entity: str, new_entity: str) -> bool:
    """
    原子重命名：将某个表的目标实体从 old_entity 重命名为 new_entity。
    同时更新 table_map 和 field_map；若 new_entity 已存在则抛错。
    """
    if not source_table or not old_entity or not new_entity:
        raise ValueError("source_table/old_entity/new_entity 不能为空")
    if old_entity == new_entity:
        return True

    conn = _conn()
    cur = conn.cursor()
    try:
        # 已存在同名目标则阻止，避免唯一键冲突
        cur.execute(
            "SELECT 1 FROM table_map WHERE source_table=? AND target_entity=?",
            (source_table, new_entity)
        )
        if cur.fetchone():
            raise sqlite3.IntegrityError(f"目标 '{new_entity}' 已存在，无法重命名为重复目标")

        # 检查旧键是否存在，不存在则直接返回
        cur.execute(
            "SELECT 1 FROM table_map WHERE source_table=? AND target_entity=?",
            (source_table, old_entity)
        )
        if not cur.fetchone():
            return False

        # 重命名 table_map
        cur.execute(
            "UPDATE table_map SET target_entity=? WHERE source_table=? AND target_entity=?",
            (new_entity, source_table, old_entity)
        )
        # 迁移 field_map
        cur.execute(
            "UPDATE field_map SET target_entity=? WHERE table_name=? AND target_entity=?",
            (new_entity, source_table, old_entity)
        )

        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
