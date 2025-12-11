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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS flow_entity_map (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            flow_define_name TEXT UNIQUE,
            source_table TEXT,
            target_entity TEXT
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS file_map_cfgs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_table TEXT,
            source_field TEXT,
            entity TEXT,
            mode TEXT,
            entity_field TEXT,
            doc_uuid TEXT,
            doc_name TEXT,
            sql_field TEXT,
            match_entity_field TEXT,
            saved_at INTEGER,
            status TEXT,
            UNIQUE(source_table, source_field, entity, mode, entity_field, doc_uuid, doc_name)
        );
        """
    )
    try:
        cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='file_map_cfgs'")
        row = cur.fetchone()
        ddl = row[0] if row else ""
        if ddl and "UNIQUE(source_table, source_field, entity, mode, entity_field, doc_uuid, doc_name)" not in ddl:
            conn.execute("BEGIN IMMEDIATE")
            cur.execute("ALTER TABLE file_map_cfgs RENAME TO file_map_cfgs_old")
            cur.execute(
                """
                CREATE TABLE file_map_cfgs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_table TEXT,
                    source_field TEXT,
                    entity TEXT,
                    mode TEXT,
                    entity_field TEXT,
                    doc_uuid TEXT,
                    doc_name TEXT,
                    sql_field TEXT,
                    match_entity_field TEXT,
                    saved_at INTEGER,
                    status TEXT,
                    UNIQUE(source_table, source_field, entity, mode, entity_field, doc_uuid, doc_name)
                );
                """
            )
            cur.execute(
                """
                INSERT INTO file_map_cfgs (id, source_table, source_field, entity, mode, entity_field, doc_uuid, doc_name, sql_field, match_entity_field, saved_at, status)
                SELECT id, source_table, source_field, entity, mode, entity_field, doc_uuid, doc_name, sql_field, match_entity_field, saved_at, status
                FROM file_map_cfgs_old
                """
            )
            cur.execute("DROP TABLE file_map_cfgs_old")
            conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
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

def get_flow_entity_map(flow_define_name: str) -> Dict[str, str]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT source_table,target_entity FROM flow_entity_map WHERE flow_define_name=?",
        (flow_define_name,)
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return {}
    return {"source_table": row[0] or "", "target_entity": row[1] or ""}

def upsert_flow_entity_map(flow_define_name: str, source_table: str, target_entity: str) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO flow_entity_map (flow_define_name,source_table,target_entity)
        VALUES (?,?,?)
        ON CONFLICT(flow_define_name) DO UPDATE SET
          source_table=excluded.source_table,
          target_entity=excluded.target_entity
        """,
        (flow_define_name, source_table or "", target_entity or "")
    )
    conn.commit()
    conn.close()

def list_flow_entity_maps() -> List[Dict[str, str]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT flow_define_name,source_table,target_entity FROM flow_entity_map ORDER BY flow_define_name")
    rows = [{"flow_define_name": r[0], "source_table": r[1] or "", "target_entity": r[2] or ""} for r in cur.fetchall()]
    conn.close()
    return rows


def upsert_file_map_cfg(cfg: Dict[str, Any]) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO file_map_cfgs (
            source_table, source_field, entity, mode, entity_field, doc_uuid, doc_name, sql_field, match_entity_field, saved_at, status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(source_table, source_field, entity, mode, entity_field, doc_uuid, doc_name)
        DO UPDATE SET entity_field=excluded.entity_field, doc_uuid=excluded.doc_uuid, doc_name=excluded.doc_name,
                      sql_field=excluded.sql_field, match_entity_field=excluded.match_entity_field,
                      saved_at=excluded.saved_at, status=excluded.status
        """,
        (
            cfg.get("source_table",""), cfg.get("source_field",""), cfg.get("entity",""), cfg.get("mode",""),
            cfg.get("entity_field",""), cfg.get("doc_uuid") or "", cfg.get("doc_name") or "",
            cfg.get("sql_field",""), cfg.get("match_entity_field",""), int(cfg.get("saved_at") or int(time.time())), cfg.get("status") or ""
        )
    )
    conn.commit()
    cur.execute(
        "SELECT id FROM file_map_cfgs WHERE source_table=? AND source_field=? AND entity=? AND mode=? AND entity_field=? AND doc_uuid=? AND doc_name=?",
        (cfg.get("source_table",""), cfg.get("source_field",""), cfg.get("entity",""), cfg.get("mode",""), cfg.get("entity_field",""), cfg.get("doc_uuid") or "", cfg.get("doc_name") or "")
    )
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else 0


def list_file_map_cfgs() -> List[Dict[str, Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id, source_table, source_field, entity, mode, entity_field, doc_uuid, doc_name, sql_field, match_entity_field, saved_at, status FROM file_map_cfgs ORDER BY saved_at DESC, id DESC")
    rows = [
        {
            "id": int(r[0]),
            "source_table": r[1] or "",
            "source_field": r[2] or "",
            "entity": r[3] or "",
            "mode": r[4] or "",
            "entity_field": r[5] or "",
            "doc_uuid": r[6] or "",
            "doc_name": r[7] or "",
            "sql_field": r[8] or "",
            "match_entity_field": r[9] or "",
            "saved_at": int(r[10] or 0),
            "status": r[11] or "",
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


def delete_file_map_cfg_by_id(cfg_id: int) -> bool:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM file_map_cfgs WHERE id=?", (int(cfg_id),))
    conn.commit()
    ok = cur.rowcount > 0
    conn.close()
    return ok


def update_file_map_status(cfg_id: int, status: str) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE file_map_cfgs SET status=? WHERE id=?", (status or "", int(cfg_id)))
    conn.commit()
    conn.close()


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

def rename_field_source(table_name: str, old_source_field: str, new_source_field: str, target_entity: str = "") -> bool:
    if not table_name or not old_source_field or not new_source_field:
        return False
    if old_source_field == new_source_field:
        return True
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT target_paths, rule, enabled, order_idx FROM field_map
            WHERE table_name=? AND source_field=? AND target_entity=?
            """,
            (table_name, old_source_field, target_entity or "")
        )
        row = cur.fetchone()
        if not row:
            return False
        cur.execute(
            "SELECT 1 FROM field_map WHERE table_name=? AND source_field=? AND target_entity=?",
            (table_name, new_source_field, target_entity or "")
        )
        exists = cur.fetchone() is not None
        now_ts = int(time.time())
        if exists:
            cur.execute(
                """
                UPDATE field_map SET target_paths=?, rule=?, enabled=?, order_idx=?, last_updated=?
                WHERE table_name=? AND source_field=? AND target_entity=?
                """,
                (row[0], row[1], int(row[2]), int(row[3]), now_ts, table_name, new_source_field, target_entity or "")
            )
            cur.execute(
                "DELETE FROM field_map WHERE table_name=? AND source_field=? AND target_entity=?",
                (table_name, old_source_field, target_entity or "")
            )
        else:
            cur.execute(
                """
                UPDATE field_map SET source_field=?, last_updated=?
                WHERE table_name=? AND source_field=? AND target_entity=?
                """,
                (new_source_field, now_ts, table_name, old_source_field, target_entity or "")
            )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
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
