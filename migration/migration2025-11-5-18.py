import sqlite3
from pathlib import Path

DB_PATH = Path("/Users/songyihong/PycharmProjects/FastAPIProject/mapping_config.db")  # 你的 DB 文件路径

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("🧩 正在修复 table_map 结构...")

# 1️⃣ 检查 target_entity 字段是否存在
cur.execute("PRAGMA table_info(table_map)")
cols = [r[1] for r in cur.fetchall()]
if "target_entity" not in cols:
    cur.execute("ALTER TABLE table_map ADD COLUMN target_entity TEXT DEFAULT ''")

# 2️⃣ 重新创建带有联合唯一约束的新表
cur.executescript("""
PRAGMA foreign_keys=off;

CREATE TABLE IF NOT EXISTS table_map_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_table TEXT,
    target_entity TEXT DEFAULT '',
    priority INTEGER DEFAULT 0,
    disabled INTEGER DEFAULT 0,
    description TEXT DEFAULT '',
    py_script TEXT DEFAULT '',
    UNIQUE(source_table, target_entity)
);

-- 迁移旧数据（防止重复 target_entity）
INSERT OR IGNORE INTO table_map_new (id, source_table, target_entity, priority, disabled, description, py_script)
SELECT id, source_table, target_entity, priority, disabled, description, py_script
FROM table_map;

DROP TABLE table_map;
ALTER TABLE table_map_new RENAME TO table_map;

PRAGMA foreign_keys=on;
""")

conn.commit()
conn.close()
print("✅ 修复完成：table_map 已支持多映射 (UNIQUE(source_table, target_entity))")
