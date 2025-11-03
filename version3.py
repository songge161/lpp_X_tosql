# -*- coding: utf-8 -*-
"""
import_pgsql_to_mysql_entity_v8.6.py
-------------------------------------
增强版：
✅ 自动重命名 temp_*.sql（基于 DDL/INSERT）
✅ 中文表名支持、UTF-8/GBK 自适应
✅ 每条记录生成 uuid（唯一）+ sid（批次）
✅ uuid / sid 与 data 平级
✅ 多线程导入 + 缓存同步 + 幂等控制
✅ 外部模块 custom_handler.py 控制：
   - 指定要处理的表
   - 每表字段定制处理逻辑
   - 默认清洗逻辑 fallback
"""
import re, json, time, pymysql, traceback, threading, os, random, importlib
from pathlib import Path
from datetime import datetime
from typing import List, Any, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from backend.mapper_core import apply_record_mapping, get_target_entity, get_all_prioritized_tables, get_table_priority

# ========== 配置 ==========
SQL_DIR = "./source/sql"
TABLE_FILE = Path("./source/table_temp.txt")
CHANGE_FILE = Path("./source/change_temp.txt")
LOG_FILE = "import_log.txt"
THREADS = 6
DRY_RUN = False
DELETE_MODE = "physical"  # "logical" or "physical"
SID = "i6qzt3nn20"  # ← 全局宏定义空间
MYSQL_CFG = dict(
    host="127.0.0.1",
    port=3307,
    user="im",
    password="root",
    database="im",
    charset="utf8mb4",
    autocommit=False
)
# ==========================

# ---------- UUID ----------
_lock = threading.Lock()
_counter = 0
def base36_encode(n: int) -> str:
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    s = ""
    while n:
        n, r = divmod(n, 36)
        s = chars[r] + s
    return s or "0"

def uuid() -> str:
    """固定10位唯一短UUID"""
    global _counter
    with _lock:
        _counter = (_counter + 1) & 0xFF
    t = int(time.time()) % 1000
    us = int(time.time() * 1e6) % 100
    pid = os.getpid() % 100
    rnd = random.randint(0, 46655)
    n = (t << 24) | (us << 16) | (pid << 8) | _counter
    code = base36_encode(n) + base36_encode(rnd)
    return code[-10:].rjust(10, "0")

# ---------- 导入自定义模块 ----------
try:
    custom_handler = importlib.import_module("custom_handler")
    ENABLED_TABLES = getattr(custom_handler, "ENABLED_TABLES", [])
    log_prefix = f"🔧 已加载自定义模块 custom_handler.py，启用表：{ENABLED_TABLES}" if ENABLED_TABLES else "🪶 custom_handler.py 加载成功（未限制表）"
    print(log_prefix)
except ModuleNotFoundError:
    custom_handler = None
    ENABLED_TABLES = []
    print("⚠️ 未找到 custom_handler.py，使用默认逻辑。")

# ---------- 正则 ----------
INSERT_RE = re.compile(
    r"insert\s+into\s+public\.\"?(?P<table>[\w\u4e00-\u9fa5]+)\"?\s*\((?P<cols>[^)]*)\)\s*values\s*\((?P<vals>[^)]*)\)\s*;",
    re.IGNORECASE
)
DDL_RE = re.compile(
    r"create\s+table\s+(?:public\.)?\"?(?P<table>[\w\u4e00-\u9fa5]+)\"?",
    re.IGNORECASE
)

# ---------- 工具 ----------
def safe_read_sql(file: Path) -> str:
    for enc in ("utf-8", "gbk", "utf-8-sig"):
        try:
            return file.read_text(encoding=enc)
        except Exception:
            continue
    return file.read_text(encoding="utf-8", errors="ignore")

def read_list(path: Path) -> List[str]:
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8", errors="ignore")
    return [re.sub(r"[^\w\u4e00-\u9fa5_]", "", line.strip()) for line in raw.splitlines() if line.strip()]

def write_list(path: Path, data: List[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(sorted(set(data))) + "\n", encoding="utf-8")

def to_timestamp(val: str) -> int:
    if not val:
        return int(time.time())
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(val, fmt).timestamp())
        except Exception:
            pass
    return int(time.time())

def log(msg: str):
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

# ---------- 自动重命名 ----------
def try_rename_from_sql(file_path: Path) -> Path:
    text = safe_read_sql(file_path)
    m = DDL_RE.search(text) or INSERT_RE.search(text)
    if not m:
        return file_path
    inner_table = m.group("table").strip()
    new_path = file_path.parent / f"{inner_table}.sql"
    if new_path.name == file_path.name:
        return file_path
    if new_path.exists():
        log(f"[SKIP_RENAME] {file_path.name} → {new_path.name} 已存在，跳过。")
        return new_path
    file_path.rename(new_path)
    log(f"[RENAME] {file_path.name} → {new_path.name}")
    return new_path

# ---------- SQL解析 ----------
def parse_values(raw: str) -> List[Any]:
    out, buf, in_str = [], [], False
    i = 0
    while i < len(raw):
        ch = raw[i]
        if in_str:
            if ch == "'":
                if i + 1 < len(raw) and raw[i + 1] == "'":
                    buf.append("'"); i += 2
                else:
                    in_str = False; i += 1
            else:
                buf.append(ch); i += 1
        else:
            if ch == "'": in_str = True; i += 1
            elif ch == ",": token = "".join(buf).strip(); out.append(_normalize(token)); buf=[]; i += 1
            else: buf.append(ch); i += 1
    token = "".join(buf).strip()
    out.append(_normalize(token))
    return out

def _normalize(token: str) -> Any:
    if token.upper() == "NULL": return ""
    if token.startswith("'") and token.endswith("'"): return token[1:-1].replace("''", "'")
    return token

def parse_sql_file(file_path: Path) -> List[Tuple[str, str, str, str, str, int, int, int]]:
    """
    解析 SQL 文件 -> [(uuid, sid, type, name, data, del, input_ts, update_ts)]
    """
    text = safe_read_sql(file_path)
    matches = INSERT_RE.finditer(text)
    entities = []

    for m in matches:
        table = m.group("table")
        cols = [c.strip().strip('"') for c in m.group("cols").split(",")]
        vals = parse_values(m.group("vals"))
        if len(cols) != len(vals):
            continue

        record = dict(zip(cols, vals))
        deleted_val = record.get("deleted", "")
        create_time_val = record.get("create_time", "")
        update_time_val = record.get("update_time", "")
        for k in ("deleted", "create_time", "update_time"):
            record.pop(k, None)

        # --- custom_handler ---
        if custom_handler:
            func = getattr(custom_handler, table, None)
            if callable(func):
                try:
                    record = func(record) or record
                except Exception as e:
                    log(f"[WARN] {table} 自定义处理异常: {e}")
            elif hasattr(custom_handler, "default"):
                record = custom_handler.default(record, table)

        # --- 应用 GUI 配置规则 ---
        name_val = record.pop("__name__", "") or ""
        try:
            mapped_record, out_name, type_override = apply_record_mapping(table, record)
            if mapped_record:
                record = mapped_record
            if out_name:
                name_val = out_name
            if type_override:
                table = type_override
        except Exception as e:
            log(f"[WARN] {table} 规则应用失败: {e}")

        # --- 序列化 JSON ---
        data_json = json.dumps(record, ensure_ascii=False)
        uuid_val = uuid()
        entities.append((
            uuid_val,
            SID,
            table,
            name_val or "",   # 确保 name 不为 None
            data_json,
            int(deleted_val or 0),
            to_timestamp(create_time_val),
            to_timestamp(update_time_val)
        ))

    return entities
# ---------- MySQL ----------
def ensure_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS entity (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        uuid VARCHAR(64) NOT NULL,
        sid VARCHAR(64) NOT NULL,
        name VARCHAR(255) NOT NULL,
        type VARCHAR(128) NOT NULL,
        data JSON NOT NULL,
        del TINYINT DEFAULT 0,
        input_date BIGINT DEFAULT 0,
        update_date BIGINT DEFAULT 0,
        KEY idx_type (type),
        KEY idx_sid (sid)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)

def insert_entities(rows):
    conn = pymysql.connect(**MYSQL_CFG)
    try:
        ensure_table(conn)
        sql = """
        INSERT INTO entity
        (`uuid`, `sid`, `type`, `name`, `data`, `del`, `input_date`, `update_date`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"[ERROR_INSERT] {e}")
    finally:
        conn.close()

def handle_deleted_tables(del_arr: List[str], mode="logical"):
    if not del_arr: return
    conn = pymysql.connect(**MYSQL_CFG)
    with conn.cursor() as cur:
        for t in del_arr:
            if mode == "logical":
                cur.execute("UPDATE entity SET del=1 WHERE type=%s", (t,))
                log(f"🟡 表 {t} 已逻辑删除")
            else:
                cur.execute("DELETE FROM entity WHERE type=%s", (t,))
                log(f"🗑️ 表 {t} 已物理删除")
        conn.commit()
    conn.close()

# ---------- 状态同步 ----------
def sync_state(data: List[str]):
    write_list(TABLE_FILE, data)
    write_list(CHANGE_FILE, data)
    log(f"🔄 状态文件已同步，共 {len(data)} 张表。")

# ---------- 主处理 ----------
def process_file(file_path: Path, allowed: Set[str]) -> str:
    try:
        real_path = try_rename_from_sql(file_path)
        table = real_path.stem
        if ENABLED_TABLES and table not in ENABLED_TABLES:
            log(f"[SKIP] {table} 不在 custom_handler 启用列表中。")
            return None
        rows = parse_sql_file(real_path)
        if allowed and table not in allowed:
            log(f"[SKIP] {table} 不在允许列表中。")
            return None
        if not rows:
            log(f"[EMPTY] {table} 无有效记录。")
            return table
        insert_entities(rows)
        log(f"[OK] {table}: {len(rows)} 条导入成功。")
        return table
    except Exception as e:
        log(f"[ERROR] {file_path.name}: {e}\n{traceback.format_exc()}")
        return None

def compute_diff(table_state: List[str], user_state: List[str]) -> Tuple[List[str], List[str]]:
    ts, us = set(table_state), set(user_state)
    return list(us - ts), list(ts - us)

# ---------- 主入口 ----------
def main():
    """
    主入口函数
    ----------------------------
    ✅ 支持多层执行顺序（基础表 → 普通表 → 依赖重表）
    ✅ 每层内部多线程并行
    ✅ 自动跳过已导入表
    ✅ 自动更新缓存文件状态
    """
    sql_dir = Path(SQL_DIR)
    all_files = sorted(sql_dir.glob("*.sql"))
    if not all_files:
        print("❌ 未找到 .sql 文件")
        return

    # ---------- 状态初始化 ----------
    table_state = read_list(TABLE_FILE)
    user_state = read_list(CHANGE_FILE)
    unlimited = len(user_state) == 0

    if unlimited:
        log("⚙️ 无限制模式：导入所有未导入表。")
        add_arr, del_arr = [], []
        allowed = set()
    else:
        add_arr, del_arr = compute_diff(table_state, user_state)
        allowed = set(user_state)
        if add_arr:
            log(f"➕ 新增表: {add_arr}")
        if del_arr:
            log(f"🗑️ 删除表: {del_arr}")
            handle_deleted_tables(del_arr, DELETE_MODE)

    imported = set()
    already = set(table_state)
    t0 = time.time()
    # 动态优先级（来自 GUI 配置），数字越大越先执行
    try:
        dyn_order = get_all_prioritized_tables()
        # 你原来的静态分层还可保留在后面
    except Exception:
        dyn_order = []
    # ---------- 定义分层执行优先级 ----------
    priority_layers = [
        dyn_order,  # 来自 GUI 的优先队列（先跑）
        # 第一层：基础表（优先执行，被引用最多）
        ["ct_fund_manage_firm", "ct_fund_base_info", "ct_fund_invest"],
        # 第二层：普通表（无特别依赖）
        [],
        # 第三层：强依赖中间表（最后执行）
        ["ct_investor_fund_base", "ct_fund_firm_mid"],
    ]

    all_file_map = {f.stem: f for f in all_files}

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        for layer_id, layer_tables in enumerate(priority_layers, start=1):
            log(f"🧩 开始第 {layer_id} 层导入：{layer_tables or '自动识别'}")
            layer_tasks = []
            layer_imported = set()

            # 如果该层为空 → 自动选取未导入、未分类的表
            if not layer_tables:
                classified = sum(priority_layers, [])
                layer_tables = [
                    name for name in all_file_map.keys()
                    if name not in already and name not in classified
                ]

            # 层内任务分发
            for table_name in layer_tables:
                if table_name in already:
                    log(f"[SKIP] {table_name} 已导入，跳过。")
                    continue
                if allowed and table_name not in allowed:
                    continue
                f = all_file_map.get(table_name)
                if not f:
                    log(f"[WARN] 未找到 {table_name}.sql，跳过。")
                    continue
                layer_tasks.append(ex.submit(process_file, f, allowed))

            # 等待层内任务完成
            for fut in as_completed(layer_tasks):
                r = fut.result()
                if r:
                    imported.add(r)
                    layer_imported.add(r)

            log(f"✅ 第 {layer_id} 层完成：共 {len(layer_imported)} 张表。")

    # ---------- 同步最终状态 ----------
    final_state = sorted((set(table_state) - set(del_arr)) | imported | set(add_arr))
    sync_state(final_state)

    # ---------- 完成日志 ----------
    log(
        f"✅ 同步完成，共 {len(final_state)} 张表，"
        f"新增 {len(imported)} 张，用时 {time.time()-t0:.2f}s"
    )
    print(
        f"\n✅ 导入流程完成：\n"
        f"  总表数：{len(final_state)}\n"
        f"  新增导入：{len(imported)}\n"
        f"  删除表：{len(del_arr)}\n"
        f"  总耗时：{time.time()-t0:.2f}s\n"
    )


# ---------- 表状态更新（增强+数据处理） ----------
def update_table_list(add_arr=None, del_arr=None, sync_db=True, process_data=True):
    """
    表状态更新函数（文件 + 数据库 + 可选数据处理）
    ------------------------------------------------------------
    ✅ 默认自动同步数据库
    ✅ 可选参数 process_data=True：新增表时自动导入 SQL 数据并执行清洗
    ✅ 支持增删并行
    ✅ 自动写日志、打印状态
    """
    add_arr = add_arr or []
    del_arr = del_arr or []

    table_state = read_list(TABLE_FILE)
    change_state = read_list(CHANGE_FILE)
    table_set = set(table_state)
    change_set = set(change_state)

    before_table, before_change = len(table_set), len(change_set)

    # --- 增加 ---
    added = []
    for t in add_arr:
        t = t.strip()
        if not t:
            continue
        if t not in table_set:
            table_set.add(t)
            change_set.add(t)
            added.append(t)

    # --- 删除 ---
    removed = []
    for t in del_arr:
        t = t.strip()
        if not t:
            continue
        if t in table_set:
            table_set.discard(t)
            change_set.discard(t)
            removed.append(t)

    # --- 写文件同步 ---
    write_list(TABLE_FILE, sorted(table_set))
    write_list(CHANGE_FILE, sorted(change_set))
    log(f"[UPDATE_LIST] add={added}, del={removed}, total={len(table_set)}")

    print(f"✅ 文件状态已更新：+{len(added)} / -{len(removed)}")
    print(f"📁 当前表数量：table_temp={len(table_set)} (原 {before_table}) | change_temp={len(change_set)} (原 {before_change})")

    # --- 删除数据库 ---
    if sync_db and removed:
        try:
            handle_deleted_tables(removed, DELETE_MODE)
            log(f"[SYNC_DB] 已同步数据库删除 {removed}")
            print(f"🗑️ 已同步删除数据库中 {len(removed)} 张表。")
        except Exception as e:
            log(f"[ERROR_SYNC_DB] {e}")
            print(f"⚠️ 同步数据库删除失败: {e}")

    # --- 新增表数据导入 ---
    if process_data and added:
        print(f"⚙️ 正在处理新增表数据：{added}")
        sql_dir = Path(SQL_DIR)
        all_files = {f.stem: f for f in sql_dir.glob("*.sql")}
        for t in added:
            f = all_files.get(t)
            if not f:
                log(f"[WARN] 未找到 {t}.sql 文件，跳过。")
                continue
            try:
                table_name = process_file(f, allowed=set([t]))
                if table_name:
                    log(f"[PROCESS_OK] {t} 已重新导入。")
                    print(f"✅ {t} 已重新导入。")
            except Exception as e:
                log(f"[ERROR_PROCESS] {t}: {e}")
                print(f"⚠️ {t} 导入失败: {e}")

    return list(sorted(table_set))

if __name__ == "__main__":
    main()
