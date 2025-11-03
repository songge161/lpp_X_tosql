# backend/mapper_core.py
# -*- coding: utf-8 -*-
from typing import Dict, Any, Tuple, Optional, List
import re, pymysql, json, time
from pathlib import Path

from backend.db import list_tables, get_priority, get_field_mappings, get_target_entity
from backend.source_fields import detect_sql_path

try:
    from version3 import MYSQL_CFG, SID
except Exception:
    MYSQL_CFG = dict(
        host="127.0.0.1", port=3307, user="im", password="root",
        database="im", charset="utf8mb4", autocommit=False
    )
    SID = "default_sid"

_CACHE = {}

# ================= 安全 Entity Fetch =================
def _entity_fetch(type_name: str, where_field: str, where_val: Any, target_path: str) -> Optional[Any]:
    """安全的 entity 查询：未命中返回 None，不复用旧缓存。"""
    key = f"E:{type_name}:{where_field}={where_val}:{target_path}"
    conn = None
    try:
        conn = pymysql.connect(**MYSQL_CFG)
        with conn.cursor() as cur:
            if target_path == "uuid":
                sql = """
                    SELECT uuid FROM entity
                    WHERE type=%s AND JSON_UNQUOTE(JSON_EXTRACT(data,%s))=%s LIMIT 1
                """
                cur.execute(sql, (type_name, f"$.{where_field}", where_val))
            else:
                path = "$." + target_path.replace("data.", "")
                sql = """
                    SELECT JSON_UNQUOTE(JSON_EXTRACT(data,%s))
                    FROM entity WHERE type=%s AND JSON_UNQUOTE(JSON_EXTRACT(data,%s))=%s LIMIT 1
                """
                cur.execute(sql, (path, type_name, f"$.{where_field}", where_val))
            row = cur.fetchone()
            if row and row[0]:
                _CACHE[key] = row[0]
                return row[0]
            else:
                # ❗没查到时，强制清理缓存旧值
                if key in _CACHE:
                    del _CACHE[key]
                return None
    except Exception as e:
        print("[_entity_fetch error]", e)
        return None
    finally:
        if conn:
            conn.close()


# ================= entity(...) JOIN 模式 =================
ENTITY_JOIN_RE = re.compile(
    r"entity\((?P<target>\w+):(?P<tfield>[\w\.]+)=(?P<sexpr>[\w\.]+)\)\.(?P<path>[\w\.]+)"
)

def _eval_entity_join(expr: str, record: Dict[str, Any]) -> Optional[Any]:
    """支持 entity(ct_fund_firm_mid:data.fund_id=ct_fund_base_info.data.id).uuid"""
    m = ENTITY_JOIN_RE.fullmatch(expr.strip())
    if not m:
        return None
    target_tbl = m.group("target")
    target_field = m.group("tfield")
    source_expr = m.group("sexpr")
    target_path = m.group("path")

    # 获取右值
    parts = source_expr.split(".")
    v = record
    for seg in parts:
        if isinstance(v, dict):
            v = v.get(seg)
        else:
            v = None
            break
    if v is None:
        return None
    return _entity_fetch(target_tbl, target_field.replace("data.", ""), v, target_path)


# ================= 直接按 type+id 取值（可选） =================
def _entity_rel_fetch(type_name: str, entity_id: Any, field: str = "uuid") -> Optional[Any]:
    """entity_rel(ct_company_info,12345)"""
    key = f"REL:{type_name}:{entity_id}:{field}"
    if key in _CACHE:
        return _CACHE[key]
    conn = pymysql.connect(**MYSQL_CFG)
    try:
        with conn.cursor() as cur:
            if field == "uuid":
                cur.execute("SELECT uuid FROM entity WHERE type=%s AND JSON_UNQUOTE(JSON_EXTRACT(data,'$.id'))=%s LIMIT 1", (type_name, entity_id))
            else:
                cur.execute("SELECT JSON_UNQUOTE(JSON_EXTRACT(data,%s)) FROM entity WHERE type=%s AND JSON_UNQUOTE(JSON_EXTRACT(data,'$.id'))=%s LIMIT 1",
                            (f"$.{field}", type_name, entity_id))
            row = cur.fetchone()
            if row and row[0]:
                _CACHE[key] = row[0]
                return row[0]
    except Exception as e:
        print("[_entity_rel_fetch error]", e)
    finally:
        conn.close()
    return None


# ================= Python 安全求值（py:{...}） =================
FUNC_COALESCE = re.compile(r"coalesce\((.+)\)$", re.I)
FUNC_CONCAT   = re.compile(r"concat\((.+)\)$", re.I)
PY_RE         = re.compile(r"py:(?P<expr>.+)", re.S)
ENTITY_SIMPLE_RE = re.compile(r"entity\((?P<typ>\w+)(?:,by=(?P<by>\w+))?(?:,src=(?P<src>\w+))?\)\.(?P<path>[\w\.]+)")
REL_RE           = re.compile(r"rel\((?P<typ>\w+)(?:,by=(?P<by>\w+))?(?:,src=(?P<src>\w+))?\)$")
SOURCE_RE        = re.compile(r"source\((?P<table>\w+)\.(?P<field>\w+)=(?P<sexpr>[\w\.]+)\)\.(?P<target>\w+)")


def _split_args(s: str) -> List[str]:
    return [a.strip() for a in s.split(",")]


def _eval_atom(atom: str, record: Dict[str, Any]) -> Any:
    a = atom.strip()
    if not a:
        return ""
    if a.startswith("'") and a.endswith("'"):
        return a[1:-1]
    if a.startswith("record."):
        return record.get(a[7:], "")
    if a.startswith("entity("):
        v = _eval_entity_join(a, record)
        # ✅ None 或空字符串都算“没命中”
        if v not in (None, "null", "NULL"):
            return v
    if a in record:
        return record.get(a)
    return a


def _eval_rule(rule: str, record: Dict[str, Any]) -> Any:
    import ast, datetime
    from types import SimpleNamespace

    r = (rule or "").strip()
    if not r:
        return None

    # ========== ✅ 新增: date(fmt, field) 或 date(fmt, record.xxx) ==========
    # 支持两种写法：
    #   date(%Y-%m-%d, ic_register_date)
    #   date(%Y-%m-%d, record.fund_record_time)
    if r.lower().startswith("date(") and "," in r:
        try:
            # 提取参数部分
            inner = r[5:-1]
            fmt_part, field_part = [x.strip() for x in inner.split(",", 1)]
            fmt = fmt_part.strip()
            if fmt.startswith("'") or fmt.startswith('"'):
                fmt = fmt[1:-1]
            # 取值
            if field_part.startswith("record."):
                val = record.get(field_part[7:], "")
            else:
                val = record.get(field_part, "")
            if not val:
                return ""
            # 格式化
            for f in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.datetime.strptime(str(val).split(".")[0], f)
                    return dt.strftime(fmt)
                except Exception:
                    continue
            return str(val).split(" ")[0]
        except Exception as e:
            print("[date(fmt, field) error]", e)
            return ""

    # ========== ✅ 保留: date:%Y-%m-%d 旧写法 ==========
    if r.startswith("date:"):
        fmt = r[5:].strip()
        val = record.get("fund_record_time") or record.get("date") or ""
        if not val:
            return ""
        try:
            for f in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.datetime.strptime(str(val).split(".")[0], f)
                    return dt.strftime(fmt)
                except Exception:
                    continue
        except Exception:
            pass
        return str(val).split(" ")[0]

    # ========== coalesce(...) ==========
    m = FUNC_COALESCE.match(r)
    if m:
        for x in _split_args(m.group(1)):
            v = _eval_atom(x, record)
            if v not in (None, "", "null", "NULL"):
                return v
        return ""

    # ========== concat(...) ==========
    m = FUNC_CONCAT.match(r)
    if m:
        return "".join(str(_eval_atom(x, record) or "") for x in _split_args(m.group(1)))

    # ========== entity 简单 ==========
    m = ENTITY_SIMPLE_RE.fullmatch(r)
    if m:
        typ, by, src, path = m.group("typ"), m.group("by") or "id", m.group("src"), m.group("path")
        src_val = record.get(src or f"{typ}_id") or record.get("id")
        if src_val is None:
            return None
        return _entity_fetch(typ, by, src_val, path)

    # ========== entity join ==========
    m = ENTITY_JOIN_RE.fullmatch(r)
    if m:
        target_tbl, tfield, sexpr, target_path = m.group("target"), m.group("tfield"), m.group("sexpr"), m.group("path")
        src_val = _eval_atom(sexpr, record)
        if src_val is None:
            return None
        return _entity_fetch(target_tbl, tfield.replace("data.", ""), src_val, target_path)

    # ========== rel(...) ==========
    m = REL_RE.fullmatch(r)
    if m:
        typ, by, src = m.group("typ"), m.group("by") or "id", m.group("src")
        src_val = record.get(src or f"{typ}_id") or record.get("id")
        if src_val is None:
            return None
        return _entity_fetch(typ, by, src_val, "uuid")

    # ========== source(...) ==========
    m = SOURCE_RE.fullmatch(r)
    if m:
        table, field, sexpr, target = m.group("table"), m.group("field"), m.group("sexpr"), m.group("target")
        src_val = _eval_atom(sexpr, record)
        return f"[source:{table}.{field}={src_val}->{target}]"

    # ========== py:{...} ==========
    m = PY_RE.match(r)
    if m:
        expr = m.group("expr").strip()
        rec_obj = SimpleNamespace(**{k: v for k, v in record.items()})

        # --- py:{...}.get(...) 字典映射 ---
        if ".get(" in expr and expr.strip().startswith("{"):
            try:
                dict_part, _, tail = expr.partition("}.get(")
                dict_part = dict_part + "}"
                args_part = tail.rsplit(")", 1)[0]
                mapping = ast.literal_eval(dict_part)
                args = [a.strip() for a in args_part.split(",")]
                key_expr = args[0]
                default_val = ast.literal_eval(args[1]) if len(args) > 1 else ""

                def _eval_key(_e: str):
                    _e = _e.strip()
                    if _e.startswith("record."):
                        return record.get(_e[7:], "")
                    if _e in record:
                        return record[_e]
                    return _e

                raw_val = _eval_key(key_expr)
                if isinstance(raw_val, str) and "," in raw_val:
                    parts = [p.strip() for p in raw_val.split(",") if p.strip()]
                    mapped = [mapping.get(p, default_val) for p in parts]
                    return ",".join(mapped)
                return mapping.get(str(raw_val), default_val)
            except Exception as e:
                print("[py-get parse error]", e)
                return None

        # --- 常规 py 表达式 ---
        try:
            safe_globals = {
                "__builtins__": {"str": str, "int": int, "float": float, "len": len,
                                 "round": round, "dict": dict, "list": list, "abs": abs}
            }
            val = eval(expr, safe_globals, {"record": rec_obj, **record})
            return val
        except Exception as e:
            print("[py expr error]", e)
            return None

    # ========== 默认 ==========
    return _eval_atom(r, record)



# ================= 应用映射 =================
def apply_record_mapping(source_table: str, record: Dict[str, Any], py_script: str = "") -> Tuple[Dict[str, Any], str, str]:
    # 每次映射前清空 Entity 缓存，避免脏缓存
    _CACHE.clear()

    mappings = get_field_mappings(source_table)
    type_override = get_target_entity(source_table) or ""
    out_name = ""
    new_rec = dict(record)

    for m in mappings:
        if not int(m["enabled"]):
            continue

        targets = [t.strip() for t in (m["target_paths"] or "").split(",") if t.strip()]
        rule = (m["rule"] or "").strip()
        src = m["source_field"] or ""

        # ---------- 规则求值 ----------
        if "||" in rule and len(targets) > 1:
            # 多规则分发：rule_a || rule_b -> 对应多个 target
            sub_rules = [x.strip() for x in rule.split("||")]
            for i, t in enumerate(targets):
                sel_rule = sub_rules[i] if i < len(sub_rules) else sub_rules[-1]
                val_i = _eval_rule(sel_rule, new_rec)

                # ✅ 关键点：只要写了 rule，就不允许回退到 source_field
                if val_i is None:
                    val_i = ""   # 不回退到 new_rec.get(src, "")

                _assign_target(new_rec, t, val_i, name_holder=lambda v: _set_name(new_rec, v))
        else:
            # 单规则或无规则
            if rule:
                val = _eval_rule(rule, new_rec)

                # ✅ 关键点：只要写了 rule，就不允许回退到 source_field
                if val is None:
                    val = ""    # 不回退

            else:
                # 没写 rule，保持“透传回退”到源字段
                val = new_rec.get(src, "")

            for t in targets:
                _assign_target(new_rec, t, val, name_holder=lambda v: _set_name(new_rec, v))

    if "__name__" not in new_rec:
        new_rec["__name__"] = out_name or new_rec.get("__name__", "")

    # ---------- 表级脚本 ----------
    if py_script and py_script.strip():
        try:
            safe_globals = {
                "__builtins__": {
                    "len": len, "str": str, "int": int, "float": float,
                    "dict": dict, "list": list, "print": print,
                    "range": range, "__import__": __import__,
                }
            }
            loc = {"record": new_rec}
            exec(py_script, safe_globals, loc)
            new_rec = loc["record"]
        except Exception as e:
            print("[py_script error]", e)

    return new_rec, new_rec.get("__name__", ""), type_override



def _set_name(rec: Dict[str, Any], v: Any):
    rec["__name__"] = str(v or "")


def _assign_target(rec: Dict[str, Any], t: str, val: Any, name_holder=None):
    if t == "name":
        if name_holder:
            name_holder(val)
        else:
            rec["__name__"] = str(val or "")
        return
    if t.startswith("data."):
        path = t[5:].split(".")
        cur = rec
        for seg in path[:-1]:
            cur = cur.setdefault(seg, {})
        cur[path[-1]] = val
        return
    rec[t] = val


# ==================== SQL 解析 & 入库工具 ====================
INSERT_RE = re.compile(
    r"insert\s+into\s+public\.\"?(?P<table>[\w\u4e00-\u9fa5]+)\"?\s*\((?P<cols>[^)]*)\)\s*values\s*\((?P<vals>[^)]*)\)\s*;",
    re.IGNORECASE
)

def _safe_read_sql(file: Path) -> str:
    for enc in ("utf-8", "gbk", "utf-8-sig"):
        try:
            return file.read_text(encoding=enc)
        except Exception:
            continue
    return file.read_text(encoding="utf-8", errors="ignore")

def _parse_values(raw: str) -> List[Any]:
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
            elif ch == ",": out.append("".join(buf).strip()); buf = []; i += 1
            else: buf.append(ch); i += 1
    out.append("".join(buf).strip())
    def _norm(v):
        v = v.strip()
        if v.upper() == "NULL": return ""
        if v.startswith("'") and v.endswith("'"):
            return v[1:-1].replace("''","'")
        return v
    return [_norm(v) for v in out]

def _parse_sql_file(sql_path: Path) -> List[Dict[str, Any]]:
    """返回所有 INSERT 记录组成的 dict 列表"""
    txt = _safe_read_sql(sql_path)
    entities = []
    for m in INSERT_RE.finditer(txt):
        cols = [c.strip().strip('"') for c in m.group("cols").split(",")]
        vals = _parse_values(m.group("vals"))
        if len(cols) != len(vals):
            continue
        entities.append(dict(zip(cols, vals)))
    return entities

def _ensure_entity_table(conn):
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

def _make_uuid10() -> str:
    n = int(time.time() * 1e6)
    base36 = "0123456789abcdefghijklmnopqrstuvwxyz"
    s = ""
    while n:
        n, r = divmod(n, 36)
        s = base36[r] + s
    return (s or "0")[-10:].rjust(10, "0")

def insert_entities(rows: List[Tuple[str,str,str,str,str,int,int,int]]):
    """rows: (uuid, sid, type, name, data_json, del, input_ts, update_ts)"""
    if not rows:
        return 0
    conn = pymysql.connect(**MYSQL_CFG)
    try:
        _ensure_entity_table(conn)
        sql = """
          INSERT INTO entity (`uuid`,`sid`,`type`,`name`,`data`,`del`,`input_date`,`update_date`)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        print("[insert_entities error]", e)
        return 0
    finally:
        conn.close()


# =============== 对外：状态 / 入库 / 删除 ===============
def check_entity_status(type_name: str) -> int:
    """返回该 type 在 entity 的记录数"""
    conn = pymysql.connect(**MYSQL_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM entity WHERE type=%s", (type_name,))
            n = cur.fetchone()[0]
            return int(n or 0)
    except Exception as e:
        print("[check_entity_status error]", e)
        return 0
    finally:
        conn.close()

def import_table_data(source_table: str, sid: str = None) -> int:
    """读取 source/sql/<table>.sql 的所有 INSERT，做映射后入库"""
    sid = sid or SID
    sql_path = detect_sql_path(source_table)
    if not sql_path.exists():
        print(f"[import_table_data] SQL not found: {sql_path}")
        return 0

    records = _parse_sql_file(sql_path)
    if not records:
        print(f"[import_table_data] No INSERT values in {sql_path.name}")
        return 0

    target_type_default = get_target_entity(source_table) or source_table
    rows_to_insert = []
    now_ts = int(time.time())

    for rec in records:
        mapped_data, out_name, type_override = apply_record_mapping(source_table, rec, py_script="")
        final_type = type_override or target_type_default
        data_json = json.dumps(mapped_data, ensure_ascii=False)
        name_val = out_name or str(mapped_data.get("__name__", "")) or ""
        rows_to_insert.append((
            _make_uuid10(), sid, final_type, name_val, data_json, 0, now_ts, now_ts
        ))

    return insert_entities(rows_to_insert)

def delete_table_data(type_name: str) -> int:
    """从 entity 物理删除该 type"""
    conn = pymysql.connect(**MYSQL_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM entity WHERE type=%s", (type_name,))
            n = int(cur.fetchone()[0] or 0)
            cur.execute("DELETE FROM entity WHERE type=%s", (type_name,))
        conn.commit()
        return n
    except Exception as e:
        conn.rollback()
        print("[delete_table_data error]", e)
        return 0
    finally:
        conn.close()


# ==================== 其它工具 ====================
def get_all_prioritized_tables() -> List[str]:
    rows = list_tables(include_disabled=False)
    return [r[0] for r in rows]

def get_table_priority(source_table: str) -> int:
    return get_priority(source_table)
