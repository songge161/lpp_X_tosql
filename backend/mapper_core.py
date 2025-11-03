# backend/mapper_core.py
# -*- coding: utf-8 -*-
from typing import Dict, Any, Tuple, Optional, List
import re, pymysql, json

from backend.db import list_tables, get_priority, get_field_mappings, get_target_entity

try:
    from version3 import MYSQL_CFG
except Exception:
    MYSQL_CFG = dict(
        host="127.0.0.1", port=3307, user="im", password="root",
        database="im", charset="utf8mb4", autocommit=False
    )

_CACHE = {}

# ================= 基础查询 =================
def _entity_fetch(type_name: str, where_field: str, where_val: Any, target_path: str) -> Optional[Any]:
    """查询 entity 表"""
    key = f"E:{type_name}:{where_field}={where_val}:{target_path}"
    if key in _CACHE:
        return _CACHE[key]
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
    except Exception as e:
        print("[_entity_fetch error]", e)
    finally:
        if conn: conn.close()
    return None


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


# ================= 直接按 type+id 取值 =================
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


# ================= 任意表 fetch() =================
def _fetch_table_value(table: str, key: str, value: Any, field: str) -> Optional[Any]:
    """fetch(table='ct_city', key='id', value=record.city_id, field='name')"""
    key_cache = f"F:{table}:{key}={value}:{field}"
    if key_cache in _CACHE:
        return _CACHE[key_cache]
    conn = pymysql.connect(**MYSQL_CFG)
    try:
        with conn.cursor() as cur:
            sql = f"SELECT {field} FROM {table} WHERE {key}=%s LIMIT 1"
            cur.execute(sql, (value,))
            row = cur.fetchone()
            if row and row[0]:
                _CACHE[key_cache] = row[0]
                return row[0]
    except Exception as e:
        print("[_fetch_table_value error]", e)
    finally:
        conn.close()
    return None


# ================= Python 安全求值 =================
def _safe_eval(expr: str, ctx: dict):
    """执行安全 Python 表达式"""
    allowed_builtins = {"str": str, "int": int, "float": float, "len": len, "dict": dict, "list": list}
    try:
        return eval(expr, {"__builtins__": allowed_builtins}, ctx)
    except Exception as e:
        print("[_safe_eval error]", e)
        return None


# ================= rule 求值 =================
FUNC_COALESCE = re.compile(r"coalesce\((.+)\)$", re.I)
FUNC_CONCAT   = re.compile(r"concat\((.+)\)$", re.I)
FUNC_PY       = re.compile(r"py:(.+)", re.I)
FUNC_FETCH    = re.compile(r"fetch\((.+)\)", re.I)
FUNC_ENTITY_REL = re.compile(r"entity_rel\(([^,]+),([^,\)]+)(?:,([^,\)]+))?\)", re.I)
ENTITY_SIMPLE_RE = re.compile(r"entity\((?P<typ>\w+)(?:,by=(?P<by>\w+))?(?:,src=(?P<src>\w+))?\)\.(?P<path>[\w\.]+)")
REL_RE           = re.compile(r"rel\((?P<typ>\w+)(?:,by=(?P<by>\w+))?(?:,src=(?P<src>\w+))?\)$")
SOURCE_RE        = re.compile(r"source\((?P<table>\w+)\.(?P<field>\w+)=(?P<sexpr>[\w\.]+)\)\.(?P<target>\w+)")
PY_RE            = re.compile(r"py:(?P<expr>.+)", re.S)
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
        if v is not None:
            return v
    if a in record:
        return record.get(a)
    return a


def _eval_rule(rule: str, record: Dict[str, Any]) -> Any:
    import datetime
    import re
    r = (rule or "").strip()
    if not r:
        return None

    # coalesce(...)
    m = FUNC_COALESCE.match(r)
    if m:
        for x in _split_args(m.group(1)):
            v = _eval_atom(x, record)
            if v not in (None, "", "null", "NULL"):
                return v
        return ""

    # concat(...)
    m = FUNC_CONCAT.match(r)
    if m:
        return "".join(str(_eval_atom(x, record) or "") for x in _split_args(m.group(1)))

    # ✅ date(fmt, expr) 日期格式化函数
    # 示例: date(%Y-%m-%d, fund_record_time)
    if r.lower().startswith("date(") and r.endswith(")"):
        try:
            inner = r[5:-1].strip()
            if "," not in inner:
                return inner  # 参数不足
            fmt, val_expr = inner.split(",", 1)
            fmt = fmt.strip()
            val_expr = val_expr.strip()

            # --- 解析取值 ---
            v = None
            if val_expr.startswith("record."):
                v = record.get(val_expr[7:], "")
            elif val_expr in record:
                v = record.get(val_expr)
            else:
                try:
                    v = eval(val_expr, {}, {"record": record})
                except Exception:
                    v = val_expr

            if not v:
                return ""

            s = str(v).strip()
            # 兼容常见格式 "2024-09-27 13:33:23.391000"
            try:
                # 去掉毫秒
                if "." in s:
                    s = s.split(".")[0]
                # 自动补全缺省时间
                if len(s) == 10:
                    dt = datetime.datetime.strptime(s, "%Y-%m-%d")
                else:
                    dt = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            except Exception:
                try:
                    dt = datetime.datetime.fromisoformat(s)
                except Exception:
                    return s
            return dt.strftime(fmt)
        except Exception as e:
            print("[date() parse error]", e)
            return ""

    # entity 简单模式
    m = ENTITY_SIMPLE_RE.fullmatch(r)
    if m:
        typ, by, src, path = m.group("typ"), m.group("by") or "id", m.group("src"), m.group("path")
        src_val = record.get(src or f"{typ}_id") or record.get("id")
        if src_val is None: return None
        return _entity_fetch(typ, by, src_val, path)

    # entity join 模式
    m = ENTITY_JOIN_RE.fullmatch(r)
    if m:
        target_tbl, tfield, sexpr, target_path = m.group("target"), m.group("tfield"), m.group("sexpr"), m.group("path")
        src_val = _eval_atom(sexpr, record)
        if src_val is None: return None
        return _entity_fetch(target_tbl, tfield.replace("data.",""), src_val, target_path)

    # rel(...) 简写
    m = REL_RE.fullmatch(r)
    if m:
        typ, by, src = m.group("typ"), m.group("by") or "id", m.group("src")
        src_val = record.get(src or f"{typ}_id") or record.get("id")
        if src_val is None: return None
        return _entity_fetch(typ, by, src_val, "uuid")

    # source(...) 查询源 SQL 或缓存（留接口实现）
    m = SOURCE_RE.fullmatch(r)
    if m:
        table, field, sexpr, target = m.group("table"), m.group("field"), m.group("sexpr"), m.group("target")
        src_val = _eval_atom(sexpr, record)
        return f"[source:{table}.{field}={src_val}->{target}]"

    # ✅ py:{expr} 内嵌 Python（含 .get 多选增强）
    m = PY_RE.match(r)
    if m:
        import ast
        from types import SimpleNamespace

        expr = m.group("expr").strip()
        rec_obj = SimpleNamespace(**{k: v for k, v in record.items()})
        safe_globals = {"__builtins__": {}}
        safe_locals = {"record": rec_obj, **record}

        # --- 支持 py:{...}.get(...,...) ---
        if ".get(" in expr and expr.strip().startswith("{"):
            try:
                dict_part, _, tail = expr.partition("}.get(")
                dict_part = dict_part + "}"
                args_part = tail.rsplit(")", 1)[0]
                mapping = ast.literal_eval(dict_part)
                args = [a.strip() for a in args_part.split(",")]
                key_expr = args[0]
                default_val = ast.literal_eval(args[1]) if len(args) > 1 else ""

                def _eval_key(expr: str):
                    expr = expr.strip()
                    if expr.startswith("record."):
                        return record.get(expr[7:], "")
                    if expr in record:
                        return record[expr]
                    try:
                        return eval(expr, safe_globals, safe_locals)
                    except Exception:
                        return expr

                key_val = _eval_key(key_expr)
                # ✅ 支持 "1,2,5" → 拼接多个映射
                if isinstance(key_val, str) and "," in key_val:
                    parts = [mapping.get(p.strip(), default_val) for p in key_val.split(",")]
                    return ",".join([str(x) for x in parts if x])
                return mapping.get(str(key_val), default_val)
            except Exception as e:
                print("[py-get parse error]", e)
                return None

        # --- 常规 py:{...} 表达式 ---
        try:
            return eval(expr, safe_globals, safe_locals)
        except Exception as e:
            print("[py expr error]", e)
            return None

    # 默认 atom
    return _eval_atom(r, record)


# ================= 应用映射 =================
def apply_record_mapping(source_table: str, record: Dict[str, Any], py_script: str = "") -> Tuple[Dict[str, Any], str, str]:
    mappings = get_field_mappings(source_table)
    type_override = get_target_entity(source_table) or ""
    out_name = ""
    new_rec = dict(record)

    for m in mappings:
        if not int(m["enabled"]):
            continue

        targets = [t.strip() for t in (m["target_paths"] or "").split(",") if t.strip()]
        rule_raw = (m["rule"] or "").strip()
        src = m["source_field"] or ""

        # === 支持多 rule（以 || 分隔） ===
        rule_parts = [r.strip() for r in rule_raw.split("||")] if "||" in rule_raw else [rule_raw]
        vals = []

        # 分别计算 rule 值
        for r in rule_parts:
            v = _eval_rule(r, new_rec)
            if v is None:
                v = new_rec.get(src, "")
            vals.append(v)

        # 对齐 rule 与 target 数量
        if len(vals) == 1 and len(targets) > 1:
            vals = vals * len(targets)
        elif len(vals) < len(targets):
            vals += [vals[-1]] * (len(targets) - len(vals))

        # === 一一赋值 ===
        for i, t in enumerate(targets):
            val = vals[i]
            if t == "name":
                out_name = str(val or "")
                new_rec["__name__"] = out_name
            elif t.startswith("data."):
                path = t[5:].split(".")
                cur = new_rec
                for seg in path[:-1]:
                    cur = cur.setdefault(seg, {})
                cur[path[-1]] = val
            else:
                new_rec[t] = val

    # 确保 name 存在
    if "__name__" not in new_rec:
        new_rec["__name__"] = out_name or ""

    # === 表级脚本执行 ===
    if py_script.strip():
        try:
            safe_globals = {
                "__builtins__": {
                    "len": len,
                    "str": str,
                    "int": int,
                    "float": float,
                    "dict": dict,
                    "list": list,
                    "print": print,
                    "range": range,
                    "__import__": __import__,
                }
            }
            loc = {"record": new_rec}
            exec(py_script, safe_globals, loc)
            new_rec = loc["record"]
        except Exception as e:
            print("[py_script error]", e)

    return new_rec, out_name, type_override



def get_all_prioritized_tables() -> List[str]:
    rows = list_tables(include_disabled=False)
    return [r[0] for r in rows]


def get_table_priority(source_table: str) -> int:
    return get_priority(source_table)
