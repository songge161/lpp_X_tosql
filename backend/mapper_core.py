# backend/mapper_core.py
# -*- coding: utf-8 -*-
from typing import Dict, Any, Tuple, Optional, List, Callable
import re, pymysql, json, time,math, datetime
from pathlib import Path
from types import SimpleNamespace
from backend.db import list_tables, get_priority, get_field_mappings, get_target_entity
from backend.source_fields import detect_sql_path
from backend.sql_utils import get_conn, is_pg, json_equals_clause

try:
    from version3 import MYSQL_CFG, SID
except Exception:
    MYSQL_CFG = dict(
        host="127.0.0.1", port=3307, user="im", password="root",
        database="im", charset="utf8mb4", autocommit=False
    )
    SID = "default_sid"

class SafeRecord(SimpleNamespace):
    def get(self, key, default=None):
        return getattr(self, key, default)

_CACHE: Dict[str, Any] = {}
# 专用于 SQL 源文件解析的缓存（不随每条映射清空）
_SQL_ROWS_CACHE: Dict[str, List[Dict[str, Any]]] = {}
_SQL_IDX_CACHE: Dict[str, Dict[str, Dict[str, Any]]] = {}

COMPLEX_EXPR_RE = re.compile(
    r"(?P<etype>(entity|sql))\.(?P<table>\w+)\(\s*(?P<cond>.+?)\s*\)\.(?P<target>[\w\.]+)",
    re.S
)

def _eval_complex_expr(expr: str, record: Dict[str, Any]) -> Optional[Any]:
    expr = re.sub(r"\s+", " ", expr.strip())
    m = COMPLEX_EXPR_RE.fullmatch(expr)
    if not m:
        return None

    etype, table, cond, target = (
        m.group("etype"), m.group("table"), m.group("cond"), m.group("target")
    )

    if "=" not in cond:
        return None
    left, right = [x.strip() for x in cond.split("=", 1)]

    # 递归右值
    if right.startswith(("entity.", "sql.")):
        right_val = _eval_complex_expr(right, record)
    else:
        right_val = _eval_rule(right, record)

    if right_val in (None, "", "null", "NULL"):
        return None

    where_field = left.split(".")[-1].replace("data.", "")
    # 根据前缀分流：entity -> 查询实体表；sql -> 查源 SQL 文件
    if etype == "sql":
        return _sql_lookup(table, where_field, right_val, target)
    return _entity_fetch(table, where_field, right_val, target)
# ==================== 🔧 SQL 直接查询补丁 ====================

SQL_COMPLEX_RE = re.compile(
    r"sql\.(?P<table>\w+)\(\s*sql\.(?P<table2>\w+)\.(?P<where_field>[\w\.]+)\s*=\s*(?P<sexpr>[^)]+)\s*\)\.(?P<target>[\w\.]+)",
    re.S
)

def _eval_sql_complex_expr(expr: str, record: Dict[str, Any]) -> Optional[Any]:
    """
    支持类似：
        sql.import_fund_info(sql.import_fund_info.fund_id=record.id).department
    的结构，直接从源 SQL 文件中查字段。
    """
    m = SQL_COMPLEX_RE.fullmatch(expr.strip())
    if not m:
        return None

    table = m.group("table")
    where_field = m.group("where_field").replace("data.", "")
    src_expr = m.group("sexpr").strip()
    target_field = m.group("target")

    # 计算右值
    v = None
    if src_expr.startswith("record."):
        v = record.get(src_expr[7:], "")
    elif src_expr in record:
        v = record.get(src_expr, "")
    else:
        try:
            from types import SimpleNamespace
            rec_obj = SafeRecord(**{k: v for k, v in record.items()})
            v = eval(src_expr, {"__builtins__": {}}, {"record": rec_obj, **record})
        except Exception:
            v = src_expr
    if not v:
        return ""

    # 直接查 SQL
    try:
        return _sql_lookup(table, where_field, v, target_field) or ""
    except Exception as e:
        print("[_eval_sql_complex_expr error]", e)
        return ""

def __date_ts__(v) -> int:
    """
    通用时间转秒级时间戳：
      - 支持字符串（含毫秒）: "2025-08-13 12:49:06.688000"
      - 支持纯日期: "2025-08-13"
      - 支持数字(秒/毫秒): 1699911111000 或 1699911111
      - 为空返回当前时间戳
    """
    import datetime, time
    if not v:
        return int(time.time())
    if isinstance(v, (int, float)):
        # 毫秒 -> 秒
        return int(v // 1000 if v > 1e12 else v)
    if isinstance(v, str):
        s = v.strip()
        # 尝试多种时间格式
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return int(datetime.datetime.strptime(s.split(".")[0], fmt).timestamp())
            except Exception:
                continue
        # 纯数字字符串
        if s.isdigit():
            return int(int(s) // 1000 if int(s) > 1e12 else int(s))
    return int(time.time())
# ================= 安全 Entity Fetch =================
def _entity_fetch(type_name: str, where_field: str, where_val: Any, target_path: str) -> Optional[Any]:
    """安全的 entity 查询：未命中返回 None，不复用旧缓存。
    兼容 MySQL 与 PostgreSQL。
    """
    key = f"E:{type_name}:{where_field}={where_val}:{target_path}"
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if target_path == "uuid":
                sql = f"SELECT uuid FROM entity WHERE type=%s AND {json_equals_clause('data', where_field)} LIMIT 1"
                cur.execute(sql, (type_name, str(where_val)))
            else:
                # 目标路径统一从 data JSON 里取文本
                tpath = target_path.replace("data.", "")
                if is_pg():
                    sql = f"SELECT data->>'{tpath}' FROM entity WHERE type=%s AND {json_equals_clause('data', where_field)} LIMIT 1"
                    cur.execute(sql, (type_name, str(where_val)))
                else:
                    sql = f"SELECT JSON_UNQUOTE(JSON_EXTRACT(data, '$.{tpath}')) FROM entity WHERE type=%s AND {json_equals_clause('data', where_field)} LIMIT 1"
                    cur.execute(sql, (type_name, str(where_val)))
            row = cur.fetchone()
            if row and row[0]:
                _CACHE[key] = row[0]
                return row[0]
            else:
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
    r"entity\("
    r"(?P<target>\w+):"
    r"(?P<tfield>[\w\.]+)="
    r"(?P<sexpr>"                  # 右值支持两类：
    r"entity\([^)]*\)\.[\w\.]+"    #   a) entity(...).path
    r"|[\w\.]+"                    #   b) record 字段或 a.b.c 链
    r")\)"
    r"\.(?P<path>[\w\.]+)"
)

def _eval_entity_join(expr: str, record: Dict[str, Any]) -> Optional[Any]:
    m = ENTITY_JOIN_RE.fullmatch(expr.strip())
    if not m:
        return None

    target_tbl  = m.group("target")
    target_field = m.group("tfield")            # 例如 data.id
    source_expr = m.group("sexpr")              # 可能是 "record.xxx" / "id" / "entity(...).yyy"
    target_path = m.group("path")               # 例如 uuid / data.xxx

    # 1) 右值如果是内层 entity(...) 表达式，先递归求值
    if source_expr.strip().startswith("entity("):
        # 直接复用 rule 求值引擎，拿到实际右值
        inner_val = _eval_rule(source_expr, record)
        if inner_val in (None, "", "null", "NULL"):
            return None
        return _entity_fetch(
            target_tbl,
            target_field.replace("data.", ""),
            inner_val,
            target_path
        )

    # 2) 否则按原先逻辑：从 record 里取右值（支持 record.xxx 或 a.b.c 链）
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

    return _entity_fetch(
        target_tbl,
        target_field.replace("data.", ""),
        v,
        target_path
    )


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

# ================= 源 SQL 查找（用于 sql.xxx(...)） =================
def _sql_lookup(table: str, where_field: str, where_val: Any, target_field: str) -> Optional[Any]:
    """在 source/sql/<table>.sql 中按列匹配并返回目标列值。
    - 解析 INSERT 语句得到行列表；简单扫描匹配 where_field==where_val；返回 target_field
    - 若文件不存在或未命中，返回 None
    - 使用进程级缓存避免重复解析
    """
    try:
        wf = str(where_field or "").replace("data.", "").strip()
        tf = str(target_field or "").strip()
        tv = str(where_val or "").strip()

        # 取 rows 缓存（不受 _CACHE.clear() 影响）
        rows = _SQL_ROWS_CACHE.get(table)
        if rows is None:
            p = detect_sql_path(table)
            if not p.exists():
                return None
            rows = _parse_sql_file(p)
            _SQL_ROWS_CACHE[table] = rows

        # 若该字段未建立索引，构建一次基于小写、去空格的索引
        idx_key = f"{table}|{wf}"
        idx = _SQL_IDX_CACHE.get(idx_key)
        if idx is None:
            idx = {}
            for r in rows:
                rv = str(r.get(wf, "")).strip().lower()
                if rv:
                    # 若存在重复键，保留第一条；如需最新策略可另加逻辑
                    idx.setdefault(rv, r)
            _SQL_IDX_CACHE[idx_key] = idx

        hit = idx.get(tv.strip().lower())
        if not hit:
            return None
        val = hit.get(tf, None)
        if val in (None, "", "NULL", "null"):
            return None
        return val
    except Exception as e:
        print("[_sql_lookup error]", e)
        return None

# ========== 对外：SQL 缓存管理 ==========
def clear_sql_cache(table: Optional[str] = None) -> Dict[str, int]:
    """清理 SQL 解析与索引缓存。table 为空则清理全部。返回统计信息。"""
    cleared_rows = 0
    cleared_idx = 0
    if table:
        if table in _SQL_ROWS_CACHE:
            del _SQL_ROWS_CACHE[table]
            cleared_rows = 1
        # 删除该表的所有字段索引
        keys = [k for k in _SQL_IDX_CACHE.keys() if k.startswith(f"{table}|")]
        for k in keys:
            del _SQL_IDX_CACHE[k]
            cleared_idx += 1
    else:
        cleared_rows = len(_SQL_ROWS_CACHE)
        cleared_idx = len(_SQL_IDX_CACHE)
        _SQL_ROWS_CACHE.clear()
        _SQL_IDX_CACHE.clear()
    return {"rows": cleared_rows, "idx": cleared_idx}

def warm_sql_cache(tables: List[str]) -> Dict[str, int]:
    """预热指定表的 SQL 解析与索引缓存。返回成功预热的表数量和索引数量。"""
    warmed_rows = 0
    warmed_idx = 0
    for tbl in tables or []:
        try:
            p = detect_sql_path(tbl)
            if not p.exists():
                continue
            rows = _SQL_ROWS_CACHE.get(tbl)
            if rows is None:
                rows = _parse_sql_file(p)
                _SQL_ROWS_CACHE[tbl] = rows
                warmed_rows += 1
            # 为每个字段建立一次索引（按需可限制字段集合）
            if rows:
                # 按首条记录的键集合建立索引，避免全表所有列带来过多索引
                sample_keys = list(rows[0].keys())
                for wf in sample_keys:
                    idx_key = f"{tbl}|{wf}"
                    if idx_key in _SQL_IDX_CACHE:
                        continue
                    idx = {}
                    for r in rows:
                        rv = str(r.get(wf, "")).strip().lower()
                        if rv:
                            idx.setdefault(rv, r)
                    _SQL_IDX_CACHE[idx_key] = idx
                    warmed_idx += 1
        except Exception:
            continue
    return {"rows": warmed_rows, "idx": warmed_idx}


# ================= Python 安全求值（py:{...}） =================
FUNC_COALESCE = re.compile(r"coalesce\((.+)\)$", re.I)
FUNC_CONCAT   = re.compile(r"concat\((.+)\)$", re.I)
PY_RE         = re.compile(r"py:(?P<expr>.+)", re.S)
ENTITY_SIMPLE_RE = re.compile(r"entity\((?P<typ>\w+)(?:,by=(?P<by>\w+))?(?:,src=(?P<src>\w+))?\)\.(?P<path>[\w\.]+)")
REL_RE           = re.compile(r"rel\((?P<typ>\w+)(?:,by=(?P<by>\w+))?(?:,src=(?P<src>\w+))?\)$")
SOURCE_RE        = re.compile(r"source\((?P<table>\w+)\.(?P<field>\w+)=(?P<sexpr>[\w\.]+)\)\.(?P<target>\w+)")


def _split_args(s: str) -> List[str]:
    return [a.strip() for a in s.split(",")]
# ========== 时间戳辅助 ==========



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
    # --- 优先检测 entity/sql 嵌套表达式 ---
    m = COMPLEX_EXPR_RE.fullmatch(r)
    if m:
        return _eval_complex_expr(r, record)
    # ✅ 支持 sql.xxx(...) 顶层结构
    m = SQL_COMPLEX_RE.fullmatch(r)
    if m:
        return _eval_sql_complex_expr(r, record)
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
        rec_obj = SafeRecord(**{k: v for k, v in record.items()})
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
                "__builtins__": {
                    "str": str, "int": int, "float": float, "len": len, "round": round,
                    "dict": dict, "list": list, "__date_ts__": __date_ts__,
                },
                "re": re,
                "json": json,
                "__date_ts__": __date_ts__
            }
            val = eval(expr, safe_globals, {"record": rec_obj, **record})
            return val
        except Exception as e:
            print("[py expr error]", e)
            return None

    # ========== 默认 ==========
    return _eval_atom(r, record)



# ================= 应用映射 =================
def apply_record_mapping(source_table: str, record: Dict[str, Any], py_script: str = "", target_entity: Optional[str] = None) -> Tuple[Dict[str, Any], str, str]:
    # 每次映射前清空 Entity 缓存，避免脏缓存
    _CACHE.clear()

    # 按当前 entity 过滤字段映射；未指定则使用表默认
    mappings = get_field_mappings(source_table, target_entity or None)
    type_override = (target_entity or get_target_entity(source_table) or "")
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
            # 注入当前 entity 上下文，脚本可读 current_entity / type_name
            loc = {
                "record": new_rec,
                "current_entity": (target_entity or ""),
                "target_entity": (target_entity or ""),
                "type_name": (target_entity or get_target_entity(source_table) or source_table)
            }
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
    with conn.cursor() as cur:
        if is_pg():
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS entity (
                    id BIGSERIAL PRIMARY KEY,
                    uuid VARCHAR(64) NOT NULL,
                    sid VARCHAR(64) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    type VARCHAR(128) NOT NULL,
                    data JSONB NOT NULL,
                    del SMALLINT DEFAULT 0,
                    input_date BIGINT DEFAULT 0,
                    update_date BIGINT DEFAULT 0
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_entity_type ON entity(type);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_entity_sid ON entity(sid);")
        else:
            cur.execute(
                """
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
            )

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
# ========== NEW: 解析 type(key)[excludes] ==========
def _parse_type_and_key(type_spec: str) -> Tuple[str, str, List[str]]:
    """
    支持如下形式：
      - 'fund'                     -> (type='fund', key='id', excludes=[])
      - 'fund(usci)'               -> (type='fund', key='usci', excludes=[])
      - 'fund(usci)[data.id,name]' -> (type='fund', key='usci', excludes=['data.id','name'])
    [] 中的排除项以逗号分隔；'name' 表示排除顶层实体 name；'data.X' 表示排除 data JSON 中的字段（支持 a.b.c 路径）。
    """
    s = (type_spec or "").strip()
    m = re.match(r"^(?P<typ>\w+)(?:\((?P<key>[\w\.]+)\))?(?:\[(?P<excl>[^\]]+)\])?$", s)
    if not m:
        return s, "id", []
    t = (m.group("typ") or "").strip()
    k = (m.group("key") or "id").strip()
    excl_raw = (m.group("excl") or "").strip()
    excludes: List[str] = []
    if excl_raw:
        excludes = [x.strip() for x in excl_raw.split(",") if x.strip()]
    return t, k, excludes

def _del_by_path(obj: Dict[str, Any], path: str):
    """从字典 obj 中删除路径 path（例如 'a.b.c'），若不存在则忽略。"""
    if not path:
        return
    parts = path.split(".")
    cur = obj
    for p in parts[:-1]:
        if not isinstance(cur, dict):
            return
        if p not in cur:
            return
        cur = cur[p]
    if isinstance(cur, dict) and parts[-1] in cur:
        try:
            del cur[parts[-1]]
        except Exception:
            pass


# ========== NEW: 抽取顶层 meta（并从 data JSON 中剔除这些 meta）==========
def _extract_entity_meta(mapped: Dict[str, Any], now_ts: int) -> Dict[str, int]:
    """
    将 del / input_date / update_date 作为 entity 顶层列使用；
    若不存在，使用默认值；从 mapped 中移除这些键，避免进入 data JSON。
    """
    meta = {
        "del": int(mapped.pop("del", 0) or 0),
        "input_date": int(mapped.pop("input_date", now_ts) or now_ts),
        "update_date": int(mapped.pop("update_date", now_ts) or now_ts),
    }
    return meta


# ========== NEW: 单条 UPSERT 到 entity ==========
def _upsert_entity_row(type_name: str, key_field: str, key_value: Any,
                       sid: str, name_val: str, data_json: str,
                       meta: Dict[str, int], import_mode: str = "upsert") -> int:
    """
    使用 (type_name, JSON_EXTRACT(data, $.<key_field>)=key_value) 实现 UPSERT。
    命中则 UPDATE：name、data、update_date；未命中则 INSERT。
    返回 1 表示成功写入（无论插入还是更新）。
    """
    if key_value in (None, ""):
        # 没有唯一键值，不写
        return 0

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # 查询是否已存在该唯一键
            sel_sql = f"""
                SELECT uuid, name, data FROM entity
                WHERE type=%s AND {json_equals_clause('data', key_field)}
                LIMIT 1
            """
            cur.execute(sel_sql, (type_name, str(key_value)))
            row = cur.fetchone()

            if row:  # 命中
                uuid, old_name, old_data_json = row[0], row[1], row[2]
                # 合并策略：当 import_mode 为 upsert 或 update_only 时也使用合并，避免覆盖丢字段
                # 旧数据解析失败则回退为替换
                merged_json = data_json
                try:
                    old_data = json.loads(old_data_json or "{}")
                    new_data = json.loads(data_json or "{}")
                    def deep_merge(a, b):
                        for k, v in b.items():
                            if isinstance(v, dict) and isinstance(a.get(k), dict):
                                deep_merge(a[k], v)
                            else:
                                a[k] = v
                        return a
                    merged = deep_merge(old_data, new_data)
                    merged_json = json.dumps(merged, ensure_ascii=False)
                except Exception:
                    merged_json = data_json

                if import_mode in ("upsert", "update_only", "upsert_merge", "update_merge"):
                    # 若 name 为空字符串，则保留旧 name
                    final_name = old_name if (name_val is None or str(name_val) == "") else name_val
                    upd_sql = """
                        UPDATE entity
                        SET name=%s,
                            data=%s,
                            del=%s,
                            update_date=%s
                        WHERE uuid=%s
                    """
                    cur.execute(
                        upd_sql,
                        (final_name, merged_json, int(meta["del"]), int(meta["update_date"]), uuid)
                    )
                else:
                    # create_only 命中则不写
                    return 0
            else:    # 未命中
                if import_mode in ("upsert", "create_only"):
                    ins_sql = """
                        INSERT INTO entity
                            (uuid,sid,type,name,data,del,input_date,update_date)
                        VALUES
                            (%s,%s,%s,%s,%s,%s,%s,%s)
                    """
                    cur.execute(
                        ins_sql,
                        (_make_uuid10(), sid, type_name, name_val, data_json,
                         int(meta["del"]), int(meta["input_date"]), int(meta["update_date"]))
                    )
                else:
                    # update_only 未命中则不写
                    return 0

        conn.commit()
        return 1
    except Exception as e:
        conn.rollback()
        print("[_upsert_entity_row error]", e)
        return 0
    finally:
        conn.close()

def upsert_entity(type_name: str, key_field: str, key_value: Any, name: str, data_json: str):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # 1) 查是否已存在同一 fund
            sql_sel = f"""
                SELECT uuid FROM entity
                WHERE type=%s AND {json_equals_clause('data', key_field)} LIMIT 1
            """
            cur.execute(sql_sel, (type_name, str(key_value)))
            row = cur.fetchone()

            now_ts = int(time.time())
            if row:  # 存在 → UPDATE
                uuid = row[0]
                sql_upd = """
                    UPDATE entity
                    SET name=%s, data=%s, update_date=%s
                    WHERE uuid=%s
                """
                cur.execute(sql_upd, (name, data_json, now_ts, uuid))
            else:   # 不存在 → INSERT
                uuid = _make_uuid10()
                sql_ins = """
                    INSERT INTO entity(uuid, sid, type, name, data, del, input_date, update_date)
                    VALUES (%s,%s,%s,%s,%s,0,%s,%s)
                """
                cur.execute(sql_ins, (uuid, SID, type_name, name, data_json, now_ts, now_ts))

        conn.commit()
        return 1
    except Exception as e:
        conn.rollback()
        print("[upsert_entity error]", e)
        return 0
    finally:
        conn.close()

# =============== 对外：状态 / 入库 / 删除 ===============
def check_entity_status(type_name: str, sid: Optional[str] = None) -> int:
    """返回该 type 在 entity 的记录数；若传入 sid 则按 sid 过滤"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if sid:
                cur.execute("SELECT COUNT(*) FROM entity WHERE type=%s AND sid=%s", (type_name, sid))
            else:
                cur.execute("SELECT COUNT(*) FROM entity WHERE type=%s", (type_name,))
            n = cur.fetchone()[0]
            return int(n or 0)
    except Exception as e:
        print("[check_entity_status error]", e)
        return 0
    finally:
        conn.close()

def import_table_data(source_table: str, sid: str = None, target_entity_spec: Optional[str] = None, import_mode: str = "upsert", progress_cb: Optional[Callable[[int, int], None]] = None) -> int:
    """
    读取 source/sql/<table>.sql 的所有 INSERT，映射后按 (type(key)) 规则 UPSERT 入库。
    - target_entity 支持 'fund' 或 'fund(id)'；后者表示统一主键是 data.id。
    - 若未配置 ()，默认 key_field='id'。
    - 确保该 key_field 写入到 data JSON（即 mapped_data[key_field] 存在）。
    - del/input_date/update_date 抽到 entity 顶层（不进 data JSON）。
    """
    sid = sid or SID
    sql_path = detect_sql_path(source_table)
    if not sql_path.exists():
        print(f"[import_table_data] SQL not found: {sql_path}")
        return 0

    records = _parse_sql_file(sql_path)
    if not records:
        print(f"[import_table_data] No INSERT values in {sql_path.name}")
        return 0

    # ⭐ 支持外部指定目标类型与排除（如 'fund'、'fund(id)'、'fund(usci)[data.id,name]'）
    target_type_spec = (target_entity_spec or get_target_entity(source_table) or source_table)
    final_type, key_field, excludes = _parse_type_and_key(target_type_spec)

    now_ts = int(time.time())
    wrote = 0
    total = len(records)
    # 进度回调节流：最多 ~100 次更新，且保证最后一次更新
    stride = 1 if total <= 100 else max(1, total // 100)
    last_cb_ts = 0.0

    for idx, rec in enumerate(records, start=1):
        # 1) 映射：按当前 final_type 过滤字段映射 & 脚本上下文
        mapped_data, out_name, type_override = apply_record_mapping(
            source_table, rec, py_script="", target_entity=final_type
        )
        type_here = (type_override or final_type).strip() or source_table

        # 2) 统一键值：优先 mapped_data，其次原始 rec
        key_val = mapped_data.get(key_field, None)
        if key_val in (None, ""):
            key_val = rec.get(key_field, "")

        # 3) 应用排除：删除指定的 data.* 字段；'name' 排除则不写顶层 name
        name_excluded = False
        for ex in excludes:
            if ex == "name":
                name_excluded = True
            elif ex.startswith("data."):
                p = ex[5:].strip()
                if p and p != key_field:
                    _del_by_path(mapped_data, p)

        # 4) 确保该统一键被写入 data JSON
        if key_field not in mapped_data:
            mapped_data[key_field] = key_val

        # 5) 元字段抽取（并从 data JSON 剔除）
        meta = _extract_entity_meta(mapped_data, now_ts=now_ts)

        # 6) name 取 mapped_data['__name__'] 回落 out_name；若排除 name 则置空
        name_val = (mapped_data.get("__name__") or out_name or "").strip()
        if name_excluded:
            name_val = ""
            if "__name__" in mapped_data:
                try:
                    del mapped_data["__name__"]
                except Exception:
                    pass

        # 7) 序列化 data（此时已不包含 del/input_date/update_date）
        data_json = json.dumps(mapped_data, ensure_ascii=False)

        # 8) UPSERT
        wrote += _upsert_entity_row(
            type_name=type_here,
            key_field=key_field,
            key_value=key_val,
            sid=sid,
            name_val=name_val,
            data_json=data_json,
            meta=meta,
            import_mode=import_mode
        )
        # 9) 进度回调（每处理一条记录）
        try:
            if progress_cb:
                now = time.time()
                if (idx == total) or (idx % stride == 0) or (now - last_cb_ts >= 0.05):
                    progress_cb(idx, total)
                    last_cb_ts = now
        except Exception:
            # 回调失败不影响主流程
            pass

    return wrote

def delete_table_data(type_name: str, sid: Optional[str] = None) -> int:
    """从 entity 物理删除该 type（按当前 sid 限制）"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # 若未显式传入，回退到全局 SID
            cur_sid = sid or SID
            cur.execute("SELECT COUNT(*) FROM entity WHERE type=%s AND sid=%s", (type_name, cur_sid))
            n = int(cur.fetchone()[0] or 0)
            cur.execute("DELETE FROM entity WHERE type=%s AND sid=%s", (type_name, cur_sid))
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
def _extract_entity_meta(mapped: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    """
    从映射完的 new_rec 中抽出需要放到 entity 顶层的元字段：
      - del               -> 顶层 del (int)
      - input_date        -> 顶层 input_date (bigint 秒/毫秒都可，你这里用秒)
      - update_date       -> 顶层 update_date (bigint)
    抽出后会从 mapped 中移除这些键，以保证 data 里不再包含它们。
    """
    meta = {}
    if "del" in mapped:
        try:
            meta["del"] = int(mapped.pop("del"))
        except Exception:
            meta["del"] = 0
    if "input_date" in mapped:
        try:
            meta["input_date"] = int(mapped.pop("input_date"))
        except Exception:
            meta["input_date"] = int(now_ts or time.time())
    if "update_date" in mapped:
        try:
            meta["update_date"] = int(mapped.pop("update_date"))
        except Exception:
            meta["update_date"] = int(now_ts or time.time())

    # 默认兜底
    if "del" not in meta:
        meta["del"] = 0
    if "input_date" not in meta:
        meta["input_date"] = int(now_ts or time.time())
    if "update_date" not in meta:
        meta["update_date"] = int(now_ts or time.time())
    return meta