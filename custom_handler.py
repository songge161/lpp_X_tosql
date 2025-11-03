# -*- coding: utf-8 -*-
"""
custom_handler.py
---------------------
用户自定义表级处理逻辑。
每个函数名即表名（或通配处理）。
必须返回处理后的 dict。
支持：
✅ 精确字段修正（如时间格式）
✅ 跨表字段自动补全（uuid/name）
✅ 通用 fetch 工具（字段/uuid 级）
✅ 统一默认清洗逻辑
"""

import re
import pymysql
from typing import Dict, Any, Optional
from version3 import MYSQL_CFG  # 直接读取配置

# ========== 通用工具函数 ==========

# 🔹 简单缓存，避免重复查询
_CACHE = {}
# ---------- 启用列表 ----------
ENABLED_TABLES = [
    "ct_company_ipo",
    "ct_investor_fund_base",
    "ct_fund_firm_mid",
    "ct_fund_manage_firm",
    "ct_fund_base_info",
]


def _cache_key(table: str, key_field: str, key_value: Any, target_field: str) -> str:
    return f"{table}:{key_field}={key_value}:{target_field}"


def fetch_field(table: str, key_field: str, key_value: Any, target_field: str) -> Optional[Any]:
    """
    从 entity 表的 data JSON 中查找指定字段的值。
    示例：
        fetch_field("ct_fund_base_info", "id", 12, "fund_name")
        → "高毅价值精选1号"
    """
    cache_k = _cache_key(table, key_field, key_value, target_field)
    if cache_k in _CACHE:
        return _CACHE[cache_k]

    conn = None
    try:
        conn = pymysql.connect(**MYSQL_CFG)
        with conn.cursor() as cur:
            sql = f"""
                SELECT JSON_UNQUOTE(JSON_EXTRACT(data, '$.{target_field}'))
                FROM entity
                WHERE type=%s AND JSON_EXTRACT(data, CONCAT('$.', %s))=%s
                LIMIT 1
            """
            cur.execute(sql, (table, key_field, key_value))
            row = cur.fetchone()
            if row and row[0]:
                _CACHE[cache_k] = row[0]
                return row[0]
    except Exception as e:
        print(f"[fetch_field] 查询失败: {table}.{key_field}={key_value} -> {e}")
    finally:
        if conn:
            conn.close()
    return None


def fetch_field_uuid(table: str, key_field: str, key_value: Any) -> Optional[str]:
    """
    从 entity 表中查找给定 id 对应的 uuid。
    示例：
        fetch_field_uuid("ct_fund_invest", "id", 123)
        → "j5k0z8hw9p"
    """
    cache_k = _cache_key(table, key_field, key_value, "uuid")
    if cache_k in _CACHE:
        return _CACHE[cache_k]

    conn = None
    try:
        conn = pymysql.connect(**MYSQL_CFG)
        with conn.cursor() as cur:
            sql = """
                SELECT uuid
                FROM entity
                WHERE type=%s AND JSON_EXTRACT(data, CONCAT('$.', %s))=%s
                LIMIT 1
            """
            cur.execute(sql, (table, key_field, key_value))
            row = cur.fetchone()
            if row and row[0]:
                _CACHE[cache_k] = row[0]
                return row[0]
    except Exception as e:
        print(f"[fetch_field_uuid] 查询失败: {table}.{key_field}={key_value} -> {e}")
    finally:
        if conn:
            conn.close()
    return None


def resolve_relation(record: Dict[str, Any], prefix: str, ref_table: str, key_field: str, name_field: str):
    """
    通用外键解析辅助函数。
    自动补全：xxx_label / xxx / xxx_optgroup 三字段
    """
    key_val = record.get(key_field)
    if not key_val:
        return record

    name_val = fetch_field(ref_table, "id", key_val, name_field)
    uuid_val = fetch_field_uuid(ref_table, "id", key_val)

    record[f"{prefix}_label"] = name_val or ""
    record[f"{prefix}"] = uuid_val or ""
    record[f"{prefix}_optgroup"] = ""
    return record

# ========== 各表逻辑 ==========


def ct_company_ipo(record: Dict[str, Any]) -> Dict[str, Any]:
    """清洗 ct_company_ipo 表的时间字段格式"""
    v = record.get("company_time", "")
    if isinstance(v, str) and v:
        match = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", v)
        if match:
            record["company_time"] = match.group(1)
    return record


def ct_investor_fund_base(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    处理 ct_investor_fund_base 表：
    - 根据 investor_id / fund_id 查找名称与 uuid
    - 输出结构：
        investor_name_label / investor_name / investor_name_optgroup
        fund_name_label / fund_name / fund_name_optgroup
    """
    record = resolve_relation(record, "investor_name", "ct_fund_invest", "investor_id", "investor_name")
    record = resolve_relation(record, "fund_name", "ct_fund_base_info", "fund_id", "fund_name")
    return record

def ct_fund_firm_mid(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    处理 ct_fund_firm_mid（基金-机构中间表）：
    - fund_id -> 基金 uuid 写入 _rel（内部+外部）
    - firm_id -> 补充 firm_name_label / firm_name / firm_name_optgroup
    """
    fund_id = record.get("fund_id")
    firm_id = record.get("firm_id")
    # ---------- fund 关系 ----------
    fund_uuid = fetch_field_uuid("ct_fund_base_info", "id", fund_id)
    if fund_uuid:
        # ✅ 写入 data 内部（record）
        record["_rel"] = fund_uuid
    # ---------- firm 关系 ----------
    if firm_id:
        record = resolve_relation(
            record,
            "firm_name",          # 前缀
            "ct_fund_manage_firm",# 目标表
            "firm_id",            # 当前键
            "firm_name"           # 目标字段
        )
    return record
def ct_fund_manage_firm(record):
    record["__name__"] = record.get("firm_name", "")
def ct_fund_base_info(record):
    record["__name__"] = record.get("fund_name", "")
def default(record: Dict[str, Any], table: str) -> Dict[str, Any]:
    """默认逻辑（全表通用清洗）"""
    for k, v in record.items():
        if isinstance(v, str):
            record[k] = v.strip()
    return record