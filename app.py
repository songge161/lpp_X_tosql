# app.py
# -*- coding: utf-8 -*-
import json
import re
from pathlib import Path
import streamlit as st

# 顶部 import 部分
from backend.db import (
    init_db, list_tables, list_mapped_tables, save_table_mapping, soft_delete_table,
    restore_table, get_target_entity, get_priority,
    get_field_mappings, upsert_field_mapping, update_field_mapping, update_many_field_mappings,
    delete_field_mapping, get_table_script, save_table_script,
    export_all, import_all,
    rename_table_target_entity  # 新增：原子重命名
)
from backend.source_fields import detect_source_fields, detect_sql_path,detect_field_comments, detect_table_title
from backend.mapper_core import apply_record_mapping, check_entity_status, import_table_data, delete_table_data

try:
    from version3 import SID
except Exception:
    SID = "default_sid"

st.set_page_config(page_title="表映射管理工具", layout="wide")
init_db()


# ================= 工具函数 =================

# function _ensure_all_fields_seeded(table_name: str, target_entity: str)
def _ensure_all_fields_seeded(table_name: str, target_entity: str):
    """
    仅在首次访问某表-实体组合时执行一次字段初始化。
    - 按 (table_name, target_entity) 维度初始化
    - 已存在映射的字段不会被覆盖
    """
    cache_key = f"seeded_{table_name}_{target_entity or ''}"
    if st.session_state.get(cache_key):
        return

    # 按当前实体读取已存在的字段映射
    existing_mappings = get_field_mappings(table_name, target_entity or None)
    existing_fields = {m["source_field"] for m in existing_mappings}

    # 从源 SQL 检测字段
    src_fields = detect_source_fields(table_name)

    # 仅为该实体缺失的字段做 upsert，target_paths 默认 data.<同名>
    for f in src_fields:
        if f not in existing_fields:
            upsert_field_mapping(table_name, f, f"data.{f}", "", 1, 0, target_entity or "")

    st.session_state[cache_key] = True


def _parse_nth_insert(table_name: str, index: int = 0):
    p = detect_sql_path(table_name)
    if not p.exists():
        return None
    txt = p.read_text(encoding="utf-8", errors="ignore")
    inserts = list(re.finditer(
        r"insert\s+into\s+public\.\"?(?P<table>[\w\u4e00-\u9fa5]+)\"?"
        r"\s*\((?P<cols>[^)]*)\)\s*values\s*\((?P<vals>[^)]*)\)",
        txt, re.IGNORECASE
    ))
    if not inserts or index >= len(inserts):
        return None
    m = inserts[index]
    cols = [c.strip().strip('"') for c in m.group("cols").split(",")]
    raw = m.group("vals")
    out, buf, in_str, i = [], [], False, 0
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
    # 转换值并返回 dict
    def _convert(v: str):
        s = (v or "").strip()
        if s.lower() in ("null", "none"):
            return ""
        # 尝试数字
        try:
            if s.startswith("-") or s.isdigit():
                return int(s)
        except Exception:
            pass
        try:
            if "." in s:
                return float(s)
        except Exception:
            pass
        return s
    vals = [_convert(x) for x in out]
    if len(cols) != len(vals):
        return None
    return dict(zip(cols, vals))


def _parse_all_inserts(table_name: str):
    p = detect_sql_path(table_name)
    if not p.exists():
        return []
    txt = p.read_text(encoding="utf-8", errors="ignore")
    inserts = list(re.finditer(
        r"insert\s+into\s+public\.\"?(?P<table>[\w\u4e00-\u9fa5]+)\"?"
        r"\s*\((?P<cols>[^)]*)\)\s*values\s*\((?P<vals>[^)]*)\)",
        txt, re.IGNORECASE
    ))
    out_records = []
    for m in inserts:
        cols = [c.strip().strip('"') for c in m.group("cols").split(",")]
        raw = m.group("vals")
        # 复用解析逻辑
        buf, items, in_str, i = [], [], False, 0
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
                elif ch == ",": items.append("".join(buf).strip()); buf = []; i += 1
                else: buf.append(ch); i += 1
        items.append("".join(buf).strip())
        def _convert(v: str):
            s = (v or "").strip()
            if s.lower() in ("null", "none"):
                return ""
            try:
                if s.startswith("-") or s.isdigit():
                    return int(s)
            except Exception:
                pass
            try:
                if "." in s:
                    return float(s)
            except Exception:
                pass
            return s
        vals = [_convert(x) for x in items]
        if len(cols) == len(vals):
            out_records.append(dict(zip(cols, vals)))
    return out_records


def _guess_table_display_name(table_name: str) -> str:
    """从 DDL/注释猜测中文名称：匹配 -- 名称: xxx 或 /* name: xxx */，否则返回源表名"""
    p = detect_sql_path(table_name)
    if not p.exists():
        return table_name
    txt = p.read_text(encoding="utf-8", errors="ignore")
    m = re.search(r"--\s*(?:名称|name)\s*[:：]\s*([^\r\n]+)", txt, re.I)
    if m:
        return m.group(1).strip()
    m = re.search(r"/\*\s*(?:名称|name)\s*[:：]\s*([^\*]+)\*/", txt, re.I)
    if m:
        return m.group(1).strip()
    # 退化：看 create table 的注释行
    m = re.search(r"comment\s+on\s+table\s+[\w\.\"']+\s+is\s+'([^']+)'", txt, re.I)
    if m:
        return m.group(1).strip()
    return table_name


# ================= 详情页（原有） =================
def render_table_detail(table_name: str):
    comment_map = detect_field_comments(table_name)
    st.title(f"表配置：{table_name}")

    # 读取当前 entity（优先会话，其次 URL，再次表默认）
    current_entity = (
        st.session_state.get("current_entity")
        or st.query_params.get("entity", "")
        or get_target_entity(table_name)
    )
    st.session_state["current_entity"] = current_entity

    # ✅ 按当前实体做首次字段初始化（仅该实体缺失的字段）
    _ensure_all_fields_seeded(table_name, current_entity or "")

    # 缓存字段映射（按 table + entity 缓存）
    cache_key = f"table_cache_{table_name}_{current_entity or ''}"
    if cache_key not in st.session_state:
        st.session_state[cache_key] = get_field_mappings(table_name, current_entity or None)
    mappings = st.session_state[cache_key]

    # 表级配置（当前管理目标 + 该目标的优先级）
    col1, col2 = st.columns([3, 1])
    with col1:
        # 用当前 entity 作为默认，允许调整（保存时按当前 entity upsert）
        target_entity = st.text_input("当前管理目标 entity", value=current_entity)
    with col2:
        # 针对当前目标读取优先级
        priority = st.number_input("优先级", value=get_priority(table_name, target_entity), step=1)

    if st.button("保存表配置", use_container_width=True):
        old_entity = (current_entity or "").strip()
        new_entity = (target_entity or "").strip()

        if not new_entity:
            st.warning("目标 entity 不能为空。")
        elif not old_entity:
            # 详情页不允许创建新目标，请到『多映射管理中心』
            st.warning("当前表未绑定目标。请到『🧩 多映射管理中心』创建目标实体。")
        elif new_entity != old_entity:
            # 执行原子重命名：同时迁移 table_map 和 field_map
            try:
                rename_table_target_entity(table_name, old_entity, new_entity)
            except Exception as e:
                st.error(f"重命名失败：{e}")
            else:
                # 切换会话与缓存到新目标
                st.session_state["current_entity"] = new_entity
                st.session_state.pop(cache_key, None)
                new_cache_key = f"table_cache_{table_name}_{new_entity}"
                st.session_state[new_cache_key] = get_field_mappings(table_name, new_entity)

                # 同步更新 URL 的 query 参数，避免下一次被旧值覆盖
                try:
                    st.query_params["page"] = "detail"
                    st.query_params["table"] = table_name
                    st.query_params["entity"] = new_entity
                except Exception:
                    st.experimental_set_query_params(page="detail", table=table_name, entity=new_entity)

                st.success(f"已重命名：{old_entity} → {new_entity}")
                st.rerun()
        else:
            # 同名：仅保存优先级
            save_table_mapping(table_name, new_entity, priority)
            st.success("表配置已保存")

    st.caption(f"当前管理目标：{target_entity or '(未指定，使用表默认)'}")
    st.markdown("---")

    # 表级 Python 脚本
    st.subheader("表级 Python 脚本")
    st.caption("在字段映射后执行，可直接修改 record。")
    # 读取当前 entity 的脚本
    current_script = get_table_script(table_name, target_entity or st.session_state.get("current_entity") or "") or ""
    py_script = st.text_area("自定义脚本", value=current_script, height=150)
    cols = st.columns([1, 1, 6])
    with cols[0]:
        if st.button("保存脚本"):
            ok = save_table_script(table_name, py_script or "", target_entity=target_entity or st.session_state.get("current_entity") or "")
            if ok:
                st.success("脚本已保存（当前 entity）")
            else:
                st.warning("当前 entity 未创建映射，请到『🧩 多映射管理中心』创建目标实体")
    with cols[1]:
        if st.button("清空脚本"):
            ok = save_table_script(table_name, "", target_entity=target_entity or st.session_state.get("current_entity") or "")
            if ok:
                st.success("脚本已清空（当前 entity）"); st.rerun()
            else:
                st.warning("当前 entity 未创建映射，请到『🧩 多映射管理中心』创建目标实体")

    st.markdown("---")

    # 字段映射（压缩行 + 单行保存 + 一键保存）
    st.subheader("字段映射配置（压缩行显示）")
    st.caption("每条一行：修改后点💾保存；底部支持一键保存全部。")

    edited_data = []

    head = st.columns([2, 3, 4, 1, 1, 1])
    head[0].markdown("**字段**")
    head[1].markdown("**target_paths**")
    head[2].markdown("**rule**")
    head[3].markdown("**状态**")
    head[4].markdown("**保存**")
    head[5].markdown("**删除**")

    for idx, m in enumerate(mappings):
        sfield = m["source_field"]
        t_key = f"tp_{table_name}_{idx}"
        r_key = f"rule_{table_name}_{idx}"

        cols = st.columns([2, 3, 4, 1, 1, 1])
        with cols[0]:
            label = sfield or "(自定义)"
            note = comment_map.get(sfield, "")
            st.text(f"{label}{f'（{note}）' if note else ''}")

        new_tpath = cols[1].text_input(label="", value=m["target_paths"], key=t_key, placeholder="target_paths")
        new_rule  = cols[2].text_input(label="", value=m["rule"],         key=r_key, placeholder="rule")

        changed = (new_tpath != m["target_paths"]) or (new_rule != m["rule"])
        if changed:
            m["target_paths"] = new_tpath
            m["rule"] = new_rule
            m["__changed__"] = True

        with cols[3]:
            st.markdown("🟠" if m.get("__changed__") else "✅")

        with cols[4]:
            if st.button("💾", key=f"save_row_{table_name}_{idx}"):
                update_field_mapping(table_name, sfield, m["target_paths"], m["rule"], target_entity or "")
                m.pop("__changed__", None)
                st.session_state[cache_key][idx] = m
                st.success(f"{sfield or '(自定义)'} 已保存")
                st.rerun()

        with cols[5]:
            if st.button("🗑", key=f"del_row_{table_name}_{idx}"):
                delete_field_mapping(table_name, sfield, target_entity or "")
                st.session_state[cache_key] = [x for x in st.session_state[cache_key] if x["source_field"] != sfield]
                st.success(f"{sfield or '(自定义)'} 已删除")
                st.rerun()

        edited_data.append(m)

    st.markdown("---")
    if st.button("💾 一键保存全部修改", use_container_width=True):
        to_save = [m for m in edited_data if m.get("__changed__")]
        if to_save:
            update_many_field_mappings(table_name, to_save, target_entity or "")
            for m in to_save:
                m.pop("__changed__", None)
            st.session_state[cache_key] = edited_data
            st.success("✅ 所有修改已保存")
        else:
            st.info("没有需要保存的字段。")

    st.markdown("---")
    # 新增自定义映射
    st.subheader("新增自定义映射")
    with st.form(f"add_{table_name}"):
        src = st.text_input("source_field（可空）")
        tgt = st.text_input("target_paths（例：data.name）")
        rule_new = st.text_input("rule（可空）")
        if st.form_submit_button("添加"):
            # 查重：当前 (table + entity) 是否已有一条空 source_field 的映射
            src_norm = (src or "").strip()
            existing_list = st.session_state.get(cache_key) or get_field_mappings(table_name, target_entity or None)
            has_empty_custom = any((m.get("source_field") or "") == "" for m in existing_list)

            if src_norm == "" and has_empty_custom:
                st.warning("当前已存在一条 source_field 为空的自定义映射，请填写 source_field 或修改现有记录。")
            else:
                upsert_field_mapping(table_name, src_norm, tgt, rule_new, target_entity=target_entity or "")
                # 刷新当前 table+entity 的缓存，确保新映射立刻可见
                st.session_state[cache_key] = get_field_mappings(table_name, target_entity or None)
                st.success("已新增映射")
                st.rerun()

    st.markdown("---")

    # 模拟打印
    st.subheader("模拟打印")
    # 解析并缓存全部样例记录
    samples_key = f"samples_{table_name}"
    if samples_key not in st.session_state:
        st.session_state[samples_key] = _parse_all_inserts(table_name)
    full_list = st.session_state[samples_key]

    # 查找筛选区域
    st.caption("查找指定记录：填写字段名与值，支持非唯一匹配")
    sf1, sf2, sf3, sf4 = st.columns([2, 2, 1, 1])
    with sf1:
        q_field = st.text_input("字段名", key=f"q_field_{table_name}")
    with sf2:
        q_value = st.text_input("字段值", key=f"q_value_{table_name}")
    with sf3:
        q_contains = st.checkbox("包含匹配", value=True, key=f"q_contains_{table_name}")
    with sf4:
        do_query = st.button("查询", key=f"do_query_{table_name}")

    filter_key = f"filter_{table_name}"
    idx_key = f"sample_idx_{table_name}"

    if do_query:
        fld = (q_field or "").strip()
        val = (q_value or "").strip()
        if fld and val:
            def _match(rec):
                rv = rec.get(fld)
                if rv is None:
                    return False
                s = str(rv)
                return (val in s) if q_contains else (s == val)
            st.session_state[filter_key] = [r for r in full_list if _match(r)]
            st.session_state[idx_key] = 0
            st.info(f"筛选到 {len(st.session_state[filter_key])} 条记录（总 {len(full_list)} 条）")
        else:
            st.warning("请填写字段名与字段值后再查询。")

    # 清除筛选
    if st.button("清除筛选", key=f"clear_query_{table_name}"):
        st.session_state.pop(filter_key, None)
        st.session_state[idx_key] = 0

    idx_key = f"sample_idx_{table_name}"
    if idx_key not in st.session_state:
        st.session_state[idx_key] = 0
    sample_index = st.session_state[idx_key]

    # 当前列表：优先过滤结果
    curr_list = st.session_state.get(filter_key) or full_list
    total_n = len(curr_list)
    st.caption(f"当前预览索引：{sample_index + 1}/{max(total_n, 1)}（总 {len(full_list)} 条）")

    cols_pg = st.columns([1, 1, 6])
    with cols_pg[0]:
        if st.button("⬅️ 上一条"):
            if sample_index > 0:
                st.session_state[idx_key] -= 1; st.rerun()
    with cols_pg[1]:
        if st.button("下一条 ➡️"):
            if sample_index + 1 < total_n:
                st.session_state[idx_key] += 1; st.rerun()

    # 取当前样例
    sample = curr_list[sample_index] if (0 <= sample_index < total_n) else {}
    with st.expander("SQL 样例记录", expanded=False):
        st.code(json.dumps(sample, ensure_ascii=False, indent=2))

    if st.button("生成模拟打印"):
        from backend.mapper_core import _extract_entity_meta
        py_now = get_table_script(table_name, target_entity or st.session_state.get("current_entity") or "") or ""
        data_rec, out_name, type_override = apply_record_mapping(
            table_name, sample, py_now, target_entity=target_entity or st.session_state.get("current_entity") or ""
        )

        # ⬇️ 抽 meta 并从 data_rec 中剔除
        meta = _extract_entity_meta(data_rec)

        preview = {
            "uuid": "(mock uuid)",
            "sid": SID,
            "type": type_override or (target_entity or table_name),
            "name": out_name or "",
            "del": int(meta["del"]),
            "input_date": int(meta["input_date"]),
            "update_date": int(meta["update_date"]),
            "data": data_rec
        }
        st.success("生成成功：")
        st.code(json.dumps(preview, ensure_ascii=False, indent=2))

    # 字段专注模式
    st.subheader("字段专注模式")
    st.caption("填写字段名（用逗号分隔）。支持两种格式：name（外层），data.xxx（映射后的 data 内部字段，支持多级）。")
    focus_fields_key = f"focus_fields_{table_name}"
    focus_page_key = f"focus_page_{table_name}"
    focus_page_size_key = f"focus_page_size_{table_name}"

    ff_cols = st.columns([5, 1, 1, 1])
    with ff_cols[0]:
        fields_input = st.text_input("字段列表", value=st.session_state.get(focus_fields_key, "name"))
    with ff_cols[1]:
        page_size = st.number_input("每页数量", value=int(st.session_state.get(focus_page_size_key, 20)), min_value=5, max_value=200, step=5)
    with ff_cols[2]:
        gen_focus = st.button("生成")
    with ff_cols[3]:
        clear_focus = st.button("清空")

    # 解析字段列表
    def _parse_fields(s: str):
        return [x.strip() for x in (s or "").split(",") if x.strip()]

    if clear_focus:
        st.session_state.pop(focus_fields_key, None)
        st.session_state.pop(focus_page_key, None)
        st.session_state.pop(focus_page_size_key, None)

    if gen_focus:
        flds = _parse_fields(fields_input)
        if not flds:
            st.warning("请填写至少一个字段。")
        else:
            st.session_state[focus_fields_key] = fields_input
            st.session_state[focus_page_key] = 0
            st.session_state[focus_page_size_key] = int(page_size)

    # 若已有字段配置，按分页打印所有记录的字段值
    if focus_fields_key in st.session_state:
        flds = _parse_fields(st.session_state[focus_fields_key])
        page = int(st.session_state.get(focus_page_key, 0))
        size = int(st.session_state.get(focus_page_size_key, 20))

        # 当前列表：优先过滤结果
        curr_list = st.session_state.get(filter_key) or full_list
        total_n = len(curr_list)
        total_pages = max(1, (total_n + size - 1) // size)
        start = page * size
        end = min(start + size, total_n)

        # 顶部分页信息与跳转
        pg_cols = st.columns([1, 1, 4])
        with pg_cols[0]:
            if st.button("⬅️ 上一页", disabled=(page <= 0)):
                st.session_state[focus_page_key] = max(0, page - 1); st.rerun()
        with pg_cols[1]:
            if st.button("下一页 ➡️", disabled=(page + 1 >= total_pages)):
                st.session_state[focus_page_key] = min(total_pages - 1, page + 1); st.rerun()
        with pg_cols[2]:
            st.caption(f"当前页：{page + 1}/{total_pages}，范围 {start + 1}-{end}，总 {total_n} 条")

        # 计算当前页的映射并抽取字段
        py_now = get_table_script(table_name, target_entity or st.session_state.get("current_entity") or "") or ""
        rows = []
        def _get_data_path(d: dict, path: str):
            v = d
            for seg in [x for x in path.split(".") if x]:
                if isinstance(v, dict):
                    v = v.get(seg, "")
                else:
                    return ""
            return v if v is not None else ""

        for i, rec in enumerate(curr_list[start:end], start=start):
            data_rec, out_name, type_override = apply_record_mapping(
                table_name, rec, py_now, target_entity=target_entity or st.session_state.get("current_entity") or ""
            )
            name_val = (data_rec.get("__name__") or out_name or "")
            row = {"#": i + 1}
            for f in flds:
                if f == "name":
                    row[f] = name_val
                elif f.startswith("data."):
                    row[f] = _get_data_path(data_rec, f[5:])
                else:
                    # 未知格式，尝试直接取映射后的顶层字段
                    row[f] = data_rec.get(f, "")
            rows.append(row)

        st.dataframe(rows, use_container_width=True)

    if st.button("返回列表"):
        st.session_state.page = "list"
        st.session_state.current_table = ""
        st.session_state.current_entity = ""
        st.rerun()


# ================= 新增：映射结果管理页 =================
def render_mapped_tables():
    st.title("🧩 映射结果管理")

    rows = list_mapped_tables()
    if not rows:
        st.info("暂无已设置映射的表。请先在『源表列表』里为表设置 target_entity。")
        return

    # 顶部批量操作
    c1, c2, c3 = st.columns([1,1,6])
    with c1:
        if st.button("一键入库（全部）", type="primary"):
            total = 0
            for r in rows:
                total += import_table_data(r["source_table"], sid=SID)
            st.success(f"✅ 完成入库，总计写入 {total} 条。")
    with c2:
        if st.button("一键删除（全部）"):
            total_del = 0
            for r in rows:
                total_del += delete_table_data(r["target_entity"])
            st.success(f"🗑 已删除 {total_del} 条（按 type 汇总）。")

    st.markdown("---")

    # 表头
    head = st.columns([3, 3, 3, 1, 1, 2])
    head[0].markdown("**名称**")
    head[1].markdown("**源表**")
    head[2].markdown("**目标 type**")
    head[3].markdown("**状态**")
    head[4].markdown("**优先度**")
    head[5].markdown("**操作**")

    # 每行
    for r in rows:
        src = r["source_table"]
        tgt = r["target_entity"]
        pri = r["priority"]
        disp_name = _guess_table_display_name(src)
        count = check_entity_status(tgt)
        status = "✅ 已入库" if count > 0 else "❌ 未入库"

        cols = st.columns([3, 3, 3, 1, 1, 2])
        cols[0].text(disp_name)
        # 跳转时携带 entity 参数，直达该目标的详情页（新标签页打开）
        cols[1].markdown(
            f'<a href="?page=detail&table={src}&entity={tgt}" target="_blank">{src}</a>',
            unsafe_allow_html=True
        )
        cols[2].text(tgt)
        cols[3].text("✅" if count > 0 else "❌")
        cols[4].text(str(pri))

        with cols[5]:
            b1, b2 = st.columns([1,1])
            with b1:
                if st.button("入库", key=f"imp_{src}_{tgt}"):
                    # 显式传入本行的 target_entity，避免多映射时混淆
                    n = import_table_data(src, sid=SID, target_entity_spec=tgt)
                    st.success(f"入库完成：写入 {n} 条")
                    st.rerun()
            with b2:
                if st.button("删除", key=f"del_{src}_{tgt}"):
                    n = delete_table_data(tgt)
                    st.success(f"删除完成：清理 {n} 条")
                    st.rerun()

# ==========================================================
# 🧩 多映射管理页（支持单表多 target_entity）
# ==========================================================
import streamlit as st
from backend.db import list_tables, list_table_targets, upsert_field_mapping,delete_table_mapping
from backend.mapper_core import import_table_data, delete_table_data, check_entity_status
from version3 import SID

@st.cache_data(ttl=30)
def _cached_list_tables():
    return [r[0] for r in list_tables()]

def render_multi_mapping():
    st.title("🧩 多映射管理中心")

    rows = list_mapped_tables()
    if not rows:
        st.info("暂无已设置映射目标。")
    else:
        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("➕ 新建映射目标"):
                st.session_state["creating_map"] = True
        with c2:
            if st.button("🔄 刷新"):
                st.rerun()

        st.divider()

        for r in rows:
            src, tgt, desc = r["source_table"], r["target_entity"], r["description"]
            pri = r.get("priority", 0)
            cols = st.columns([3, 3, 3, 2])
            # 源表列改为可点击跳详情（携带 entity，新标签页打开）
            cols[0].markdown(
                f'🗂️ <a href="?page=detail&table={src}&entity={tgt}" target="_blank"><code>{src}</code></a>',
                unsafe_allow_html=True
            )
            cols[1].markdown(f"🎯 `{tgt}`")
            new_desc = cols[2].text_input("描述", value=desc or "", key=f"desc_{src}_{tgt}")
            with cols[3]:
                b1, b2 = st.columns([1,1])
                with b1:
                    if st.button("保存", key=f"save_{src}_{tgt}"):
                        save_table_mapping(src, tgt, pri, new_desc or "")
                        st.success("描述已更新")
                        st.rerun()
                with b2:
                    if st.button("❌ 删除", key=f"del_{src}_{tgt}"):
                        # 弹出确认层：携带表/实体/描述
                        st.session_state["confirm_del_show"] = True
                        st.session_state["confirm_del_src"] = src
                        st.session_state["confirm_del_tgt"] = tgt
                        st.session_state["confirm_del_desc"] = new_desc or ""

        # 删除确认弹层（全局唯一）
        if st.session_state.get("confirm_del_show"):
            st.warning(
                f"确认删除该映射？\n\n- 源表：{st.session_state.get('confirm_del_src','')}\n- 实体：{st.session_state.get('confirm_del_tgt','')}\n- 描述：{st.session_state.get('confirm_del_desc','')}\n\n删除后会同时清理该实体下的所有字段映射。"
            )
            cdel = st.columns([1,1,6])
            with cdel[0]:
                if st.button("确定删除", key="confirm_delete_go"):
                    delete_table_mapping(st.session_state.get("confirm_del_src",""), st.session_state.get("confirm_del_tgt",""))
                    st.success("已删除映射，并清理对应字段")
                    st.session_state["confirm_del_show"] = False
                    st.session_state.pop("confirm_del_src", None)
                    st.session_state.pop("confirm_del_tgt", None)
                    st.session_state.pop("confirm_del_desc", None)
                    st.rerun()
            with cdel[1]:
                if st.button("取消", key="confirm_delete_cancel"):
                    st.session_state["confirm_del_show"] = False
                    st.rerun()

            # ========== 创建新映射弹窗 ==========
        if st.session_state.get("creating_map"):
            st.subheader("➕ 新建映射目标")
            table_name = st.text_input("源表名")
            target_entity = st.text_input("目标实体名")
            desc = st.text_input("描述", "自动生成的映射")
            pri = st.number_input("优先级", value=0)
            if st.button("创建映射"):
                save_table_mapping(table_name, target_entity, pri, desc)
                # ✅ 新建后立即为该实体生成基础字段映射（不覆盖既有字段）
                _ensure_all_fields_seeded(table_name, target_entity or "")
                st.success("✅ 新映射已创建，并初始化基础字段映射")
                st.session_state["creating_map"] = False
                st.rerun()
        st.markdown("---")

# ================= 列表页（原有） =================
def render_table_list():
    st.title("源表列表")

    top = st.columns([1,1,6])
    with top[0]:
        if st.button("导出配置"):
            cfg = export_all()
            st.download_button(
                "下载 mapping_config.json",
                data=json.dumps(cfg, ensure_ascii=False, indent=2),
                file_name="mapping_config.json",
                mime="application/json",
                key="download_all_btn"
            )
    with top[1]:
        upf = st.file_uploader("导入配置", type=["json"])
        if upf:
            obj = json.loads(upf.read().decode("utf-8"))
            import_all(obj)
            st.success("导入完成"); st.rerun()

    st.markdown("---")

    # 入口：映射结果管理 / 多映射管理中心（按钮式链接，点击在新标签页打开）
    cols_nav = st.columns([2, 2, 6])
    btn_style = "display:inline-block;padding:.5rem 1rem;border-radius:.5rem;border:1px solid #d0d0d0;background:#f6f6f6;text-decoration:none;color:#222;"
    with cols_nav[0]:
        st.markdown(f'<a href="?page=mapped" target="_blank" style="{btn_style}">🧩 映射结果管理</a>', unsafe_allow_html=True)
    with cols_nav[1]:
        st.markdown(f'<a href="?page=multi_mapping" target="_blank" style="{btn_style}">🧩 多映射管理中心</a>', unsafe_allow_html=True)

    st.markdown("---")

    # 搜索 & 回收站
    col_s = st.columns([3, 2])
    with col_s[0]:
        search = st.text_input("搜索")
    with col_s[1]:
        show_disabled = st.checkbox("显示停用表", value=False)

    rows = list_tables(include_disabled=show_disabled)
    if search:
        rows = [r for r in rows if search.lower() in r[0].lower()]

    st.markdown("**源表 | 目标entity | 优先级 | 操作 | 状态**")
    for src, tgt, pri, dis, desc in rows:
        col = st.columns([3, 3, 1, 1, 2])
        with col[0]:
            link = f"?page=detail&table={src}" + (f"&entity={tgt}" if (tgt or "").strip() else "")
            st.markdown(f"[{src}]({link})", unsafe_allow_html=True)
        with col[1]:
            st.text(tgt or "")
        with col[2]:
            st.text(str(pri))
        with col[3]:
            if dis:
                if st.button("恢复", key=f"res_{src}_{tgt}"):
                    restore_table(src); st.rerun()
            else:
                if st.button("停用", key=f"del_{src}_{tgt}"):
                    soft_delete_table(src); st.rerun()
        with col[4]:
            st.text("停用" if dis else "启用")


# ================= 入口 =================
def main():
    if "page" not in st.session_state:
        st.session_state.page = "list"
        st.session_state.current_table = ""
        st.session_state.current_entity = ""

    q = st.query_params
    if "page" in q:
        st.session_state.page = q["page"]
    if "table" in q:
        st.session_state.current_table = q["table"]
    if "entity" in q:
        st.session_state.current_entity = q["entity"]

    if st.session_state.page == "list":
        render_table_list()
    elif st.session_state.page == "multi_mapping":
        render_multi_mapping()
    elif st.session_state.page == "mapped":
        render_mapped_tables()
    else:
        render_table_detail(st.session_state.current_table)


if __name__ == "__main__":
    main()
