# app.py
# -*- coding: utf-8 -*-
import json
import re
from pathlib import Path
import streamlit as st

from backend.db import (
    init_db, list_tables, list_mapped_tables, save_table_mapping, soft_delete_table,
    restore_table, get_target_entity, get_priority,
    get_field_mappings, upsert_field_mapping, update_field_mapping, update_many_field_mappings,
    delete_field_mapping, get_table_script, save_table_script,
    export_all, import_all
)
from backend.source_fields import detect_source_fields, detect_sql_path
from backend.mapper_core import apply_record_mapping, check_entity_status, import_table_data, delete_table_data

try:
    from version3 import SID
except Exception:
    SID = "default_sid"

st.set_page_config(page_title="表映射管理工具", layout="wide")
init_db()


# ================= 工具函数 =================
def _ensure_all_fields_seeded(table_name: str):
    """
    仅在首次访问某表时执行一次字段初始化。
    - 已存在映射的字段不会被覆盖。
    - 避免页面刷新重复写入导致原配置丢失。
    """
    cache_key = f"seeded_{table_name}"

    # ✅ 如果本次运行中已经初始化过，直接返回
    if st.session_state.get(cache_key):
        return

    # ✅ 从数据库加载已有映射（防止覆盖）
    existing_mappings = get_field_mappings(table_name)
    existing_fields = {m["source_field"] for m in existing_mappings}

    # ✅ 检测源 SQL 的字段
    src_fields = detect_source_fields(table_name)

    # ✅ 仅对数据库中不存在的字段进行初始化
    for f in src_fields:
        if f not in existing_fields:
            upsert_field_mapping(table_name, f, f"data.{f}", "", 1, 0)

    # ✅ 标记为已初始化
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

    def _norm(v):
        v = v.strip()
        if v.upper() == "NULL": return ""
        if v.startswith("'") and v.endswith("'"):
            return v[1:-1].replace("''","'")
        return v

    vals = [_norm(v) for v in out]
    if len(cols) != len(vals): return None
    return dict(zip(cols, vals))


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
    st.title(f"表配置：{table_name}")
    _ensure_all_fields_seeded(table_name)

    # 缓存字段映射，避免重复插入
    cache_key = f"table_cache_{table_name}"
    if cache_key not in st.session_state:
        st.session_state[cache_key] = get_field_mappings(table_name)
    mappings = st.session_state[cache_key]

    # 表级配置
    col1, col2 = st.columns([3, 1])
    with col1:
        target_entity = st.text_input("默认目标 entity", value=get_target_entity(table_name))
    with col2:
        priority = st.number_input("优先级", value=get_priority(table_name), step=1)

    if st.button("保存表配置", use_container_width=True):
        save_table_mapping(table_name, target_entity, priority)
        st.success("表配置已保存")

    st.markdown("---")

    # 表级 Python 脚本
    st.subheader("表级 Python 脚本")
    st.caption("在字段映射后执行，可直接修改 record。")
    current_script = get_table_script(table_name) or ""
    py_script = st.text_area("自定义脚本", value=current_script, height=150)
    cols = st.columns([1, 1, 6])
    with cols[0]:
        if st.button("保存脚本"):
            save_table_script(table_name, py_script or "")
            st.success("脚本已保存")
    with cols[1]:
        if st.button("清空脚本"):
            save_table_script(table_name, "")
            st.success("脚本已清空"); st.rerun()

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
            st.text(sfield or "(自定义)")

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
                update_field_mapping(table_name, sfield, m["target_paths"], m["rule"])
                m.pop("__changed__", None)
                st.session_state[cache_key][idx] = m
                st.success(f"{sfield or '(自定义)'} 已保存")
                st.rerun()

        with cols[5]:
            if st.button("🗑", key=f"del_row_{table_name}_{idx}"):
                delete_field_mapping(table_name, sfield)
                st.session_state[cache_key] = [x for x in st.session_state[cache_key] if x["source_field"] != sfield]
                st.success(f"{sfield or '(自定义)'} 已删除")
                st.rerun()

        edited_data.append(m)

    st.markdown("---")
    if st.button("💾 一键保存全部修改", use_container_width=True):
        to_save = [m for m in edited_data if m.get("__changed__")]
        if to_save:
            update_many_field_mappings(table_name, to_save)
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
            upsert_field_mapping(table_name, src, tgt, rule_new)
            st.session_state.pop(cache_key, None)
            st.success("✅ 已添加新字段")
            st.rerun()

    st.markdown("---")

    # 模拟打印
    st.subheader("模拟打印")
    idx_key = f"sample_idx_{table_name}"
    if idx_key not in st.session_state:
        st.session_state[idx_key] = 0
    sample_index = st.session_state[idx_key]

    cols_pg = st.columns([1, 1, 6])
    with cols_pg[0]:
        if st.button("⬅️ 上一条"):
            if sample_index > 0:
                st.session_state[idx_key] -= 1; st.rerun()
    with cols_pg[1]:
        if st.button("下一条 ➡️"):
            st.session_state[idx_key] += 1; st.rerun()

    sample = _parse_nth_insert(table_name, sample_index) or {}
    with st.expander("SQL 样例记录", expanded=False):
        st.code(json.dumps(sample, ensure_ascii=False, indent=2))

    if st.button("生成模拟打印"):
        from backend.mapper_core import _extract_entity_meta
        py_now = get_table_script(table_name) or ""
        data_rec, out_name, type_override = apply_record_mapping(table_name, sample, py_now)

        # ⬇️ 抽 meta 并从 data_rec 中剔除
        meta = _extract_entity_meta(data_rec)

        preview = {
            "uuid": "(mock uuid)",
            "sid": SID,
            "type": type_override or table_name,
            "name": out_name or "",
            "del": int(meta["del"]),  # 顶层
            "input_date": int(meta["input_date"]),  # 顶层
            "update_date": int(meta["update_date"]),  # 顶层
            "data": data_rec  # 不再含 del/input_date/update_date
        }
        st.success("生成成功：")
        st.code(json.dumps(preview, ensure_ascii=False, indent=2))

    if st.button("返回列表"):
        st.session_state.page = "list"
        st.session_state.current_table = ""
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
        cols[1].markdown(f"[{src}](?page=detail&table={src})", unsafe_allow_html=True)
        cols[2].text(tgt)
        cols[3].text("✅" if count > 0 else "❌")
        cols[4].text(str(pri))

        with cols[5]:
            b1, b2 = st.columns([1,1])
            with b1:
                if st.button("入库", key=f"imp_{src}"):
                    n = import_table_data(src, sid=SID)
                    st.success(f"入库完成：写入 {n} 条")
                    st.rerun()
            with b2:
                if st.button("删除", key=f"del_{src}"):
                    n = delete_table_data(tgt)
                    st.success(f"删除完成：清理 {n} 条")
                    st.rerun()


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

    # 入口：映射结果管理
    if st.button("🧩 映射结果管理", type="secondary"):
        st.session_state.page = "mapped"
        st.rerun()

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
            st.markdown(f"[{src}](?page=detail&table={src})", unsafe_allow_html=True)
        with col[1]:
            st.text(tgt or "")
        with col[2]:
            st.text(str(pri))
        with col[3]:
            if dis:
                if st.button("恢复", key=f"res_{src}"):
                    restore_table(src); st.rerun()
            else:
                if st.button("停用", key=f"del_{src}"):
                    soft_delete_table(src); st.rerun()
        with col[4]:
            st.text("停用" if dis else "启用")


# ================= 入口 =================
def main():
    if "page" not in st.session_state:
        st.session_state.page = "list"
        st.session_state.current_table = ""

    q = st.query_params
    if "page" in q:
        st.session_state.page = q["page"]
    if "table" in q:
        st.session_state.current_table = q["table"]

    if st.session_state.page == "list":
        render_table_list()
    elif st.session_state.page == "mapped":
        render_mapped_tables()
    else:
        render_table_detail(st.session_state.current_table)


if __name__ == "__main__":
    main()
