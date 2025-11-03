# app.py
# -*- coding: utf-8 -*-
import json
import re
import streamlit as st

from backend.db import (
    init_db, list_tables, save_table_mapping, soft_delete_table,
    restore_table, get_target_entity, get_priority,
    get_field_mappings, upsert_field_mapping, update_field_mapping, update_many_field_mappings,
    delete_field_mapping, get_table_script, save_table_script,
    export_all, import_all
)
from backend.source_fields import detect_source_fields, detect_sql_path
from backend.mapper_core import apply_record_mapping

try:
    from version3 import SID
except Exception:
    SID = "default_sid"

# ================= 初始化 =================
st.set_page_config(page_title="表映射管理工具", layout="wide")
init_db()


# ================= 工具函数 =================
def _seed_fields_once(table_name: str):
    """
    仅当该表在 field_map 中“没有任何记录”时，按源 SQL 字段一次性生成默认映射：
      source_field=f, target_paths=data.f, rule=''
    之后再进入该表，不再自动覆盖或新增，避免用户改动被重置。
    """
    existing = get_field_mappings(table_name)
    if existing:   # 已有记录 -> 不再自动生成
        return

    src_fields = detect_source_fields(table_name)
    for f in src_fields:
        upsert_field_mapping(table_name, f, f"data.{f}", "", 1, 0)


def _get_sample_record_from_sql(table_name: str, index: int = 0):
    """解析 SQL 第 index 条 insert"""
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
            return v[1:-1].replace("''", "'")
        return v
    vals = [_norm(v) for v in out]
    if len(cols) != len(vals): return None
    return dict(zip(cols, vals))


# ================= 页面：详情 =================
def render_table_detail(table_name: str):
    st.title(f"表配置：{table_name}")

    # 仅第一次为空时生成默认字段
    _seed_fields_once(table_name)

    # === 载入 DB 映射 + 本地快照（用于改动标记） ===
    cache_key = f"map_rows_{table_name}"
    snap_key  = f"map_snap_{table_name}"  # {source_field: (target_paths, rule)}

    db_rows = get_field_mappings(table_name)

    # 如果没有本地缓存，初始化缓存与快照
    if cache_key not in st.session_state:
        st.session_state[cache_key] = db_rows
    if snap_key not in st.session_state:
        st.session_state[snap_key] = {r["source_field"]: (r["target_paths"], r["rule"]) for r in db_rows}

    mappings = st.session_state[cache_key]
    snapshot = st.session_state[snap_key]

    # ---- 表级配置 ----
    col1, col2 = st.columns([3, 1])
    with col1:
        target_entity = st.text_input("默认目标 entity", value=get_target_entity(table_name))
    with col2:
        priority = st.number_input("优先级", value=get_priority(table_name), step=1)

    if st.button("保存表配置", use_container_width=True):
        save_table_mapping(table_name, target_entity, priority)
        st.success("表配置已保存")

    st.divider()

    # ---- 表脚本 ----
    st.subheader("表级 Python 脚本")
    st.caption("该脚本在字段映射后执行，可直接修改 record。")
    current_script = get_table_script(table_name) or ""
    py_script = st.text_area("自定义脚本", value=current_script, height=150, key=f"py_script_{table_name}")
    cols = st.columns([1, 1, 6])
    with cols[0]:
        if st.button("保存脚本", key=f"save_script_{table_name}"):
            save_table_script(table_name, py_script or "")
            st.success("脚本已保存")
    with cols[1]:
        if st.button("清空脚本", key=f"clear_script_{table_name}"):
            save_table_script(table_name, "")
            st.success("脚本已清空"); st.rerun()

    st.divider()

    # ---- 字段映射（单行紧凑） ----
    st.subheader("字段映射（单行、手动保存；支持一键保存全部）")

    head = st.columns([2, 3, 5, 1, 1])
    head[0].markdown("**字段**")
    head[1].markdown("**target_paths**")
    head[2].markdown("**rule**")
    head[3].markdown("**状态**")
    head[4].markdown("**操作**")

    changed_any = False
    to_save_all = []

    for idx, m in enumerate(mappings):
        sfield = m["source_field"]
        # 控件键固定为“表+字段”，避免刷新时错乱/重复
        tp_key = f"tp__{table_name}__{sfield}"
        rl_key = f"rl__{table_name}__{sfield}"

        cols = st.columns([2, 3, 5, 1, 1])
        with cols[0]:
            st.text(sfield or "(自定义)")

        new_tpath = cols[1].text_input("", value=m["target_paths"], key=tp_key, placeholder="target_paths")
        new_rule  = cols[2].text_input("", value=m["rule"],        key=rl_key, placeholder="rule")

        # 改动检测：与快照比
        snap_tp, snap_rule = snapshot.get(sfield, ("", ""))
        is_changed = (new_tpath != snap_tp) or (new_rule != snap_rule)

        with cols[3]:
            st.markdown("🟠" if is_changed else "✅")

        with cols[4]:
            c1, c2 = st.columns(2)
            with c1:
                if st.button("💾", key=f"save_row__{table_name}__{sfield}"):
                    update_field_mapping(table_name, sfield, new_tpath, new_rule)
                    # 同步本地缓存与快照
                    m["target_paths"] = new_tpath
                    m["rule"] = new_rule
                    snapshot[sfield] = (new_tpath, new_rule)
                    st.success(f"{sfield} 已保存")
                    st.rerun()
            with c2:
                if st.button("🗑", key=f"del_row__{table_name}__{sfield}"):
                    delete_field_mapping(table_name, sfield)
                    # 从本地缓存与快照移除
                    st.session_state[cache_key] = [x for x in st.session_state[cache_key] if x["source_field"] != sfield]
                    snapshot.pop(sfield, None)
                    st.success(f"{sfield} 已删除")
                    st.rerun()

        # 累积到“保存全部”队列
        if is_changed:
            changed_any = True
            to_save_all.append({
                "source_field": sfield,
                "target_paths": new_tpath,
                "rule": new_rule
            })

    st.divider()

    # ✅ 一键保存全部
    if st.button("💾 一键保存全部修改", use_container_width=True):
        if changed_any:
            update_many_field_mappings(table_name, to_save_all)
            # 更新快照
            for it in to_save_all:
                snapshot[it["source_field"]] = (it["target_paths"], it["rule"])
            # 同步本地缓存
            fresh = get_field_mappings(table_name)
            st.session_state[cache_key] = fresh
            st.success("✅ 所有修改已保存")
            st.rerun()
        else:
            st.info("没有需要保存的字段。")

    st.divider()

    # ---- 新增自定义 ----
    st.subheader("新增自定义映射")
    with st.form(f"add_{table_name}"):
        src = st.text_input("source_field（可空）", key=f"add_src_{table_name}")
        tgt = st.text_input("target_paths（例：data.name）", key=f"add_tgt_{table_name}")
        rule_new = st.text_input("rule（可空）", key=f"add_rule_{table_name}")
        if st.form_submit_button("添加"):
            upsert_field_mapping(table_name, src, tgt, rule_new)
            # 刷新缓存与快照
            fresh = get_field_mappings(table_name)
            st.session_state[cache_key] = fresh
            st.session_state[snap_key][src or ""] = (tgt, rule_new)
            st.success("✅ 已添加新字段")
            st.rerun()

    st.divider()

    # ---- 模拟打印 ----
    st.subheader("模拟打印")
    idx_key = f"sample_idx_{table_name}"
    if idx_key not in st.session_state:
        st.session_state[idx_key] = 0
    sample_index = st.session_state[idx_key]

    cols_pg = st.columns([1, 1, 6])
    with cols_pg[0]:
        if st.button("⬅️ 上一条", key=f"prev_{table_name}"):
            if sample_index > 0:
                st.session_state[idx_key] -= 1; st.rerun()
    with cols_pg[1]:
        if st.button("下一条 ➡️", key=f"next_{table_name}"):
            st.session_state[idx_key] += 1; st.rerun()

    sample = _get_sample_record_from_sql(table_name, st.session_state[idx_key]) or {}
    with st.expander("SQL 样例记录", expanded=False):
        st.code(json.dumps(sample, ensure_ascii=False, indent=2))

    if st.button("生成模拟打印", key=f"print_{table_name}"):
        py_now = get_table_script(table_name) or ""
        data_rec, out_name, type_override = apply_record_mapping(table_name, sample, py_now)
        preview = {
            "uuid": "(mock uuid)",
            "sid": SID,
            "type": type_override or table_name,
            "name": out_name or "",
            "data": data_rec
        }
        st.success("生成成功：")
        st.code(json.dumps(preview, ensure_ascii=False, indent=2))

    if st.button("返回列表", key=f"back_{table_name}"):
        st.session_state.page = "list"
        st.session_state.current_table = ""
        st.rerun()


# ================= 页面：列表 =================
def render_table_list():
    st.title("源表列表")

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("导出配置", key="export_all_btn"):
            cfg = export_all()
            st.download_button(
                "下载 mapping_config.json",
                data=json.dumps(cfg, ensure_ascii=False, indent=2),
                file_name="mapping_config.json",
                mime="application/json",
                key="download_all_btn"
            )
    with col2:
        file = st.file_uploader("导入配置", type=["json"], key="import_all_btn")
        if file:
            obj = json.loads(file.read().decode("utf-8"))
            import_all(obj)
            st.success("导入完成"); st.rerun()

    st.divider()

    col_s = st.columns([3, 2])
    with col_s[0]:
        search = st.text_input("搜索", key="search_tables")
    with col_s[1]:
        show_disabled = st.checkbox("显示停用表", value=False, key="show_disabled")

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
    else:
        render_table_detail(st.session_state.current_table)


if __name__ == "__main__":
    main()
