# app.py
# -*- coding: utf-8 -*-
import json
import re
from pathlib import Path
import streamlit as st
import time
from typing import Any, Dict

# 顶部 import 部分
from backend.db import (
    init_db, list_tables, list_mapped_tables, save_table_mapping, soft_delete_table,
    restore_table, get_target_entity, get_priority,
    get_field_mappings, upsert_field_mapping, update_field_mapping, update_many_field_mappings,
    delete_field_mapping, get_table_script, save_table_script,
    export_all, import_all,
    rename_table_target_entity,
    list_table_targets,
    get_flow_entity_map, upsert_flow_entity_map, list_flow_entity_maps,
    list_file_mappings, upsert_file_mapping, delete_file_mapping
)
from backend.source_fields import detect_source_fields, detect_sql_path,detect_field_comments, detect_table_title
from backend.mapper_core import apply_record_mapping, check_entity_status, import_table_data, delete_table_data, clear_sql_cache, _parse_sql_file, _extract_entity_meta, _upsert_entity_row
from backend.sql_utils import update_runtime_db, current_cfg
from backend.presets import init_presets_db, list_presets, save_preset, delete_preset, get_last_runtime, save_last_runtime

try:
    from version3 import SID
except Exception:
    SID = "default_sid"

st.set_page_config(page_title="表映射管理工具", layout="wide")
init_db()
init_presets_db()

# =============== 侧边栏：数据库与 SID 选择 ===============
if "db_kind" not in st.session_state:
    st.session_state.db_kind = "mysql"
if "db_cfg" not in st.session_state:
    st.session_state.db_cfg = {
        "host": "127.0.0.1",
        "port": 3307,
        "user": "im",
        "password": "root",
        "database": "im",
        "charset": "utf8mb4",
        "autocommit": False,
        "schema": "public",  # 仅 PG 使用
    }
if "current_sid" not in st.session_state:
    st.session_state.current_sid = SID
# 启动时尝试恢复最近一次应用的运行时配置
_last = get_last_runtime()
if _last:
    st.session_state.db_kind = _last.get("kind") or st.session_state.db_kind
    st.session_state.db_cfg = {
        "host": _last.get("host", st.session_state.db_cfg.get("host")),
        "port": int(_last.get("port", st.session_state.db_cfg.get("port"))),
        "user": _last.get("user", st.session_state.db_cfg.get("user")),
        "password": _last.get("password", st.session_state.db_cfg.get("password")),
        "database": _last.get("database", st.session_state.db_cfg.get("database")),
        "charset": _last.get("charset", st.session_state.db_cfg.get("charset")),
        "autocommit": bool(_last.get("autocommit", st.session_state.db_cfg.get("autocommit"))),
        "schema": _last.get("schema", st.session_state.db_cfg.get("schema")),
    }
    # 兼容：如无 sid 则回退使用 schema
    st.session_state.current_sid = _last.get("sid") or _last.get("schema") or st.session_state.current_sid
    try:
        update_runtime_db(st.session_state.db_kind, st.session_state.db_cfg)
    except Exception as e:
        st.warning(f"恢复上次配置失败：{e}")

with st.sidebar:
    st.header("库/空间目标")
    st.caption("列表：名称-sid（删除：❌）；支持添加与应用")

    # 预设列表：点击即切换
    presets = list_presets()
    if presets:
        for p in presets:
            disp_label = (p.get('name') or '').strip()
            # 兼容旧预设：无 sid 则显示 schema
            sid_label = (p.get('sid') or p.get('schema') or '').strip()
            label = f"{disp_label}-{sid_label}" if sid_label else disp_label
            cols_row = st.columns([4, 1])
            with cols_row[0]:
                if st.button(label or "(未命名)", key=f"preset_select_{p.get('name','')}"):
                    st.session_state["selected_preset_name"] = p.get("name")
                    st.session_state["selected_preset_label"] = label or p.get("name")
            with cols_row[1]:
                if st.button("❌", key=f"preset_del_{p.get('name','')}"):
                    try:
                        delete_preset(p.get("name"))
                        st.success("已删除预设")
                        st.rerun()
                    except Exception as e:
                        st.error(f"删除失败：{e}")
        if st.session_state.get("selected_preset_label"):
            st.caption(f"已选中：{st.session_state.get('selected_preset_label')}")
    else:
        st.info("暂无预设，请点击下方『添加』进行创建")

    # 交互：添加 & 应用
    ctrl_cols = st.columns([1, 1])
    with ctrl_cols[0]:
        if st.button("添加"):
            st.session_state["show_add_panel"] = True
    with ctrl_cols[1]:
        if st.button("应用"):
            sel_name = st.session_state.get("selected_preset_name")
            if not sel_name:
                st.warning("请先在上方列表里选择一个条目。")
            else:
                # 找到并应用
                presets = list_presets()
                target = next((x for x in presets if x.get("name") == sel_name), None)
                if not target:
                    st.warning("选中的条目不存在，请刷新后重试。")
                else:
                    st.session_state.db_kind = (target.get("kind") or st.session_state.db_kind)
                    st.session_state.db_cfg = {
                        "host": target.get("host") or st.session_state.db_cfg.get("host"),
                        "port": int(target.get("port") or st.session_state.db_cfg.get("port")),
                        "user": target.get("user") or st.session_state.db_cfg.get("user"),
                        "password": target.get("password") or st.session_state.db_cfg.get("password"),
                        "database": target.get("database") or st.session_state.db_cfg.get("database"),
                        "charset": target.get("charset") or st.session_state.db_cfg.get("charset"),
                        "autocommit": bool(target.get("autocommit") if target.get("autocommit") is not None else st.session_state.db_cfg.get("autocommit")),
                        # 统一：schema 即为 SID；兼容旧数据使用 schema
                        "schema": target.get("sid") or target.get("schema") or st.session_state.db_cfg.get("schema"),
                    }
                    # 同步当前 SID，兼容旧数据
                    st.session_state.current_sid = target.get("sid") or target.get("schema") or st.session_state.current_sid
                    try:
                        update_runtime_db(st.session_state.db_kind, st.session_state.db_cfg)
                        save_last_runtime(st.session_state.db_kind, st.session_state.db_cfg, st.session_state.current_sid)
                        st.success("已应用选中条目")
                    except Exception as e:
                        st.error(f"应用失败：{e}")
                    st.rerun()

    # 添加面板（弹出式）
    if st.session_state.get("show_add_panel"):
        with st.form("add_preset_form"):
            st.subheader("添加库连接与SID")
            preset_name = st.text_input("名称", value="")
            kind_label_to_val = {"mysql": "mysql", "postgres": "pg"}
            kind_choice = st.selectbox("数据库类型", options=list(kind_label_to_val.keys()), index=0)
            host_inp = st.text_input("主机", value=st.session_state.db_cfg.get("host", "127.0.0.1"))
            port_inp = st.number_input("端口", value=int(st.session_state.db_cfg.get("port", 3306)), step=1)
            user_inp = st.text_input("用户", value=st.session_state.db_cfg.get("user", "root"))
            pwd_inp  = st.text_input("密码", value=st.session_state.db_cfg.get("password", ""))
            db_inp   = st.text_input("库/数据库", value=st.session_state.db_cfg.get("database", ""))
            # 统一：空间即 SID
            schema_inp = st.text_input("空间(sid)", value=st.session_state.db_cfg.get("schema", ""))

            c1, c2 = st.columns([1,1])
            with c1:
                do_save = st.form_submit_button("保存")
            with c2:
                do_cancel = st.form_submit_button("取消")

            if do_cancel:
                st.session_state["show_add_panel"] = False
                st.rerun()

            if do_save:
                name_norm = (preset_name or "").strip()
                if not name_norm:
                    st.warning("请填写预设名称。")
                elif not db_inp:
                    st.warning("请填写库/数据库名称。")
                else:
                    try:
                        save_preset(
                            name=name_norm,
                            kind=kind_label_to_val.get(kind_choice, "mysql"),
                            host=host_inp,
                            port=int(port_inp or 0),
                            user=user_inp,
                            password=pwd_inp,
                            database=db_inp,
                            charset=st.session_state.db_cfg.get("charset"),
                            autocommit=st.session_state.db_cfg.get("autocommit"),
                            # 同步保存：schema 与 sid 使用同一值
                            schema=(schema_inp or None),
                            sid=(schema_inp or None),
                        )
                        # 关闭添加面板并选中新建条目
                        st.session_state["show_add_panel"] = False
                        new_label = f"{name_norm}-{(schema_inp or '').strip()}" if (schema_inp or '').strip() else name_norm
                        st.session_state["selected_preset_name"] = name_norm
                        st.session_state["selected_preset_label"] = new_label
                        st.success("✅ 预设已保存")
                        st.rerun()
                    except Exception as e:
                        st.error(f"保存失败：{e}")

    # 批次（SID）单独维护
    # 已统一：SID 即为空间(schema)，不再单独维护


# ================= 工具函数 =================

def render_top_tabs(active: str):
    tabs = [
        ("home", "🏠主页"),
        ("mapped", "🧩 映射结果管理"),
        ("multi_mapping", "🧩 多映射管理中心"),
        ("flow", "🧰 流程管理"),
        ("user_dept", "👥 用户部门管理"),
        ("file", "📃 文件管理"),
    ]
    st.markdown(
        """
        <style>
        .top-tabs { display:flex; gap:8px; flex-wrap: wrap; margin:8px 0 14px; }
        .top-tabs a { font-size:15px; padding:8px 14px; line-height:1.3; border-radius:8px; border:1px solid #d0d0d0; background:#f7f7f7; text-decoration:none; color:#222; }
        .top-tabs a.active { background:#264653; color:#fff; border-color:#264653; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    html = ["<div class='top-tabs'>"]
    for key, label in tabs:
        is_active = (key == (active or "")) or (key == "home" and (active or "") in ("list", "home"))
        cls = "active" if is_active else ""
        target_page = "home" if key == "home" else key
        html.append(f"<a class='{cls}' href='?page={target_page}'>{label}</a>")
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)

# 读取本地 SQL 文件的 INSERT 行
def _read_sql_rows(table: str):
    p = detect_sql_path(table)
    if not p.exists():
        return []
    return _parse_sql_file(p)

# 选择字段列用于展示
def _pick_cols(rows, cols):
    return [{k: r.get(k, "") for k in cols} for r in rows]

# 综合构建流程实例摘要（基于本地 SQL 文件）
def _build_instance_rows():
    hi = _read_sql_rows("act_hi_procinst")
    ru_task = _read_sql_rows("act_ru_task")
    ru_exec = _read_sql_rows("act_ru_execution")
    ru_var  = _read_sql_rows("act_ru_variable")
    hi_task = _read_sql_rows("act_hi_taskinst")
    hi_act  = _read_sql_rows("act_hi_actinst")
    copies  = _read_sql_rows("bpm_process_instance_copy")
    def_info = _read_sql_rows("bpm_process_definition_info")
    cats    = _read_sql_rows("bpm_category")

    def _code_of(def_id):
        s = str(def_id or "")
        return s.split(":")[0] if ":" in s else s

    # 映射：定义编码 -> 定义信息 / 分类名称
    def_by_code = {}
    for d in def_info:
        c = _code_of(d.get("process_definition_id"))
        def_by_code.setdefault(c, d)
    cat_name_by_code = {}
    for c in cats:
        try:
            del_flag = int(str(c.get("deleted", 0) or 0))
        except Exception:
            del_flag = 0
        if del_flag != 1:
            cat_name_by_code[str(c.get("code",""))] = c.get("name","")

    from collections import defaultdict
    def _group(rows, key):
        g = defaultdict(list)
        for r in rows:
            pid = str(r.get(key, "")).strip()
            if pid:
                g[pid].append(r)
        return g

    g_task = _group(ru_task, "proc_inst_id_")
    g_exec = _group(ru_exec, "proc_inst_id_")
    g_var  = _group(ru_var,  "proc_inst_id_")
    g_htask= _group(hi_task, "proc_inst_id_")
    g_hact = _group(hi_act,  "proc_inst_id_")
    g_copy = _group(copies,  "process_instance_id")

    rows = []
    for r in hi:
        pid = r.get("id_", "")
        def_id = r.get("proc_def_id_", "")
        code = _code_of(def_id)
        di = def_by_code.get(code, {})
        cat_name = cat_name_by_code.get(code, code)

        tasks = g_task.get(pid, [])
        execs = g_exec.get(pid, [])
        vars_ = g_var.get(pid, [])
        htasks= g_htask.get(pid, [])
        hacts = g_hact.get(pid, [])
        cps   = g_copy.get(pid, [])

        open_names = sorted({t.get("name_","") for t in tasks if t.get("name_")})
        assignees  = sorted({t.get("assignee_","") for t in tasks if t.get("assignee_")})
        act_ids    = sorted({e.get("act_id_","") for e in execs if e.get("act_id_")})

        # 变量摘要：仅取前 5 个 name_=value
        def _val(v):
            return v.get("text_") or v.get("double_") or v.get("long_") or ""
        var_pairs = [f"{v.get('name_','')}={_val(v)}" for v in vars_ if v.get("name_")]
        var_summary = ", ".join(var_pairs[:5])

        users = sorted({x.get("user_id") for x in cps if x.get("user_id")})

        rows.append({
            "proc_inst_id": pid,
            "proc_def_id": def_id,
            "def_code": code,
            "category": cat_name,
            "flow_define_name": r.get("name_",""),
            "business_key": r.get("business_key_",""),
            "start_time": r.get("start_time_",""),
            "end_time": r.get("end_time_",""),
            "open_task_count": len(tasks),
            "open_task_names": ",".join(open_names),
            "open_assignees": ",".join(assignees),
            "current_activities": ",".join(act_ids),
            "hist_task_count": len(htasks),
            "hist_act_count": len(hacts),
            "copy_count": len(cps),
            "copy_users": ",".join(map(str, users)),
            "def_desc": di.get("description",""),
            "form_type": di.get("form_type",""),
            "form_id": di.get("form_id",""),
            "vars": var_summary,
        })
    # 按开始时间倒序
    rows.sort(key=lambda x: str(x.get("start_time","")), reverse=True)
    return rows

# 构建单个流程实例的 JSON 预览（基于 Flowable/Activiti act_* 与 bpm_* 本地 SQL）
def _build_instance_json(proc_inst_id: str) -> Dict[str, Any]:
    pid = str(proc_inst_id or "").strip()
    if not pid:
        return {}
    hi = _read_sql_rows("act_hi_procinst")
    hist = next((r for r in hi if str(r.get("id_","")) == pid), None)
    if not hist:
        return {"procInstId": pid, "error": "not found in act_hi_procinst"}

    def _code_of(def_id):
        s = str(def_id or "")
        return s.split(":")[0] if ":" in s else s

    def_id = hist.get("proc_def_id_", "")
    def_code = _code_of(def_id)

    # 运行时/历史明细
    ru_task = [r for r in _read_sql_rows("act_ru_task") if str(r.get("proc_inst_id_","")) == pid]
    ru_exec = [r for r in _read_sql_rows("act_ru_execution") if str(r.get("proc_inst_id_","")) == pid]
    ru_var  = [r for r in _read_sql_rows("act_ru_variable") if str(r.get("proc_inst_id_","")) == pid]
    hi_task = [r for r in _read_sql_rows("act_hi_taskinst") if str(r.get("proc_inst_id_","")) == pid]
    hi_act  = [r for r in _read_sql_rows("act_hi_actinst")  if str(r.get("proc_inst_id_","")) == pid]
    hi_var  = [r for r in _read_sql_rows("act_hi_varinst")  if str(r.get("proc_inst_id_","")) == pid]
    hi_cmts = [r for r in _read_sql_rows("act_hi_comment")  if str(r.get("proc_inst_id_","")) == pid]
    copies  = [r for r in _read_sql_rows("bpm_process_instance_copy") if str(r.get("process_instance_id","")) == pid]

    # 定义与分类
    def_info_all = _read_sql_rows("bpm_process_definition_info")
    def_info = next((d for d in def_info_all if _code_of(d.get("process_definition_id")) == def_code), {})
    cats = _read_sql_rows("bpm_category")
    _cat_map = {}
    _cat_map_any = {}
    for c in cats:
        code = str(c.get("code",""))
        name = c.get("name","")
        _cat_map_any[code] = name
        try:
            del_flag = int(str(c.get("deleted", 0) or 0))
        except Exception:
            del_flag = 0
        if del_flag != 1:
            _cat_map[code] = name
    cat_name_by_code = _cat_map
    category_name = cat_name_by_code.get(def_code, def_code)
    flow_define_name = str(hist.get("name_", "") or "")

    # 表单信息
    form_preview = {}
    form_type = str(def_info.get("form_type",""))
    form_id = def_info.get("form_id")
    if form_type == "10" and form_id:
        forms = _read_sql_rows("bpm_form")
        fi = next((f for f in forms if str(f.get("id","")) == str(form_id)), None)
        if fi:
            form_preview = {
                "id": fi.get("id",""),
                "name": fi.get("name",""),
                "status": fi.get("status",""),
                "remark": fi.get("remark",""),
                "fields": fi.get("fields",""),
                "conf": fi.get("conf",""),
            }
    else:
        form_preview = {
            "form_type": form_type,
            "form_id": form_id or "",
            "form_fields": def_info.get("form_fields",""),
            "form_conf": def_info.get("form_conf",""),
        }

    # 变量归并为 name -> value
    def _var_value(v):
        return v.get("text_") or v.get("double_") or v.get("long_") or ""
    runtime_vars = {str(v.get("name_","")): _var_value(v) for v in ru_var if v.get("name_")}
    hist_vars    = {str(v.get("name_","")): _var_value(v) for v in hi_var if v.get("name_")}

    # 运行时任务与执行树精选字段
    run_tasks = _pick_cols(ru_task, ["id_","name_","assignee_","owner_","create_time_","due_date_","category_","priority_","proc_inst_id_"])
    run_execs = _pick_cols(ru_exec, ["id_","parent_id_","super_exec_","act_id_","is_active_","is_concurrent_","is_scope_","proc_inst_id_"])
    # 历史任务与节点轨迹精选字段
    hist_tasks = _pick_cols(hi_task, [
        "id_","task_id_","name_","assignee_","owner_",
        "start_time_","end_time_","duration_",
        "delete_reason_","proc_inst_id_","parent_task_id_"
    ])
    hist_acts  = _pick_cols(hi_act,  ["id_","act_id_","act_name_","assignee_","start_time_","end_time_","task_id_","proc_inst_id_"])

    # 抄送记录精选字段
    copy_rows = _pick_cols(copies, ["id","user_id","start_user_id","task_id","task_name","category","process_instance_id","process_instance_name","create_time","update_time"]) 

    # 汇总 JSON
    # 活动流水线（按开始时间排序）
    pipeline = []
    acts_sorted = sorted(hi_act, key=lambda a: str(a.get("start_time_", "") or ""))
    for a in acts_sorted:
        ex = str(a.get("execution_id_", ""))
        tid = str(a.get("task_id_", ""))
        activity = {
            "id_": a.get("id_", ""),
            "act_id_": a.get("act_id_", ""),
            "act_name_": a.get("act_name_", ""),
            "act_type_": a.get("act_type_", ""),
            "assignee_": a.get("assignee_", ""),
            "start_time_": a.get("start_time_", ""),
            "end_time_": a.get("end_time_", ""),
            "duration_": a.get("duration_", ""),
            "task_id_": tid,
        }
        task_detail = next((t for t in hi_task if str(t.get("id_", "")) == tid), None)
        if not task_detail:
            task_detail = next((t for t in ru_task if str(t.get("id_", "")) == tid), None)
        comments = [
            {
                "id_": c.get("id_", ""),
                "time_": c.get("time_", ""),
                "user_id_": c.get("user_id_", ""),
                "action_": c.get("action_", ""),
                "message_": c.get("message_", ""),
            }
            for c in hi_cmts if str(c.get("task_id_", "")) == tid
        ]
        var_run = [
            {
                "name_": v.get("name_", ""),
                "value": _var_value(v),
                "create_time_": v.get("create_time_", ""),
                "last_updated_time_": v.get("last_updated_time_", ""),
            }
            for v in ru_var if str(v.get("execution_id_", "")) == ex
        ]
        var_hist = [
            {
                "name_": v.get("name_", ""),
                "value": _var_value(v),
                "create_time_": v.get("create_time_", ""),
                "last_updated_time_": v.get("last_updated_time_", ""),
            }
            for v in hi_var if str(v.get("execution_id_", "")) == ex
        ]
        pipeline.append({
            "activity": activity,
            "task": task_detail or {},
            "comments": comments,
            "variables": {"runtime": var_run, "history": var_hist},
        })

    # segments：将 sequenceFlow / exclusiveGateway 归入前后节点之间的“经由”链路，使关系更直观
    def _is_node(act_type: str) -> bool:
        t = (act_type or "").lower()
        return t in ("startevent", "usertask", "endevent")
    def _is_via(act_type: str) -> bool:
        t = (act_type or "").lower()
        return t in ("sequenceflow", "exclusivegateway")
    def _fmt_node(a: Dict[str, Any]) -> Dict[str, Any]:
        if not a:
            return {}
        return {
            "key": a.get("act_id_",""),
            "name": a.get("act_name_",""),
            "type": a.get("act_type_",""),
            "assignee": a.get("assignee_",""),
            "start": a.get("start_time_",""),
            "end": a.get("end_time_",""),
            "duration": a.get("duration_",""),
        }
    def _fmt_via(a: Dict[str, Any]) -> Dict[str, Any]:
        if not a:
            return {}
        return {
            "key": a.get("act_id_",""),
            "type": a.get("act_type_",""),
            "name": a.get("act_name_",""),
            "time": a.get("start_time_",""),
        }
    def _trim_task(t: Dict[str, Any]) -> Dict[str, Any]:
        if not t:
            return {}
        keys = [
            "id_","parent_task_id_",
            "name_","assignee_","owner_","start_time_","end_time_",
            "duration_","priority_","category_","delete_reason_",
        ]
        return {k: t.get(k, "") for k in keys}
    def _values_imp(entry: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        vv = []
        vhist = entry.get("variables", {}).get("history", [])
        vrun  = entry.get("variables", {}).get("runtime", [])
        vv.extend(vhist or [])
        vv.extend(vrun or [])
        def getv(name: str):
            for v in vv:
                if str(v.get("name_","")) == name:
                    return v.get("value","")
            return ""
        out["TASK_STATUS"] = getv("TASK_STATUS")
        out["TASK_REASON"] = getv("TASK_REASON")
        out["loopCounter"] = getv("loopCounter")
        assg = ""
        for v in vv:
            n = str(v.get("name_",""))
            if n.endswith("_assignee"):
                assg = v.get("value","")
                break
        out["assignee_var"] = assg
        return out
    def _last_comment(comments: Dict[str, Any]) -> Dict[str, Any]:
        cs = list(comments or [])
        if not cs:
            return {}
        cs.sort(key=lambda c: str(c.get("time_","")))
        last = cs[-1]
        return {"time": last.get("time_",""), "user_id": last.get("user_id_",""), "message": last.get("message_",""), "action": last.get("action_","")}
    def _actors(entry: Dict[str, Any]) -> Dict[str, Any]:
        s = set()
        act = entry.get("activity", {})
        t = entry.get("task", {})
        s.add(str(act.get("assignee_","")))
        s.add(str(t.get("assignee_","")))
        s.add(str(t.get("owner_","")))
        for c in entry.get("comments", []) or []:
            s.add(str(c.get("user_id_","")))
        s = {x for x in s if x and x != ""}
        return {"ids": sorted(list(s))}
    segments = []
    i = 0
    while i < len(pipeline):
        seg_from_entry = pipeline[i]
        a = seg_from_entry["activity"]
        if not _is_node(a.get("act_type_","")):
            i += 1
            continue
        via = []
        j = i + 1
        while j < len(pipeline):
            aj = pipeline[j]["activity"]
            if _is_via(aj.get("act_type_","")):
                via.append(_fmt_via(aj))
                j += 1
                continue
            # 遇到下一节点则结束当前分段
            break
        seg_to_entry = pipeline[j] if j < len(pipeline) else {"activity": {}}
        to_node = seg_to_entry.get("activity", {})
        for k in range(len(via)):
            if (via[k].get("type", "")).lower() == "sequenceflow" and not via[k].get("name"):
                guess = next((x.get("act_name_") for x in hi_act if str(x.get("act_id_", "")) == via[k].get("key", "") and x.get("act_name_")), "")
                if not guess:
                    guess = to_node.get("act_name_", "")
                via[k]["name"] = guess or via[k].get("name", "")
        segments.append({
            "from": _fmt_node(a),
            "via": via,
            "to": _fmt_node(to_node) if to_node else {},
            "to_task": _trim_task(seg_to_entry.get("task", {})) if to_node else {},
            "to_values": _values_imp(seg_to_entry) if to_node else {},
            "to_comment_last": _last_comment(seg_to_entry.get("comments", []) or []) if to_node else {},
            "to_actor_ids": _actors(seg_to_entry).get("ids", []) if to_node else [],
        })
        i = j if j > i else i + 1

    # 任务ID → 最近一条批注
    comments_by_task = {}
    try:
        tids = {str(t.get("id_","")) for t in hi_task if t.get("id_")}
        for tid in tids:
            cs = [c for c in hi_cmts if str(c.get("task_id_","")) == tid]
            comments_by_task[tid] = _last_comment(cs)
    except Exception:
        comments_by_task = {}

    out = {
        "procInstId": pid,
        "procDefId": def_id,
        "defCode": def_code,
        "processName": category_name,
        "flow_define_name": flow_define_name,
        "businessKey": hist.get("business_key_",""),
        "startTime": hist.get("start_time_",""),
        "endTime": hist.get("end_time_",""),
        "starterUserId": hist.get("start_user_id_",""),
        "definition": {
            "description": def_info.get("description",""),
            "modelId": def_info.get("model_id",""),
            "icon": def_info.get("icon",""),
            "formType": form_type,
            "formId": form_id or "",
            "categoryName": category_name,
            "flowDefineName": flow_define_name,
            "formPreview": form_preview,
        },
        "runtime": {
            "tasks": run_tasks,
            "executions": run_execs,
            "variables": runtime_vars,
        },
        "history": {
            "tasks": hist_tasks,
            "activities": hist_acts,
            "variables": hist_vars,
            "comments_by_task": comments_by_task,
        },
        "copies": copy_rows,
        "pipeline": pipeline,
        "segments": segments,
    }
    return out

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
        r"\s*\((?P<cols>[^)]*)\)\s*values\s*\((?P<vals>[\s\S]*?)\)\s*;",
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
        r"\s*\((?P<cols>[^)]*)\)\s*values\s*\((?P<vals>[\s\S]*?)\)\s*;",
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


_USER_MAP = None
_USER_NAME_MAP = None
_DEPT_MAP = None

def _user_dept_maps():
    global _USER_MAP, _USER_NAME_MAP, _DEPT_MAP
    if _USER_MAP is None:
        rows = _parse_all_inserts("sys_user")
        m = {}
        nmap = {}
        for r in rows:
            uid = str(r.get("user_id") or "").strip()
            if not uid:
                continue
            name = str(r.get("nick_name") or "").strip()
            dept_id = str(r.get("dept_id") or "").strip()
            prev = m.get(uid)
            if prev:
                if not prev.get("dept_id") and dept_id:
                    m[uid] = {"name": name, "dept_id": dept_id}
            else:
                m[uid] = {"name": name, "dept_id": dept_id}
            if name:
                prev = nmap.get(name)
                if dept_id:
                    nmap[name] = {"name": name, "dept_id": dept_id}
                elif not prev:
                    nmap[name] = {"name": name, "dept_id": dept_id}
        _USER_MAP = m
        _USER_NAME_MAP = nmap
    if _DEPT_MAP is None:
        rows = _parse_all_inserts("sys_dept")
        _DEPT_MAP = {str(r.get("dept_id") or "").strip(): str(r.get("dept_name") or r.get("name") or "").strip() for r in rows}
    return _USER_MAP, _DEPT_MAP

def _enrich_nodes_with_user(nodes):
    umap, dmap = _user_dept_maps()
    out = []
    for nd in nodes:
        t = nd.get("task") or {}
        assignee_id = str((t.get("assignee_") or nd.get("assignee") or "")).strip()
        info = umap.get(assignee_id)
        if not info and assignee_id:
            info = (_USER_NAME_MAP or {}).get(assignee_id)
        if info:
            nd["assignee_val"] = info.get("name","")
            dep = dmap.get(info.get("dept_id",""), "")
            if not dep and info.get("name"):
                aux = (_USER_NAME_MAP or {}).get(info.get("name",""))
                dep = dmap.get((aux or {}).get("dept_id",""), "") or dep
            nd["dept"] = dep
        out.append(nd)
    return out

def _build_flow_import_bundle(pid: str, match: Dict[str, Any] = None) -> Dict[str, Any]:
    data = _build_instance_json(pid)
    rt_vars_list = data.get("runtime", {}).get("variables", [])
    hi_vars_list = data.get("history", {}).get("variables", [])
    def _var_map(vs):
        if isinstance(vs, dict):
            return vs
        m = {}
        for v in vs or []:
            if isinstance(v, dict):
                n = str(v.get("name_",""))
                if n:
                    m[n] = v.get("value","")
        return m
    rt_vars_map = _var_map(rt_vars_list)
    hi_vars_map = _var_map(hi_vars_list)
    biz_name = str(rt_vars_map.get("businessName") or hi_vars_map.get("businessName") or "")
    segs = data.get("segments", []) or []
    nodes = []
    for idx in range(len(segs)):
        seg = segs[idx]
        frm = seg.get("from", {})
        via = seg.get("via", []) or []
        to_ = seg.get("to", {})
        if idx == 0:
            nodes.append({
                "id": frm.get("key",""),
                "type": frm.get("type",""),
                "name": frm.get("name",""),
                "assignee": frm.get("assignee",""),
                "start": frm.get("start",""),
                "end": frm.get("end",""),
                "duration": frm.get("duration",""),
                "next": {"to": to_.get("key",""), "via": via},
            })
        next_obj = {}
        if idx + 1 < len(segs):
            nxt = segs[idx + 1]
            nxt_from = nxt.get("from", {})
            if nxt_from.get("key") == to_.get("key"):
                next_obj = {"to": nxt.get("to", {}).get("key",""), "via": nxt.get("via", []) or []}
        lc = seg.get("to_comment_last", {})
        nodes.append({
            "id": to_.get("key",""),
            "type": to_.get("type",""),
            "name": to_.get("name",""),
            "assignee": to_.get("assignee",""),
            "start": to_.get("start",""),
            "end": to_.get("end",""),
            "duration": to_.get("duration",""),
            "lastComment": {"time": lc.get("time",""), "userId": lc.get("user_id",""), "message": lc.get("message","")},
            "task": seg.get("to_task", {}) or {},
            "value": seg.get("to_values", {}) or {},
            "actor_ids": seg.get("to_actor_ids", []) or [],
            "next": next_obj,
        })
    nodes = _enrich_nodes_with_user(nodes)
    assignees = {}
    for seg in segs:
        k = seg.get("to", {}).get("key","")
        a = seg.get("to_task", {}).get("assignee_","")
        if k:
            assignees[k] = a
    history_vars = {
        "processStatus": str(hi_vars_map.get("processStatus","")),
        "taskStatus": str(hi_vars_map.get("taskStatus") or hi_vars_map.get("TASK_STATUS") or ""),
        "taskReason": str(hi_vars_map.get("taskReason") or hi_vars_map.get("TASK_REASON") or ""),
        "nrOfInstances": str(hi_vars_map.get("nrOfInstances","")),
        "nrOfActiveInstances": str(hi_vars_map.get("nrOfActiveInstances","")),
        "nrOfCompletedInstances": str(hi_vars_map.get("nrOfCompletedInstances","")),
        "isSign": str(hi_vars_map.get("isSign","")),
        "assignees": assignees,
    }
    starter_code = str(data.get("starterUserId") or ((nodes[0] or {}).get("assignee") or ((nodes[0] or {}).get("task") or {}).get("assignee_") or "")).strip() if nodes else str(data.get("starterUserId") or "")
    preview_obj = {
        "meta": {
            "businessName": biz_name,
            "processName": data.get("defCode",""),
            "flowDefineName": data.get("flow_define_name",""),
            "startTime": data.get("startTime",""),
            "endTime": data.get("endTime",""),
            "icon": data.get("definition", {}).get("icon", ""),
            "starterCode": starter_code,
        },
        "variables": {"runtime": {}, "history": history_vars},
        "nodes": nodes,
    }
    def _flow_table(flow_name: str):
        fm = get_flow_entity_map(flow_name)
        return fm.get("source_table") or {
            "合伙协议": "ct_partner_agreement",
            "募集协议审批流程": "ct_fund_base_info",
            "托管协议流程审批": "ct_fund_custody_agmt",
            "其他流程": "ct_agreement_other",
            "项目合规性审查": "ct_project_base_info",
            "基金出资记录": "ct_invest_record",
            "项目退出": "ct_fund_quit_record",
            "会议管理审批流程": "ct_meeting_manage",
            "业务审批": "ct_fund_meet_manage",
            "基金公示审核": "ct_fund_publicity_review",
            "股权直投业务审批": "ct_project_meet_manage",
            "股权直投，其他协议": "ct_project_agreement_other",
        }.get(flow_name)
    fields_obj = {}
    fdef = str(data.get("flow_define_name",""))
    tbl = _flow_table(fdef)
    entity = ""
    out_name = ""
    type_override = ""
    used_match = None
    # 默认实体类型来自流程映射，即使没有样例匹配
    fm0 = get_flow_entity_map(fdef)
    if fm0.get("target_entity"):
        entity = fm0.get("target_entity")
    if tbl:
        recs = _parse_all_inserts(tbl)
        mm = match if match is not None else next((r for r in recs if str(r.get("process_instance_id","")) == str(pid)), None)
        if mm:
            script = get_table_script(tbl, entity or None) or ""
            mapped, out_name, type_override = apply_record_mapping(tbl, mm, script, target_entity=entity or "")
            _ = _extract_entity_meta(mapped)
            fields_obj = mapped or {}
            used_match = mm
    src = json.dumps(preview_obj, ensure_ascii=False)
    esc = src.replace("'", "''")
    fields_obj = fields_obj or {}
    fields_obj["source_flow"] = esc
    try:
        raw = fields_obj.get("source_flow", "")
        parsed = json.loads(raw.replace("''", "'")) if raw else {}
    except Exception:
        parsed = preview_obj
    meta_info = parsed.get("meta", {}) or {}
    hist = (parsed.get("variables", {}) or {}).get("history", {}) or {}
    nodes_md = []
    def _fmt_duration_auto(v):
        if v in (None, ""):
            return ""
        s = str(v).strip()
        try:
            x = float(s)
        except Exception:
            return s
        secs = x / 1000.0 if x >= 1000 else x
        secs = int(secs)
        d = secs // 86400; secs %= 86400
        h = secs // 3600; secs %= 3600
        m = secs // 60; secs %= 60
        parts = []
        if d: parts.append(f"{d} 天")
        if h: parts.append(f"{h} 小时")
        if m: parts.append(f"{m} 分钟")
        if secs and not parts:
            parts.append(f"{secs} 秒")
        return " ".join(parts) or "0 秒"
    def _fmt_time(v):
        if v in (None, ""):
            return ""
        s = str(v).strip()
        try:
            x = float(s)
            ms = int(x) if x >= 1e11 else int(x * 1000)
            from datetime import datetime
            dt = datetime.fromtimestamp(ms / 1000.0)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                from datetime import datetime
                t = s.replace("T", " ").replace("Z", "")
                dt = datetime.fromisoformat(t)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return s
    def _ts_num(v):
        s = str(v or "").strip()
        if not s:
            return 0
        try:
            x = float(s)
            ms = int(x) if x >= 1e11 else int(x * 1000)
            return ms
        except Exception:
            try:
                from datetime import datetime
                t = s.replace("T", " ").replace("Z", "")
                return int(datetime.fromisoformat(t).timestamp() * 1000)
            except Exception:
                return 0
    nds_sorted = []
    nodes_src = (parsed.get("nodes", []) or [])
    for nd in nodes_src:
        t = nd.get("task", {}) or {}
        name0 = (t.get("name_") or nd.get("name", "") or "").strip()
        if name0 in ("开始", "结束"):
            continue
        nds_sorted.append(nd)
    if not nds_sorted and nodes_src:
        nds_sorted = nodes_src
    # 排序：从新到旧（开始时间优先，降序）
    nds_sorted.sort(key=lambda n: _ts_num((n.get("task") or {}).get("start_time_") or n.get("start", "") or (n.get("task") or {}).get("end_time_") or n.get("end", "")), reverse=True)
    # 追加未出现在节点中的纯任务（如仅在 act_hi_taskinst 存在的子任务）
    hist_tasks_list = (data.get("history", {}) or {}).get("tasks", []) or []
    present_ids = {str(((nd.get("task") or {}).get("id_")) or "").strip() for nd in nds_sorted}
    extra_nodes = []
    cm_map = ((data.get("history", {}) or {}).get("comments_by_task", {}) or {})
    for ht in hist_tasks_list:
        tid = str(ht.get("id_") or "").strip()
        if not tid or tid in present_ids:
            continue
        extra_nodes.append({
            "id": ht.get("task_id_", ""),
            "type": "userTask",
            "name": ht.get("name_", ""),
            "assignee": ht.get("assignee_", ""),
            "start": ht.get("start_time_", ""),
            "end": ht.get("end_time_", ""),
            "duration": ht.get("duration_", ""),
            "lastComment": cm_map.get(tid, {}) or {},
            "task": {
                "id_": ht.get("id_", ""),
                "parent_task_id_": ht.get("parent_task_id_", ""),
                "name_": ht.get("name_", ""),
                "assignee_": ht.get("assignee_", ""),
                "owner_": ht.get("owner_", ""),
                "start_time_": ht.get("start_time_", ""),
                "end_time_": ht.get("end_time_", ""),
                "duration_": ht.get("duration_", ""),
                "priority_": ht.get("priority_", ""),
                "category_": ht.get("category_", ""),
                "delete_reason_": ht.get("delete_reason_", ""),
            },
            "value": {},
            "actor_ids": [str(ht.get("assignee_", ""))],
            "next": {},
        })
    if extra_nodes:
        nds_sorted.extend(extra_nodes)
        nds_sorted = _enrich_nodes_with_user(nds_sorted)
    # 父子任务展示：优先展示父任务，再展示其子任务
    task_map = {}
    for nd in nds_sorted:
        t = nd.get("task") or {}
        tid = str(t.get("id_") or "").strip()
        if tid:
            task_map[tid] = nd
    from collections import defaultdict
    children_map = defaultdict(list)
    for nd in nds_sorted:
        t = nd.get("task") or {}
        p = str(t.get("parent_task_id_") or "").strip()
        if p:
            children_map[p].append(nd)
    visited = set()

    import re
    def _split_msg(s: str):
        s0 = (s or '').strip()
        inline_extra = ''
        suggest = ''
        parts = re.split(r"[，,]?\s*(?:理由为|原因是)\s*[:：]", s0)
        if len(parts) >= 2:
            inline_extra = (parts[0] or '').strip().rstrip('，。')
            suggest = (parts[1] or '').strip().rstrip('，。')
            return inline_extra, suggest
        suggest = s0
        return inline_extra, suggest

    def _fmt_block(nd: Dict[str, Any], label_child: bool = False):
        t = nd.get("task", {}) or {}
        lc = nd.get("lastComment", {}) or {}
        rawm = (str(lc.get('message') or '') + ' ' + str(t.get('delete_reason_') or '')).lower()
        mk = '⚪'
        for kw in ['同意','通过','批准','审核通过']:
            if kw in rawm:
                mk = '🟢'
                break
        if mk == '⚪':
            for kw in ['驳回','退回','拒绝','不通过','不同意']:
                if kw in rawm:
                    mk = '🔴'
                    break
        task_name = (t.get('name_') or nd.get('name','') or '').strip()
        assignee = (t.get('assignee_') or nd.get('assignee','') or '').strip()
        start_txt = _fmt_time(t.get('start_time_') or nd.get('start',''))
        end_txt = _fmt_time(t.get('end_time_') or nd.get('end',''))
        dur_text = _fmt_duration_auto(t.get('duration_')) or _fmt_duration_auto(nd.get('duration'))
        msg = (lc.get('message') or '').strip()
        inline_extra, suggest_text = _split_msg(msg)
        if (not any([assignee, start_txt, end_txt, (dur_text or ''), msg])) and (task_name in ('结束','')):
            return []
        status_text = ("审批通过" if mk=='🟢' else ("审批未通过" if mk=='🔴' else ""))
        if (not str(meta_info.get('endTime','')).strip()) and mk == '⚪':
            status_text = "审批中"
        # 单行状态：父任务用“审批任务：xxx”，子任务用“xxx→子任务”
        header = (f"**审批任务：{task_name} {mk}{(inline_extra or status_text)}**" if not label_child
                  else f"**{task_name}→子任务 {mk}{(inline_extra or status_text)}**")
        out = [header, ""]
        av = str(nd.get("assignee_val") or "").strip()
        dp = str(nd.get("dept") or "").strip()
        disp = (f"{av}（{dp}）" if av and dp else (av or assignee))
        if disp:
            out.append(f"审批人：{disp}")
            out.append("")
        line = []
        if start_txt:
            line.append(f"创建时间：{start_txt}")
        if end_txt:
            line.append(f"审批时间： {end_txt}")
        if dur_text:
            line.append(f"耗时： {dur_text}")
        if line:
            out.append(" ".join(line))
            out.append("")
        out.append(f"审批建议：{suggest_text}" if suggest_text else "审批建议：")
        out.append("")
        return out

    for nd in nds_sorted:
        t = nd.get("task", {}) or {}
        tid = str(t.get("id_") or "").strip()
        if not tid or tid in visited:
            continue
        parent_id = str(t.get("parent_task_id_") or "").strip()
        if parent_id:
            pnd = task_map.get(parent_id)
            if pnd and str((pnd.get('task') or {}).get('id_') or '').strip() not in visited:
                nodes_md.extend(_fmt_block(pnd, label_child=False))
                visited.add(str((pnd.get('task') or {}).get('id_') or '').strip())
            nodes_md.extend(_fmt_block(nd, label_child=True))
            visited.add(tid)
            continue
        nodes_md.extend(_fmt_block(nd, label_child=False))
        visited.add(tid)
        for ch in children_map.get(tid, []):
            ctid = str((ch.get('task') or {}).get('id_') or '').strip()
            if ctid and ctid not in visited:
                nodes_md.extend(_fmt_block(ch, label_child=True))
                visited.add(ctid)
    hs_raw = str(hist.get('taskStatus','')).strip()
    code_map = {
        '0':'待审批','1':'审批中','2':'审批通过','3':'审批不通过','4':'已取消','5':'已回退','6':'委派中','7':'审批通过中','8':'自动抄送'
    }
    concl = code_map.get(hs_raw)
    if not concl:
        hs = hs_raw.lower()
        hmk = ''
        for kw in ['通过','同意','批准','审核通过']:
            if kw in hs:
                hmk = '审核通过'
                break
        if not hmk:
            for kw in ['驳回','拒绝','不通过','不同意']:
                if kw in hs:
                    hmk = '审核未通过'
                    break
        concl = '审批通过' if hmk=='审核通过' else ('审批未通过' if hmk=='审核未通过' else hs_raw)
    ended_raw = meta_info.get('endTime','')
    ended_flag = bool(str(ended_raw).strip())
    head_icon = '🟢' if concl in ('审批通过','审批通过中') else ('🔴' if concl in ('审批未通过','审批不通过') else '⚪')
    header1 = f"**结束流程：在 {_fmt_time(ended_raw)} 结束**"
    header2 = f"{head_icon} {concl}"
    nds = parsed.get("nodes", []) or []
    umap, _ = _user_dept_maps()
    scode = str(meta_info.get("starterCode") or "").strip()
    sname = (umap.get(scode) or {}).get("name", "")
    starter = sname or (str(nds[0].get("assignee_val") or ((nds[0].get("task") or {}).get("assignee_") or nds[0].get("assignee") or "")).strip() if nds else "")
    flow_name = str(meta_info.get("flowDefineName") or meta_info.get("processName") or "").strip()
    start_md = f"**发起流程：【{starter}】在 {_fmt_time(meta_info.get('startTime',''))} 发起【 {flow_name} 】流程**"
    flow_md = "\n".join(([header1, header2, ""] if ended_flag else []) + nodes_md + ["", start_md]).strip()
    fields_obj["flow_md"] = flow_md
    # 统一补全：确保 data 中包含 name/type/id
    if (fields_obj.get("__name__") in (None, "")):
        fields_obj["__name__"] = biz_name
    type_name = (type_override or entity or tbl or fdef or "flow_instance")
    fields_obj["name"] = biz_name
    # 仅针对流程入库：当映射未提供 bt 或为空时，用 businessName 填充
    if not str(fields_obj.get("bt", "")).strip():
        fields_obj["bt"] = biz_name
    fields_obj["type"] = type_name
    key_field = "id"
    key_val = fields_obj.get("id") or (used_match or {}).get("id") or str(pid or "")
    fields_obj["id"] = key_val
    meta = _extract_entity_meta(fields_obj)
    final_name = biz_name
    return {
        "fields_obj": fields_obj,
        "flow_md": flow_md,
        "meta": meta,
        "type_name": type_name,
        "key_field": key_field,
        "key_val": key_val,
        "final_name": final_name,
        "tbl": tbl,
        "entity": entity,
        "out_name": out_name,
        "type_override": type_override,
        "match": used_match,
    }

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

    # 浮动导航（详情页快速跳转）
    st.markdown(
        """
        <style>
        .fixed-nav { position: fixed; top: 100px; right: 24px; background: rgba(30,30,30,0.9); color:#fff; padding: 10px 12px; border-radius: 10px; box-shadow: 0 4px 12px rgba(0,0,0,0.15); z-index: 9999; font-size: 13px; }
        .fixed-nav .title { font-weight: 600; margin-bottom: 8px; }
        .fixed-nav a { display:block; color:#fff; text-decoration: none; padding: 4px 0; }
        .fixed-nav a:hover { text-decoration: underline; }
        </style>
        <div class="fixed-nav">
          <div class="title">🔎 快速导航</div>
          <a href="#sec-config">表配置</a>
          <a href="#sec-script">表级脚本</a>
          <a href="#sec-mapping">字段映射</a>
          <a href="#sec-add">新增映射</a>
          <a href="#sec-print">模拟打印</a>
          <a href="#sec-focus">字段专注</a>
        </div>
        """,
        unsafe_allow_html=True,
    )

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
    st.markdown("<div id=\"sec-config\"></div>", unsafe_allow_html=True)
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
    st.markdown("<div id=\"sec-script\"></div>", unsafe_allow_html=True)
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

    st.subheader("SQL 缓存")
    ccols = st.columns([3, 1, 6])
    with ccols[0]:
        cache_tbl = st.text_input("表名（留空清理全部）", key=f"cache_tbl_{table_name}")
    with ccols[1]:
        if st.button("清理一次", key=f"clear_sql_cache_{table_name}"):
            tbl = (cache_tbl or "").strip() or None
            info = clear_sql_cache(tbl)
            st.success(f"已清理：rows={info.get('rows',0)}, idx={info.get('idx',0)}")
            st.rerun()

    # 字段映射（压缩行 + 单行保存 + 一键保存）
    st.markdown("<div id=\"sec-mapping\"></div>", unsafe_allow_html=True)
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
    st.markdown("<div id=\"sec-add\"></div>", unsafe_allow_html=True)
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
    st.markdown("<div id=\"sec-print\"></div>", unsafe_allow_html=True)
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
    st.markdown("<div id=\"sec-focus\"></div>", unsafe_allow_html=True)
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
    render_top_tabs('mapped')

    rows = list_mapped_tables()
    if not rows:
        st.info("暂无已设置映射的表。请先在『源表列表』里为表设置 target_entity。")
        return

    # 顶部批量操作
    c1, c2, c3 = st.columns([1,1,6])
    with c1:
        # 批量入库方式选择
        bulk_mode_label_to_val = {
            "创建更新": "upsert",
            "仅更新": "update_only",
            "仅创建": "create_only",
        }
        bulk_mode = st.selectbox(
            "入库方式",
            options=list(bulk_mode_label_to_val.keys()),
            index=0,
            key="bulk_import_mode"
        )
        if st.button("一键入库（全部）", type="primary"):
            total = 0
            progress_placeholder = st.empty()
            for r in rows:
                table = r["source_table"]
                start_ts = time.time()
                with progress_placeholder:
                    bar = st.progress(0, text=f"正在入库：{table}")
                def _fmt_eta(s):
                    try:
                        s = int(s)
                    except Exception:
                        s = 0
                    if s >= 3600:
                        h = s // 3600
                        m = (s % 3600) // 60
                        return f"{h}小时{m}分"
                    m = s // 60
                    sec = s % 60
                    return f"{m:02d}:{sec:02d}"
                def _cb(done, all):
                    all = max(all, 1)
                    pct = int(done * 100 / all)
                    elapsed = max(time.time() - start_ts, 0.001)
                    eta = int((all - done) * (elapsed / max(done, 1)))
                    bar.progress(pct, text=f"正在入库：{table}（{done}/{all}，预计剩余 {_fmt_eta(eta)}）")
                total += import_table_data(
                    table,
                    sid=st.session_state.get("current_sid", SID),
                    target_entity_spec=r["target_entity"],
                    import_mode=bulk_mode_label_to_val.get(bulk_mode, "upsert"),
                    progress_cb=_cb
                )
            progress_placeholder.empty()
            st.success(f"✅ 完成入库（{bulk_mode}），总计写入 {total} 条。")
    with c2:
        if st.button("一键删除（全部）"):
            total_del = 0
            for r in rows:
                total_del += delete_table_data(r["target_entity"], sid=st.session_state.get("current_sid", SID)) 
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
        count = check_entity_status(tgt, sid=st.session_state.get("current_sid", SID))
        status = "✅ 已入库" if count > 0 else "❌ 未入库"

        cols = st.columns([3, 3, 3, 1, 1, 3])
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
            # 行级入库方式选择 + 操作按钮
            mode_label_to_val = {
                "创建更新": "upsert",
                "仅更新": "update_only",
                "仅创建": "create_only",
            }
            row_mode_label = st.selectbox(
                "入库方式",
                options=list(mode_label_to_val.keys()),
                index=0,
                key=f"mode_{src}_{tgt}"
            )
            b1, b2 = st.columns([1,1])
            with b1:
                if st.button("入库", key=f"imp_{src}_{tgt}"):
                    progress_placeholder = st.empty()
                    start_ts = time.time()
                    bar = progress_placeholder.progress(0, text=f"正在入库：{src} → {tgt}")
                    def _fmt_eta(s):
                        try:
                            s = int(s)
                        except Exception:
                            s = 0
                        if s >= 3600:
                            h = s // 3600
                            m = (s % 3600) // 60
                            return f"{h}小时{m}分"
                        m = s // 60
                        sec = s % 60
                        return f"{m:02d}:{sec:02d}"
                    def _cb(done, all):
                        all = max(all, 1)
                        pct = int(done * 100 / all)
                        elapsed = max(time.time() - start_ts, 0.001)
                        eta = int((all - done) * (elapsed / max(done, 1)))
                        bar.progress(pct, text=f"正在入库：{src} → {tgt}（{done}/{all}，预计剩余 {_fmt_eta(eta)}）")
                    n = import_table_data(
                        src,
                        sid=st.session_state.get("current_sid", SID),
                        target_entity_spec=tgt,
                        import_mode=mode_label_to_val.get(row_mode_label, "upsert"),
                        progress_cb=_cb
                    )
                    progress_placeholder.empty()
                    st.success(f"入库完成（{row_mode_label}）：写入 {n} 条")
                    st.rerun()
            with b2:
                if st.button("删除", key=f"del_{src}_{tgt}"):
                    n = delete_table_data(tgt, sid=st.session_state.get("current_sid", SID))
                    st.success(f"删除完成：清理 {n} 条")
                    st.rerun()

# ==========================================================
# 🧩 多映射管理页（支持单表多 target_entity）
# ==========================================================
from backend.db import list_tables, list_table_targets, upsert_field_mapping,delete_table_mapping
from backend.mapper_core import import_table_data, delete_table_data, check_entity_status

@st.cache_data(ttl=30)
def _cached_list_tables():
    return [r[0] for r in list_tables()]

def render_multi_mapping():
    st.title("🧩 多映射管理中心")
    render_top_tabs('multi_mapping')

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
    st.title("🏠 主页")
    render_top_tabs('list')

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

    # 顶部导航已包含所有管理入口，主页继续保留导出/导入功能

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

# ========== 新页面：流程管理 / 文件管理 ==========

def render_flow_mgmt():
    st.title("🧰 流程管理")
    render_top_tabs('flow')
    super_tabs = st.tabs(["表单转换管理", "表单转换入库", "后台数据"])

    with super_tabs[0]:
        st.subheader("表单转换管理")
        kw = st.text_input("关键词（实例ID/业务键/定义编码）", key="form_conv_kw")
        code_filter = st.text_input("按定义编码过滤（如 ContractApproval）", key="form_conv_code")
        rows = _build_instance_rows()
        flow_names_inst = {r.get("flow_define_name","") for r in rows if r.get("flow_define_name")}
        flow_names_cfg = {x.get("flow_define_name","") for x in list_flow_entity_maps()}
        flow_names = sorted({s for s in (flow_names_inst | flow_names_cfg) if s})
        flow_filter = st.selectbox("按流程名称过滤（flowDefineName）", options=["全部"] + flow_names, index=0, key="form_conv_flowname")
        def _match(r):
            s = (kw or "").strip().lower()
            ok_kw = (not s) or s in str(r.get("proc_inst_id","")) .lower() or s in str(r.get("business_key","")) .lower() or s in str(r.get("def_code","")) .lower()
            ok_code = (not code_filter) or str(r.get("def_code","")) == code_filter
            ok_flow = (flow_filter in ("全部", "", None)) or str(r.get("flow_define_name","")) == flow_filter
            return ok_kw and ok_code and ok_flow
        view = [r for r in rows if _match(r)]
        ids = [r.get("proc_inst_id") for r in view]
        pid = st.selectbox("选择实例ID", options=ids or [""], index=0 if ids else None, key="form_conv_pid")
        if pid:
            data = _build_instance_json(pid)
            rt_vars_list = data.get("runtime", {}).get("variables", [])
            hi_vars_list = data.get("history", {}).get("variables", [])
            def _var_map(vs):
                if isinstance(vs, dict):
                    return vs
                m = {}
                for v in vs or []:
                    if isinstance(v, dict):
                        n = str(v.get("name_",""))
                        if n:
                            m[n] = v.get("value","")
                return m
            rt_vars_map = _var_map(rt_vars_list)
            hi_vars_map = _var_map(hi_vars_list)
            biz_name = str(rt_vars_map.get("businessName") or hi_vars_map.get("businessName") or "")
            if st.button("查看预览", key=f"preview_{pid}"):
                st.session_state["flow_preview_pid"] = pid
            if st.session_state.get("flow_preview_pid") == pid:
                segs = data.get("segments", []) or []
                nodes = []
                for idx in range(len(segs)):
                    seg = segs[idx]
                    frm = seg.get("from", {})
                    via = seg.get("via", []) or []
                    to_ = seg.get("to", {})
                    if idx == 0:
                        nodes.append({
                            "id": frm.get("key",""),
                            "type": frm.get("type",""),
                            "name": frm.get("name",""),
                            "assignee": frm.get("assignee",""),
                            "start": frm.get("start",""),
                            "end": frm.get("end",""),
                            "duration": frm.get("duration",""),
                            "next": {"to": to_.get("key",""), "via": via},
                        })
                    next_obj = {}
                    if idx + 1 < len(segs):
                        nxt = segs[idx + 1]
                        nxt_from = nxt.get("from", {})
                        if nxt_from.get("key") == to_.get("key"):
                            next_obj = {"to": nxt.get("to", {}).get("key",""), "via": nxt.get("via", []) or []}
                    lc = seg.get("to_comment_last", {}) or {}
                    nodes.append({
                        "id": to_.get("key",""),
                        "type": to_.get("type",""),
                        "name": to_.get("name",""),
                        "assignee": to_.get("assignee",""),
                        "start": to_.get("start",""),
                        "end": to_.get("end",""),
                        "duration": to_.get("duration",""),
                        "lastComment": {"time": lc.get("time",""), "userId": lc.get("user_id",""), "message": lc.get("message","")},
                        "task": seg.get("to_task", {}) or {},
                        "value": seg.get("to_values", {}) or {},
                        "actor_ids": seg.get("to_actor_ids", []) or [],
                        "next": next_obj,
                    })
                assignees = {}
                for seg in segs:
                    k = seg.get("to", {}).get("key","")
                    a = seg.get("to_task", {}).get("assignee_","")
                    if k:
                        assignees[k] = a
                history_vars = {
                    "processStatus": str(hi_vars_map.get("processStatus","")),
                    "taskStatus": str(hi_vars_map.get("taskStatus") or hi_vars_map.get("TASK_STATUS") or ""),
                    "taskReason": str(hi_vars_map.get("taskReason") or hi_vars_map.get("TASK_REASON") or ""),
                    "nrOfInstances": str(hi_vars_map.get("nrOfInstances","")),
                    "nrOfActiveInstances": str(hi_vars_map.get("nrOfActiveInstances","")),
                    "nrOfCompletedInstances": str(hi_vars_map.get("nrOfCompletedInstances","")),
                    "isSign": str(hi_vars_map.get("isSign","")),
                    "assignees": assignees,
                }

                def _flow_table_map():
                    return {
                        "合伙协议": "ct_partner_agreement",
                        "募集协议审批流程": "ct_fund_base_info",
                        "托管协议流程审批": "ct_fund_custody_agmt",
                        "其他流程": "ct_agreement_other",
                        "项目合规性审查": "ct_project_base_info",
                        "基金出资记录": "ct_invest_record",
                        "项目退出": "ct_fund_quit_record",
                        "会议管理审批流程": "ct_meeting_manage",
                        "业务审批": "ct_fund_meet_manage",
                        "基金公示审核": "ct_fund_publicity_review",
                        "股权直投业务审批": "ct_project_meet_manage",
                        "股权直投，其他协议": "ct_project_agreement_other",
                    }
                def _flow_table(flow_name: str):
                    fm = get_flow_entity_map(flow_name)
                    return fm.get("source_table") or _flow_table_map().get(flow_name)

                fields_obj = {}
                fdef = str(data.get("flow_define_name",""))
                tbl = _flow_table(fdef)
                if tbl:
                    recs = _parse_all_inserts(tbl)
                    match = next((r for r in recs if str(r.get("process_instance_id","")) == str(pid)), None)
                    if match:
                        fm = get_flow_entity_map(fdef)
                        entity = (fm.get("target_entity") or get_target_entity(tbl) or "")
                        script = get_table_script(tbl, entity or None) or ""
                        mapped, _, _ = apply_record_mapping(tbl, match, script, target_entity=entity or "")
                        _ = _extract_entity_meta(mapped)
                        fields_obj = mapped or {}
                preview_obj = {
                    "meta": {
                        "businessName": biz_name,
                        "processName": data.get("defCode",""),
                        "flowDefineName": data.get("flow_define_name",""),
                        "startTime": data.get("startTime",""),
                        "endTime": data.get("endTime",""),
                        "icon": data.get("definition", {}).get("icon", ""),
                    },
                    "variables": {"runtime": {}, "history": history_vars, "fields": fields_obj},
                    "nodes": nodes,
                }
                st.json(preview_obj)

        st.markdown("---")
        st.subheader("流程字段映射管理")
        fmap = {
            "合伙协议": "ct_partner_agreement",
            "募集协议审批流程": "ct_fund_base_info",
            "托管协议流程审批": "ct_fund_custody_agmt",
            "其他流程": "ct_agreement_other",
            "项目合规性审查": "ct_project_base_info",
            "基金出资记录": "ct_invest_record",
            "项目退出": "ct_fund_quit_record",
            "会议管理审批流程": "ct_meeting_manage",
            "业务审批": "ct_fund_meet_manage",
            "基金公示审核": "ct_fund_publicity_review",
            "股权直投业务审批": "ct_project_meet_manage",
            "股权直投，其他协议": "ct_project_agreement_other",
        }
        for k, v in fmap.items():
            fm = get_flow_entity_map(k)
            curr_entity = fm.get("target_entity") or get_target_entity(v) or ""
            curr_table = fm.get("source_table") or v
            key_ent = f"flow_entity_custom_{k}"
            key_src = f"flow_source_custom_{k}"
            curr_link_entity = (str(st.session_state.get(key_ent) or "").strip() or str(curr_entity or "").strip())
            curr_link_table = (str(st.session_state.get(key_src) or "").strip() or str(curr_table or "").strip())
            c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 1])
            with c1:
                st.text(k)
            with c2:
                link = f"?page=detail&table={curr_link_table}"
                if curr_link_entity:
                    link += f"&entity={curr_link_entity}"
                st.markdown(f'🗂️ <a href="{link}" target="_blank"><code>{curr_link_table}</code></a>', unsafe_allow_html=True)
            with c3:
                custom_src = st.text_input("source_table", value=curr_table, key=key_src)
            with c4:
                custom_ent = st.text_input("entity", value=curr_entity, key=key_ent)
            with c5:
                if st.button("保存", key=f"flow_entity_save_{k}"):
                    src_val = (str(st.session_state.get(key_src) or custom_src or "").strip())
                    ent_val = (str(st.session_state.get(key_ent) or custom_ent or "").strip())
                    if src_val or ent_val:
                        if ent_val:
                            save_table_mapping(src_val, ent_val, 0, "")
                        upsert_flow_entity_map(k, src_val, ent_val)
                        st.success("已保存流程映射")
                        st.rerun()

    with super_tabs[1]:
        st.subheader("表单转换入库")
        rows = _build_instance_rows()
        flow_names_inst = {r.get("flow_define_name","") for r in rows if r.get("flow_define_name")}
        flow_names_cfg = {x.get("flow_define_name","") for x in list_flow_entity_maps()}
        flow_names = sorted({s for s in (flow_names_inst | flow_names_cfg) if s})
        flow_sel = st.selectbox("流程类型(flowDefineName)", options=flow_names or [""])
        def _match_flow(r):
            return str(r.get("flow_define_name","")) == str(flow_sel)
        view = [r for r in rows if _match_flow(r)]
        pids = [r.get("proc_inst_id") for r in view]
        idx_key = f"flow_pid_idx_{flow_sel}"
        if idx_key not in st.session_state:
            st.session_state[idx_key] = 0
        index = st.session_state[idx_key]
        nav = st.columns([1, 1, 3])
        with nav[0]:
            if st.button("⬅️ 上一条", key=f"flow_prev_{flow_sel}"):
                if index > 0:
                    st.session_state[idx_key] = index - 1
                    st.rerun()
        with nav[1]:
            if st.button("下一条 ➡️", key=f"flow_next_{flow_sel}"):
                if index + 1 < len(pids):
                    st.session_state[idx_key] = index + 1
                    st.rerun()
        with nav[2]:
            typed_pid = st.text_input("指定实例ID", value="", key=f"flow_pid_input_{flow_sel}")
        final_pid = (typed_pid or "").strip() or (pids[index] if (0 <= index < len(pids)) else "")
        if final_pid and st.button("生成模拟打印", key=f"mock_print_{final_pid}"):
            data = _build_instance_json(final_pid)
            rt_vars_list = data.get("runtime", {}).get("variables", [])
            hi_vars_list = data.get("history", {}).get("variables", [])
            def _var_map(vs):
                if isinstance(vs, dict):
                    return vs
                m = {}
                for v in vs or []:
                    if isinstance(v, dict):
                        n = str(v.get("name_",""))
                        if n:
                            m[n] = v.get("value","")
                return m
            rt_vars_map = _var_map(rt_vars_list)
            hi_vars_map = _var_map(hi_vars_list)
            biz_name = str(rt_vars_map.get("businessName") or hi_vars_map.get("businessName") or "")
            segs = data.get("segments", []) or []
            nodes = []
            for idx in range(len(segs)):
                seg = segs[idx]
                frm = seg.get("from", {})
                via = seg.get("via", []) or []
                to_ = seg.get("to", {})
                if idx == 0:
                    nodes.append({
                        "id": frm.get("key",""),
                        "type": frm.get("type",""),
                        "name": frm.get("name",""),
                        "assignee": frm.get("assignee",""),
                        "start": frm.get("start",""),
                        "end": frm.get("end",""),
                        "duration": frm.get("duration",""),
                        "next": {"to": to_.get("key",""), "via": via},
                    })
                next_obj = {}
                if idx + 1 < len(segs):
                    nxt = segs[idx + 1]
                    nxt_from = nxt.get("from", {})
                    if nxt_from.get("key") == to_.get("key"):
                        next_obj = {"to": nxt.get("to", {}).get("key",""), "via": nxt.get("via", []) or []}
                lc = seg.get("to_comment_last", {}) or {}
                nodes.append({
                    "id": to_.get("key",""),
                    "type": to_.get("type",""),
                    "name": to_.get("name",""),
                    "assignee": to_.get("assignee",""),
                    "start": to_.get("start",""),
                    "end": to_.get("end",""),
                    "duration": to_.get("duration",""),
                    "lastComment": {"time": lc.get("time",""), "userId": lc.get("user_id",""), "message": lc.get("message","")},
                    "task": seg.get("to_task", {}) or {},
                    "value": seg.get("to_values", {}) or {},
                    "actor_ids": seg.get("to_actor_ids", []) or [],
                    "next": next_obj,
                })
            nodes = _enrich_nodes_with_user(nodes)
            assignees = {}
            for seg in segs:
                k = seg.get("to", {}).get("key","")
                a = seg.get("to_task", {}).get("assignee_","")
                if k:
                    assignees[k] = a
            history_vars = {
                "processStatus": str(hi_vars_map.get("processStatus","")),
                "taskStatus": str(hi_vars_map.get("taskStatus") or hi_vars_map.get("TASK_STATUS") or ""),
                "taskReason": str(hi_vars_map.get("taskReason") or hi_vars_map.get("TASK_REASON") or ""),
                "nrOfInstances": str(hi_vars_map.get("nrOfInstances","")),
                "nrOfActiveInstances": str(hi_vars_map.get("nrOfActiveInstances","")),
                "nrOfCompletedInstances": str(hi_vars_map.get("nrOfCompletedInstances","")),
                "isSign": str(hi_vars_map.get("isSign","")),
                "assignees": assignees,
            }
            starter_code = str(data.get("starterUserId") or ((nodes[0] or {}).get("assignee") or ((nodes[0] or {}).get("task") or {}).get("assignee_") or "")).strip() if nodes else str(data.get("starterUserId") or "")
            preview_obj = {
                "meta": {
                    "businessName": biz_name,
                    "processName": data.get("defCode",""),
                    "flowDefineName": data.get("flow_define_name",""),
                    "startTime": data.get("startTime",""),
                    "endTime": data.get("endTime",""),
                    "icon": data.get("definition", {}).get("icon", ""),
                    "starterCode": starter_code,
                },
                "variables": {"runtime": {}, "history": history_vars},
                "nodes": nodes,
            }
            def _flow_table(flow_name: str):
                fm = get_flow_entity_map(flow_name)
                return fm.get("source_table") or {
                    "合伙协议": "ct_partner_agreement",
                    "募集协议审批流程": "ct_fund_base_info",
                    "托管协议流程审批": "ct_fund_custody_agmt",
                    "其他流程": "ct_agreement_other",
                    "项目合规性审查": "ct_project_base_info",
                    "基金出资记录": "ct_invest_record",
                    "项目退出": "ct_fund_quit_record",
                    "会议管理审批流程": "ct_meeting_manage",
                    "业务审批": "ct_fund_meet_manage",
                    "基金公示审核": "ct_fund_publicity_review",
                    "股权直投业务审批": "ct_project_meet_manage",
                    "股权直投，其他协议": "ct_project_agreement_other",
                }.get(flow_name)
            fields_obj = {}
            fdef = str(data.get("flow_define_name",""))
            tbl = _flow_table(fdef)
            entity = ""
            out_name = ""
            type_override = ""
            if tbl:
                recs = _parse_all_inserts(tbl)
                match = next((r for r in recs if str(r.get("process_instance_id","")) == str(final_pid)), None)
                if match:
                    fm = get_flow_entity_map(fdef)
                    entity = (fm.get("target_entity") or get_target_entity(tbl) or "")
                    script = get_table_script(tbl, entity or None) or ""
                    mapped, out_name, type_override = apply_record_mapping(tbl, match, script, target_entity=entity or "")
                    _ = _extract_entity_meta(mapped)
                    fields_obj = mapped or {}
            src = json.dumps(preview_obj, ensure_ascii=False)
            esc = src.replace("'", "''")
            if fields_obj is None:
                fields_obj = {}
            fields_obj["source_flow"] = esc
            try:
                raw = fields_obj.get("source_flow", "")
                parsed = json.loads(raw.replace("''", "'")) if raw else {}
            except Exception:
                parsed = preview_obj
            meta_info = parsed.get("meta", {}) or {}
            hist = (parsed.get("variables", {}) or {}).get("history", {}) or {}
            nodes_md = []
            def _fmt_time(v):
                if v in (None, ""):
                    return ""
                s = str(v).strip()
                try:
                    x = float(s)
                    ms = int(x) if x >= 1e11 else int(x * 1000)
                    from datetime import datetime
                    dt = datetime.fromtimestamp(ms / 1000.0)
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    try:
                        from datetime import datetime
                        t = s.replace("T", " ").replace("Z", "")
                        dt = datetime.fromisoformat(t)
                        return dt.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        return s
            def _fmt_duration_auto(v):
                if v in (None, ""):
                    return ""
                s = str(v).strip()
                try:
                    x = float(s)
                except Exception:
                    return s
                secs = x / 1000.0 if x >= 1000 else x
                secs = int(secs)
                d = secs // 86400; secs %= 86400
                h = secs // 3600; secs %= 3600
                m = secs // 60; secs %= 60
                parts = []
                if d: parts.append(f"{d} 天")
                if h: parts.append(f"{h} 小时")
                if m: parts.append(f"{m} 分钟")
                if secs and not parts:
                    parts.append(f"{secs} 秒")
                return " ".join(parts) or "0 秒"
            def _ts_num(v):
                s = str(v or "").strip()
                if not s:
                    return 0
                try:
                    x = float(s)
                    ms = int(x) if x >= 1e11 else int(x * 1000)
                    return ms
                except Exception:
                    try:
                        from datetime import datetime
                        t = s.replace("T", " ").replace("Z", "")
                        return int(datetime.fromisoformat(t).timestamp() * 1000)
                    except Exception:
                        return 0
            nds_sorted = []
            nodes_src = (parsed.get("nodes", []) or [])
            for nd in nodes_src:
                t = nd.get("task", {}) or {}
                name0 = (t.get("name_") or nd.get("name", "") or "").strip()
                if name0 in ("开始", "结束"):
                    continue
                nds_sorted.append(nd)
            if not nds_sorted and nodes_src:
                nds_sorted = nodes_src
            # 排序：从新到旧（开始时间优先，降序）
            nds_sorted.sort(key=lambda n: _ts_num((n.get("task") or {}).get("start_time_") or n.get("start", "") or (n.get("task") or {}).get("end_time_") or n.get("end", "")), reverse=True)
            hist_tasks_list = (data.get("history", {}) or {}).get("tasks", []) or []
            present_ids = {str(((nd.get("task") or {}).get("id_")) or "").strip() for nd in nds_sorted}
            extra_nodes = []
            cm_map = ((data.get("history", {}) or {}).get("comments_by_task", {}) or {})
            for ht in hist_tasks_list:
                tid = str(ht.get("id_") or "").strip()
                if not tid or tid in present_ids:
                    continue
                extra_nodes.append({
                    "id": ht.get("task_id_", ""),
                    "type": "userTask",
                    "name": ht.get("name_", ""),
                    "assignee": ht.get("assignee_", ""),
                    "start": ht.get("start_time_", ""),
                    "end": ht.get("end_time_", ""),
                    "duration": ht.get("duration_", ""),
                    "lastComment": cm_map.get(tid, {}) or {},
                    "task": {
                        "id_": ht.get("id_", ""),
                        "parent_task_id_": ht.get("parent_task_id_", ""),
                        "name_": ht.get("name_", ""),
                        "assignee_": ht.get("assignee_", ""),
                        "owner_": ht.get("owner_", ""),
                        "start_time_": ht.get("start_time_", ""),
                        "end_time_": ht.get("end_time_", ""),
                        "duration_": ht.get("duration_", ""),
                        "priority_": ht.get("priority_", ""),
                        "category_": ht.get("category_", ""),
                        "delete_reason_": ht.get("delete_reason_", ""),
                    },
                    "value": {},
                    "actor_ids": [str(ht.get("assignee_", ""))],
                    "next": {},
                })
            if extra_nodes:
                nds_sorted.extend(extra_nodes)
                nds_sorted = _enrich_nodes_with_user(nds_sorted)
                nds_sorted.sort(key=lambda n: _ts_num((n.get("task") or {}).get("start_time_") or n.get("start", "") or (n.get("task") or {}).get("end_time_") or n.get("end", "")), reverse=True)
            # 父子任务展示：优先展示父任务，再展示其子任务（单行状态）
            task_map = {}
            for nd in nds_sorted:
                t0 = nd.get("task") or {}
                tid0 = str(t0.get("id_") or "").strip()
                if tid0:
                    task_map[tid0] = nd
            from collections import defaultdict
            children_map = defaultdict(list)
            for nd in nds_sorted:
                t0 = nd.get("task") or {}
                p0 = str(t0.get("parent_task_id_") or "").strip()
                if p0:
                    children_map[p0].append(nd)
            visited = set()
            import re
            def _split_msg(s: str):
                s0 = (s or '').strip()
                inline_extra = ''
                suggest = ''
                parts = re.split(r"[，,]?\s*(?:理由为|原因是)\s*[:：]", s0)
                if len(parts) >= 2:
                    inline_extra = (parts[0] or '').strip().rstrip('，。')
                    suggest = (parts[1] or '').strip().rstrip('，。')
                    return inline_extra, suggest
                suggest = s0
                return inline_extra, suggest
            def _fmt_block(nd: Dict[str, Any], label_child: bool = False):
                t = nd.get("task", {}) or {}
                lc = nd.get("lastComment", {}) or {}
                rawm = (str(lc.get('message') or '') + ' ' + str(t.get('delete_reason_') or '')).lower()
                mk = '⚪'
                for kw in ['同意','通过','批准','审核通过']:
                    if kw in rawm:
                        mk = '🟢'
                        break
                if mk == '⚪':
                    for kw in ['驳回','退回','拒绝','不通过','不同意']:
                        if kw in rawm:
                            mk = '🔴'
                            break
                task_name = (t.get('name_') or nd.get('name','') or '').strip()
                assignee = (t.get('assignee_') or nd.get('assignee','') or '').strip()
                start_txt = _fmt_time(t.get('start_time_') or nd.get('start',''))
                end_txt = _fmt_time(t.get('end_time_') or nd.get('end',''))
                dur_text = _fmt_duration_auto(t.get('duration_')) or _fmt_duration_auto(nd.get('duration'))
                msg = (lc.get('message') or '').strip()
                inline_extra, suggest_text = _split_msg(msg)
                if (not any([assignee, start_txt, end_txt, (dur_text or ''), msg])) and (task_name in ('结束','')):
                    return []
                status_text = ("审批通过" if mk=='🟢' else ("审批未通过" if mk=='🔴' else ""))
                if (not str(meta_info.get('endTime','')).strip()) and mk == '⚪':
                    status_text = "审批中"
                header = (f"**审批任务：{task_name} {mk}{(inline_extra or status_text)}**" if not label_child
                          else f"**{task_name}→子任务 {mk}{(inline_extra or status_text)}**")
                out = [header, ""]
                av = str(nd.get("assignee_val") or "").strip()
                dp = str(nd.get("dept") or "").strip()
                disp = (f"{av}（{dp}）" if av and dp else (av or assignee))
                if disp:
                    out.append(f"审批人：{disp}")
                    out.append("")
                line = []
                if start_txt:
                    line.append(f"创建时间：{start_txt}")
                if end_txt:
                    line.append(f"审批时间： {end_txt}")
                if dur_text:
                    line.append(f"耗时： {dur_text}")
                if line:
                    out.append(" ".join(line))
                    out.append("")
                out.append(f"审批建议：{suggest_text}" if suggest_text else "审批建议：")
                out.append("")
                return out
            for nd in nds_sorted:
                t = nd.get("task", {}) or {}
                tid = str(t.get("id_") or "").strip()
                if not tid or tid in visited:
                    continue
                parent_id = str(t.get("parent_task_id_") or "").strip()
                if parent_id:
                    pnd = task_map.get(parent_id)
                    if pnd and str((pnd.get('task') or {}).get('id_') or '').strip() not in visited:
                        nodes_md.extend(_fmt_block(pnd, label_child=False))
                        visited.add(str((pnd.get('task') or {}).get('id_') or '').strip())
                    nodes_md.extend(_fmt_block(nd, label_child=True))
                    visited.add(tid)
                    continue
                nodes_md.extend(_fmt_block(nd, label_child=False))
                visited.add(tid)
                for ch in children_map.get(tid, []):
                    ctid = str((ch.get('task') or {}).get('id_') or '').strip()
                    if ctid and ctid not in visited:
                        nodes_md.extend(_fmt_block(ch, label_child=True))
                        visited.add(ctid)
            if not nodes_md:
                for tsk in (data.get("runtime", {}) or {}).get("tasks", []) or []:
                    name_rt = (tsk.get("name_", "") or "").strip()
                    assignee_rt = (tsk.get("assignee_", "") or "").strip()
                    start_txt_rt = _fmt_time(tsk.get("create_time_"))
                    nodes_md.append(f"**审批任务：{name_rt}**")
                    nodes_md.append("⚪审批中")
                    nodes_md.append("")
                    disp_rt = assignee_rt
                    if disp_rt:
                        nodes_md.append(f"审批人：{disp_rt}")
                        nodes_md.append("")
                    line_rt = []
                    if start_txt_rt:
                        line_rt.append(f"创建时间：{start_txt_rt}")
                    if line_rt:
                        nodes_md.append(" ".join(line_rt))
                        nodes_md.append("")
                    nodes_md.append("审批建议：")
                    nodes_md.append("")
            hs_raw = str(hist.get('taskStatus','')).strip()
            code_map = {
                '0':'待审批','1':'审批中','2':'审批通过','3':'审批不通过','4':'已取消','5':'已回退','6':'委派中','7':'审批通过中','8':'自动抄送'
            }
            concl = code_map.get(hs_raw)
            if not concl:
                hs = hs_raw.lower()
                hmk = ''
                for kw in ['通过','同意','批准','审核通过']:
                    if kw in hs:
                        hmk = '审核通过'
                        break
                if not hmk:
                    for kw in ['驳回','拒绝','不通过','不同意']:
                        if kw in hs:
                            hmk = '审核未通过'
                            break
                concl = '审批通过' if hmk=='审核通过' else ('审批未通过' if hmk=='审核未通过' else hs_raw)
            ended_raw = meta_info.get('endTime','')
            ended_flag = bool(str(ended_raw).strip())
            head_icon = '🟢' if concl in ('审批通过','审批通过中') else ('🔴' if concl in ('审批未通过','审批不通过') else '⚪')
            header1 = f"**结束流程：在 {_fmt_time(ended_raw)} 结束**"
            header2 = f"{head_icon} {concl}"
            nds = parsed.get("nodes", []) or []
            umap, _ = _user_dept_maps()
            scode = str(meta_info.get("starterCode") or "").strip()
            sname = (umap.get(scode) or {}).get("name", "")
            starter = sname or (str(nds[0].get("assignee_val") or ((nds[0].get("task") or {}).get("assignee_") or nds[0].get("assignee") or "")).strip() if nds else "")
            flow_name = str(meta_info.get("flowDefineName") or meta_info.get("processName") or "").strip()
            start_md = f"**发起流程：【{starter}】在 {_fmt_time(meta_info.get('startTime',''))} 发起【 {flow_name} 】流程**"
            flow_md = "\n".join(([header1, header2, ""] if ended_flag else []) + nodes_md + ["", start_md]).strip()
            fields_obj["flow_md"] = flow_md
            meta = _extract_entity_meta(fields_obj)
            entity_obj = {
                "uuid": "(mock uuid)",
                "sid": st.session_state.get("current_sid", SID),
                "type": type_override or (entity or tbl or ""),
                "name": out_name or "",
                "del": int(meta.get("del", 0)),
                "input_date": int(meta.get("input_date", 0)),
                "update_date": int(meta.get("update_date", 0)),
                "data": fields_obj,
            }
            st.code(json.dumps(entity_obj, ensure_ascii=False, indent=2))
            md = str(fields_obj.get("flow_md", "")).strip()
            if md:
                st.markdown(md)

        write_mode = st.selectbox(
            "写入模式",
            options=["合并写入（默认）", "仅保存 source_flow/flow_md 覆盖"],
            index=0,
            key=f"flow_write_mode_{flow_sel}"
        )
        if typed_pid and st.button("入库当前", key=f"import_{final_pid}"):
            bundle = _build_flow_import_bundle(final_pid)
            fields_obj = bundle.get("fields_obj") or {}
            flow_md = bundle.get("flow_md") or ""
            meta = bundle.get("meta") or {}
            type_name = bundle.get("type_name") or ""
            key_field = bundle.get("key_field") or "id"
            key_val = bundle.get("key_val") or ""
            final_name = bundle.get("final_name") or ""
            used_match = bundle.get("match")
            if write_mode == "仅保存 source_flow/flow_md 覆盖":
                key_val = (used_match or {}).get("id") or key_val or str(final_pid or "")
                if fields_obj.get("id") in (None, "") and key_val:
                    fields_obj["id"] = key_val
                cover_obj = {
                    key_field: key_val,
                    "name": fields_obj.get("name", ""),
                    "bt": fields_obj.get("bt", ""),
                    "type": fields_obj.get("type", type_name),
                    "source_flow": fields_obj.get("source_flow",""),
                    "flow_md": flow_md
                }
                data_json = json.dumps(cover_obj, ensure_ascii=False)
                import_mode = "upsert_replace"
                sid = st.session_state.get("current_sid", SID)
                wrote = _upsert_entity_row(type_name, key_field, key_val, sid, final_name, data_json, meta, import_mode=import_mode)
                st.success(f"入库完成：写入 {wrote} 条")
            else:
                if not key_val:
                    key_val = str(final_pid or "")
                    if key_val:
                        fields_obj["id"] = key_val
                data_json = json.dumps(fields_obj, ensure_ascii=False)
                import_mode = "upsert"
                sid = st.session_state.get("current_sid", SID)
                wrote = _upsert_entity_row(type_name, key_field, key_val, sid, final_name, data_json, meta, import_mode=import_mode)
                st.success(f"入库完成：写入 {wrote} 条")
            st.stop()

        elif st.button("批量入库当前流程全部", key=f"import_all_{flow_sel}"):
            rows = _build_instance_rows()
            def _match_flow(r):
                return str(r.get("flow_define_name","")) == str(flow_sel)
            view = [r for r in rows if _match_flow(r)]
            pids_all = [r.get("proc_inst_id") for r in view]
            pg = st.progress(0)
            total = len(pids_all)
            wrote_sum = 0
            for i, pid0 in enumerate(pids_all or []):
                pg.progress(int(((i) / (total or 1)) * 100))
                if not pid0:
                    continue
                bundle = _build_flow_import_bundle(pid0)
                fields_obj = bundle.get("fields_obj") or {}
                flow_md = bundle.get("flow_md") or ""
                meta = bundle.get("meta") or {}
                type_name = bundle.get("type_name") or ""
                key_field = bundle.get("key_field") or "id"
                key_val = bundle.get("key_val") or ""
                final_name = bundle.get("final_name") or ""
                used_match = bundle.get("match")
                if write_mode == "仅保存 source_flow/flow_md 覆盖":
                    if not key_val:
                        key_val = str(pid0 or "")
                        if key_val:
                            fields_obj["id"] = key_val
                    cover_obj = {
                        key_field: key_val,
                        "name": fields_obj.get("name", ""),
                        "bt": fields_obj.get("bt", ""),
                        "type": fields_obj.get("type", type_name),
                        "source_flow": fields_obj.get("source_flow",""),
                        "flow_md": flow_md
                    }
                    data_json = json.dumps(cover_obj, ensure_ascii=False)
                    import_mode = "upsert_replace"
                else:
                    if not key_val:
                        key_val = str(pid0 or "")
                        if key_val:
                            fields_obj["id"] = key_val
                    data_json = json.dumps(fields_obj, ensure_ascii=False)
                    import_mode = "upsert"
                sid = st.session_state.get("current_sid", SID)
                wrote = _upsert_entity_row(type_name, key_field, key_val, sid, final_name, data_json, meta, import_mode=import_mode)
                wrote_sum += int(wrote or 0)
            pg.progress(100)
            st.success(f"批量入库完成：写入 {wrote_sum} 条")

    with super_tabs[2]:
        tabs = st.tabs(["实例预览(JSON)", "流程定义", "表单库", "分类", "表达式库", "监听器库", "实例抄送", "用户组", "实例总览", "全部实例"]) 

        # 实例预览（JSON）
        with tabs[0]:
            st.subheader("按流程实例聚合（JSON 预览）")
            # 选择实例来源于历史实例表
            hi = _read_sql_rows("act_hi_procinst")
            all_pids = [r.get("id_","") for r in hi if r.get("id_")]
            kw = st.text_input("关键词（实例ID/业务键）", key="json_kw")
            def _match_pid(r):
                s = (kw or "").strip().lower()
                return (not s) or s in str(r.get("id_","")) .lower() or s in str(r.get("business_key_","")) .lower()
            view_pids = [r.get("id_","") for r in hi if _match_pid(r)]
            pid = st.selectbox("选择实例ID", options=view_pids or all_pids, index=0 if (view_pids or all_pids) else None, key="json_pid")
            if pid:
                data = _build_instance_json(pid)
                st.json(data)
                st.download_button("下载 JSON", data=json.dumps(data, ensure_ascii=False, indent=2), file_name=f"procinst_{pid}.json", mime="application/json")

        # 流程定义
        with tabs[1]:
            kw = st.text_input("关键词（定义ID/模型ID/描述）", key="pd_kw")
            recs = _parse_all_inserts("bpm_process_definition_info")
            def _code_of(pd_id: str):
                s = str(pd_id or "")
                return s.split(":")[0] if ":" in s else s
            for r in recs:
                r["_code"] = _code_of(r.get("process_definition_id"))
            code = st.text_input("按分类编码过滤（例如 ContractApproval）", key="pd_code")
            def _match(r):
                def _has(s):
                    return (kw or "").strip().lower() in str(s or "").lower()
                ok_kw = (not kw) or _has(r.get("process_definition_id")) or _has(r.get("model_id")) or _has(r.get("description"))
                ok_code = (not code) or (str(r.get("_code","")) == code)
                return ok_kw and ok_code
            view = [r for r in recs if _match(r)]
            cols = ["process_definition_id", "model_id", "description", "form_type", "form_id", "_code"]
            st.dataframe([{k: v for k, v in r.items() if k in cols} for r in view], use_container_width=True)

        # 表单库
        with tabs[2]:
            kw = st.text_input("关键词（表单名/备注）", key="form_kw")
            recs = _parse_all_inserts("bpm_form")
            def _match(r):
                s1 = str(r.get("name",""))
                s2 = str(r.get("remark",""))
                return (not kw) or (kw.lower() in s1.lower() or kw.lower() in s2.lower())
            view = [r for r in recs if _match(r)]
            cols = ["id","name","status","remark"]
            st.dataframe([{k: v for k, v in r.items() if k in cols} for r in view], use_container_width=True)

        # 分类
        with tabs[3]:
            recs = _parse_all_inserts("bpm_category")
            cols = ["id","name","code","status","sort"]
            st.dataframe([{k: v for k, v in r.items() if k in cols} for r in recs], use_container_width=True)

        # 表达式库
        with tabs[4]:
            kw = st.text_input("关键词（表达式名/内容）", key="expr_kw")
            recs = _parse_all_inserts("bpm_process_expression")
            def _match(r):
                s1 = str(r.get("name",""))
                s2 = str(r.get("expression",""))
                return (not kw) or (kw.lower() in s1.lower() or kw.lower() in s2.lower())
            view = [r for r in recs if _match(r)]
            cols = ["id","name","status","expression"]
            st.dataframe([{k: v for k, v in r.items() if k in cols} for r in view], use_container_width=True)

        # 监听器库
        with tabs[5]:
            kw = st.text_input("关键词（监听器名/事件/值）", key="lst_kw")
            recs = _parse_all_inserts("bpm_process_listener")
            def _match(r):
                s1 = str(r.get("name",""))
                s2 = str(r.get("event",""))
                s3 = str(r.get("value",""))
                return (not kw) or (kw.lower() in s1.lower() or kw.lower() in s2.lower() or kw.lower() in s3.lower())
            view = [r for r in recs if _match(r)]
            cols = ["id","name","type","status","event","value_type","value"]
            st.dataframe([{k: v for k, v in r.items() if k in cols} for r in view], use_container_width=True)

        # 实例抄送
        with tabs[6]:
            kw = st.text_input("关键词（实例ID/任务ID/名称）", key="copy_kw")
            recs = _parse_all_inserts("bpm_process_instance_copy")
            def _match(r):
                s1 = str(r.get("process_instance_id",""))
                s2 = str(r.get("task_id",""))
                s3 = str(r.get("task_name",""))
                return (not kw) or (kw.lower() in s1.lower() or kw.lower() in s2.lower() or kw.lower() in s3.lower())
            view = [r for r in recs if _match(r)]
            cols = ["id","user_id","start_user_id","process_instance_id","process_instance_name","task_id","task_name","category"]
            st.dataframe([{k: v for k, v in r.items() if k in cols} for r in view], use_container_width=True)

        # 用户组
        with tabs[7]:
            recs = _parse_all_inserts("bpm_user_group")
            cols = ["id","name","description","user_ids","status"]
            st.dataframe([{k: v for k, v in r.items() if k in cols} for r in recs], use_container_width=True)

        # 实例总览（按 process_instance_id 聚合）
        with tabs[8]:
            recs = _read_sql_rows("bpm_process_instance_copy")
            if not recs:
                st.info("暂无实例数据。")
            else:
                from collections import defaultdict
                groups = defaultdict(list)
                for r in recs:
                    pid = str(r.get("process_instance_id","")).strip()
                    if pid:
                        groups[pid].append(r)
                rows = []
                for pid, items in groups.items():
                    name = next((x.get("process_instance_name") for x in items if x.get("process_instance_name")), "")
                    users = sorted({x.get("user_id") for x in items if x.get("user_id")})
                    starters = sorted({x.get("start_user_id") for x in items if x.get("start_user_id")})
                    tasks = sorted({x.get("task_id") for x in items if x.get("task_id")})
                    cats = sorted({x.get("category") for x in items if x.get("category")})
                    ctimes = [x.get("create_time") for x in items if x.get("create_time")]
                    utimes = [x.get("update_time") for x in items if x.get("update_time")]
                    rows.append({
                        "process_instance_id": pid,
                        "process_instance_name": name,
                        "copies": len(items),
                        "users": ",".join(map(str, users)),
                        "starters": ",".join(map(str, starters)),
                        "task_count": len(tasks),
                        "categories": ",".join(map(str, cats)),
                        "first_create_time": min(ctimes) if ctimes else "",
                        "last_update_time": max(utimes) if utimes else "",
                    })
                st.dataframe(rows, use_container_width=True)

        # 全部实例（运行时 + 历史）
        with tabs[9]:
            st.subheader("历史实例")
            hi = _read_sql_rows("act_hi_procinst")
            hist_cols = ["id_","proc_def_id_","start_time_","end_time_","business_key_"]
            st.dataframe(_pick_cols(hi, hist_cols), use_container_width=True)

            st.subheader("运行时：执行树")
            ru_exec = _read_sql_rows("act_ru_execution")
            exec_cols = ["id_","proc_inst_id_","parent_id_","super_exec_","act_id_","is_active_","is_concurrent_","is_scope_"]
            st.dataframe(_pick_cols(ru_exec, exec_cols), use_container_width=True)

            st.subheader("运行时：任务")
            ru_task = _read_sql_rows("act_ru_task")
            task_cols = ["id_","proc_inst_id_","name_","assignee_","owner_","create_time_","due_date_","category_","priority_"]
            st.dataframe(_pick_cols(ru_task, task_cols), use_container_width=True)

            st.subheader("运行时：变量")
            ru_var = _read_sql_rows("act_ru_variable")
            var_cols = ["id_","proc_inst_id_","execution_id_","name_","text_","double_","long_"]
            st.dataframe(_pick_cols(ru_var, var_cols), use_container_width=True)

        # 流程实例（综合）
        with tabs[9]:
            kw = st.text_input("关键词（实例ID/业务键/定义编码）", key="inst_kw")
            code_filter = st.text_input("按定义编码过滤（如 ContractApproval）", key="inst_code")
            rows = _build_instance_rows()
            def _match(r):
                s = (kw or "").strip().lower()
                ok_kw = (not s) or s in str(r.get("proc_inst_id","")).lower() or s in str(r.get("business_key","")).lower() or s in str(r.get("def_code","")).lower()
                ok_code = (not code_filter) or str(r.get("def_code","")) == code_filter
                return ok_kw and ok_code
            view = [r for r in rows if _match(r)]
            cols = [
                "proc_inst_id","proc_def_id","def_code","category","business_key","start_time","end_time",
                "open_task_count","open_task_names","open_assignees","current_activities",
                "hist_task_count","hist_act_count","copy_count","copy_users","def_desc","form_type","form_id","vars"
            ]
            st.dataframe([{k: v for k, v in r.items() if k in cols} for r in view], use_container_width=True)

            # 详情抽屉
            inst_ids = [r.get("proc_inst_id") for r in view]
            if inst_ids:
                sel = st.selectbox("选择实例ID查看详情", options=inst_ids, index=0, key="inst_sel")
                if sel:
                    st.markdown("---")
                    st.subheader("实例详情")
                    # 运行时任务
                    st.markdown("**运行时任务**")
                    ru_task = _read_sql_rows("act_ru_task")
                    task_cols = ["id_","proc_inst_id_","name_","assignee_","owner_","create_time_","due_date_","category_","priority_"]
                    task_detail = [r for r in ru_task if str(r.get("proc_inst_id_","")) == str(sel)]
                    st.dataframe(_pick_cols(task_detail, task_cols), use_container_width=True)

                    # 运行时执行树
                    st.markdown("**运行时执行树**")
                    ru_exec = _read_sql_rows("act_ru_execution")
                    exec_cols = ["id_","proc_inst_id_","parent_id_","super_exec_","act_id_","is_active_","is_concurrent_","is_scope_"]
                    exec_detail = [r for r in ru_exec if str(r.get("proc_inst_id_","")) == str(sel)]
                    st.dataframe(_pick_cols(exec_detail, exec_cols), use_container_width=True)

                    # 历史节点轨迹
                    st.markdown("**历史节点轨迹（act_hi_actinst）**")
                    hi_act = _read_sql_rows("act_hi_actinst")
                    hact_cols = ["id_","proc_inst_id_","act_id_","act_name_","start_time_","end_time_","assignee_","task_id_"]
                    hact_detail = [r for r in hi_act if str(r.get("proc_inst_id_","")) == str(sel)]
                    st.dataframe(_pick_cols(hact_detail, hact_cols), use_container_width=True)

                    # 变量全部键值
                    st.markdown("**变量（全部）**")
                    ru_var = _read_sql_rows("act_ru_variable")
                    def _val(v):
                        return v.get("text_") or v.get("double_") or v.get("long_") or ""
                    var_detail = [r for r in ru_var if str(r.get("proc_inst_id_","")) == str(sel)]
                    var_rows = [{"name_": v.get("name_",""), "value": _val(v), "execution_id_": v.get("execution_id_",""), "id_": v.get("id_","")} for v in var_detail]
                    st.dataframe(var_rows, use_container_width=True)

                    # 表单预览（绑定 bpm_process_definition_info → bpm_form）
                    st.markdown("**表单预览**")
                    hi = _read_sql_rows("act_hi_procinst")
                    curr = next((r for r in hi if str(r.get("id_","")) == str(sel)), None)
                    def _code_of(def_id):
                        s = str(def_id or "")
                        return s.split(":")[0] if ":" in s else s
                    if curr:
                        def_id = curr.get("proc_def_id_","")
                        code = _code_of(def_id)
                        def_info = _read_sql_rows("bpm_process_definition_info")
                        di = next((d for d in def_info if _code_of(d.get("process_definition_id")) == code), None)
                        if di:
                            st.text(f"定义描述：{di.get('description','')}")
                            st.text(f"表单类型：{di.get('form_type','')} 表单ID：{di.get('form_id','')}")
                            form_type = str(di.get("form_type",""))
                            if form_type == "10" and di.get("form_id"):
                                forms = _read_sql_rows("bpm_form")
                                fi = next((f for f in forms if str(f.get("id","")) == str(di.get("form_id"))), None)
                                if fi:
                                    st.text(f"表单名称：{fi.get('name','')} 状态：{fi.get('status','')}")
                                    st.text(f"备注：{fi.get('remark','')}")
                                    st.text(f"字段：{fi.get('fields','')}")
                                else:
                                    st.info("未找到对应的公共表单记录")
                            else:
                                st.text(f"定义内置字段：{di.get('form_fields','')}")
                                st.text(f"定义内置配置：{di.get('form_conf','')}")
                        else:
                            st.info("未找到对应的流程定义扩展记录")
            else:
                st.info("暂无匹配的实例。")

def render_file_mgmt():
    st.title("📃 文件管理")
    render_top_tabs('file')
    st.info("文件管理：在此统一管理文件映射规则，并可预览映射效果。")

    st.subheader("文件映射管理")
    tabs = st.tabs(["映射列表", "新增映射", "预览解析示例"])

    with tabs[0]:
        kw = st.text_input("按源表过滤", key="file_map_kw")
        all_maps = list_file_mappings()
        view = [m for m in all_maps if (not kw or kw.strip() in (m.get("source_table") or ""))]
        st.dataframe(view, use_container_width=True)
        del_id = st.number_input("删除映射ID", value=0, step=1, key="file_map_del_id")
        if st.button("删除", key="file_map_del_btn"):
            if int(del_id) > 0:
                ok = delete_file_mapping(int(del_id))
                if ok:
                    st.success("已删除映射")
                    st.rerun()
                else:
                    st.error("删除失败")

    with tabs[1]:
        st.caption("根据指引：entity_field 与 (doc_uuid, doc_name) 不能同时存在")
        src_tbl = st.selectbox("source_table", options=[r[0] for r in list_tables(include_disabled=True)], key="file_map_src_tbl")
        src_field = st.text_input("source_field", key="file_map_src_field")
        entity = st.selectbox("entity", options=[x.get("target_entity") or "" for x in list_mapped_tables()] + [""], key="file_map_entity")
        entity_field = st.text_input("entity_field", key="file_map_entity_field")
        doc_uuid = st.text_input("doc_uuid", key="file_map_doc_uuid")
        doc_name = st.text_input("doc_name", key="file_map_doc_name")
        desc = st.text_input("备注", key="file_map_desc")
        order_idx = st.number_input("排序", value=0, step=1, key="file_map_order")
        enabled = st.checkbox("启用", value=True, key="file_map_enabled")
        if st.button("保存映射", key="file_map_save"):
            ef = (entity_field or "").strip()
            du, dn = (doc_uuid or "").strip(), (doc_name or "").strip()
            if ef and (du or dn):
                st.error("entity_field 与 doc_uuid/doc_name 不能同时填写")
            elif not src_tbl or not src_field or not entity:
                st.error("请填写 source_table、source_field、entity")
            else:
                ok = upsert_file_mapping(src_tbl, src_field, entity, ef, du, dn, desc, int(enabled), int(order_idx))
                if ok:
                    st.success("已保存映射")
                    st.rerun()
                else:
                    st.error("保存失败")

    with tabs[2]:
        st.caption("从源 SQL 的文本字段解析 文件名@URL 列表，展示解析与分发示例")
        demo_tbl = st.selectbox("选择源表", options=[r[0] for r in list_tables(include_disabled=True)], key="file_map_demo_tbl")
        rows = _read_sql_rows(demo_tbl)
        cols = st.text_input("打印字段（逗号分隔）", value="need,upload_files", key="file_map_demo_cols")
        pick = [c.strip() for c in cols.split(",") if c.strip()]
        st.json(_pick_cols(rows[:10], pick))
        st.caption("解析规则：支持 '文件名@URL'，多个以逗号分隔；URL 中可解析日期片段作为存储路径维度")


def render_user_dept_mgmt():
    st.title("👥 用户部门管理")
    render_top_tabs('user_dept')
    kw = st.text_input("关键词（姓名/ID/部门）", key="user_dept_kw")
    only_missing = st.checkbox("仅看缺失部门", value=False, key="user_dept_missing")
    rows = _parse_all_inserts("sys_user")
    umap, dmap = _user_dept_maps()
    data = []
    for r in rows:
        uid = str(r.get("user_id") or "").strip()
        name = str(r.get("nick_name") or "").strip()
        did = str(r.get("dept_id") or "").strip()
        dname = dmap.get(did, "")
        item = {"user_id": uid, "nick_name": name, "dept_id": did, "dept_name": dname}
        if kw:
            s = kw.strip().lower()
            if not (s in uid.lower() or s in name.lower() or s in did.lower() or s in dname.lower()):
                continue
        if only_missing and dname:
            continue
        data.append(item)
    st.dataframe(data, use_container_width=True)
    cols = st.columns([1,1,6])
    with cols[0]:
        if st.button("🔄 刷新映射", key="user_dept_refresh"):
            global _USER_MAP, _USER_NAME_MAP, _DEPT_MAP
            _USER_MAP = None
            _USER_NAME_MAP = None
            _DEPT_MAP = None
            _user_dept_maps()
            st.rerun()


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
    elif st.session_state.page == "flow":
        render_flow_mgmt()
    elif st.session_state.page == "user_dept":
        render_user_dept_mgmt()
    elif st.session_state.page == "file":
        render_file_mgmt()
    elif st.session_state.page == "home":
        render_table_list()
    else:
        render_table_detail(st.session_state.current_table)
if __name__ == "__main__":
    main()
