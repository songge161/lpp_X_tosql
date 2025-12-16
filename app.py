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
    get_flow_entity_map, upsert_flow_entity_map, list_flow_entity_maps
)
from backend.source_fields import detect_source_fields, detect_sql_path,detect_field_comments, detect_table_title
from backend.mapper_core import apply_record_mapping, check_entity_status, import_table_data, delete_table_data, clear_sql_cache, _parse_sql_file, _extract_entity_meta, _upsert_entity_row
from backend.sql_utils import update_runtime_db, current_cfg
from backend.presets import init_presets_db, list_presets, save_preset, delete_preset, get_last_runtime, save_last_runtime

try:
    from version3 import SID, uuid as gen_uuid10
except Exception:
    SID = "default_sid"
    import time, random, os
    def gen_uuid10():
        t = int(time.time()) % 1000
        us = int(time.time() * 1e6) % 100
        pid = os.getpid() % 100
        rnd = random.randint(0, 46655)
        n = (t << 24) | (us << 16) | (pid << 8) | (rnd & 0xFF)
        chars = "0123456789abcdefghijklmnopqrstuvwxyz"
        s = ""; x = n
        while x:
            x, r = divmod(x, 36)
            s = chars[r] + s
        s = (s or "0") + "".join(chars[random.randint(0,35)] for _ in range(3))
        return s[-10:].rjust(10, "0")

st.set_page_config(page_title="表映射管理工具", layout="wide")
init_db()
init_presets_db()

# =============== 侧边栏：数据库与 SID 选择 ===============
if "source_input_kind" not in st.session_state:
    st.session_state.source_input_kind = "file"
if "source_db_cfg" not in st.session_state:
    st.session_state.source_db_cfg = {
        "host": "127.0.0.1",
        "port": 5432,
        "user": "postgres",
        "password": "",
        "database": "postgres",
        "schema": "public",
    }
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

from backend.presets import get_last_source
_last_src = get_last_source()
if _last_src:
    st.session_state.source_db_cfg = {
        "host": _last_src.get("host", st.session_state.source_db_cfg.get("host")),
        "port": (int(_last_src.get("port")) if _last_src.get("port") is not None else st.session_state.source_db_cfg.get("port")),
        "user": _last_src.get("user", st.session_state.source_db_cfg.get("user")),
        "password": _last_src.get("password", st.session_state.source_db_cfg.get("password")),
        "database": _last_src.get("database", st.session_state.source_db_cfg.get("database")),
        "schema": _last_src.get("schema", st.session_state.source_db_cfg.get("schema")),
    }
    st.session_state.source_input_kind = ("db" if str(_last_src.get("kind","")) == "pg" else "file")
    try:
        from backend.sql_utils import update_source_db
        update_source_db("pg", st.session_state.source_db_cfg)
    except Exception:
        pass

with st.sidebar:
    st.header("源库选择")
    _src_opts = ["本地文件", "接入数据库（pgsql）"]
    _src_sel = st.radio("源数据来源", options=_src_opts, index=(0 if st.session_state.get("source_input_kind") != "db" else 1))
    st.session_state.source_input_kind = ("db" if _src_sel == "接入数据库（pgsql）" else "file")
    try:
        from backend.presets import save_last_source
        if st.session_state.source_input_kind == "db":
            from backend.sql_utils import update_source_db
            update_source_db("pg", st.session_state.source_db_cfg)
            save_last_source("pg", st.session_state.source_db_cfg)
        else:
            save_last_source("file", {})
    except Exception:
        pass
    if st.session_state.source_input_kind == "db":
        from backend.sql_utils import get_source_conn
        try:
            c = get_source_conn()
            try:
                c.close()
            finally:
                pass
            cfg = st.session_state.source_db_cfg
            st.caption(f"源库连接：{cfg.get('host')}:{cfg.get('port')}/{cfg.get('database')}@{cfg.get('schema')} 已连接")
        except Exception as e:
            st.error(f"源库连接失败：{e}")
    if st.session_state.source_input_kind == "db":
        from backend.presets import list_src_presets, save_src_preset, delete_src_preset, save_last_source
        st.subheader("源库连接（pgsql）")
        src_presets = list_src_presets()
        if src_presets:
            for p in src_presets:
                label = f"{(p.get('name') or '').strip()}-{(p.get('schema') or '').strip()}"
                cols_row = st.columns([4,1,1])
                with cols_row[0]:
                    if st.button(label or "(未命名)", key=f"src_preset_select_{p.get('name','')}"):
                        st.session_state["selected_src_preset_name"] = p.get("name")
                        st.session_state["selected_src_preset_label"] = label or p.get("name")
                with cols_row[1]:
                    if st.button("❌", key=f"src_preset_del_{p.get('name','')}"):
                        try:
                            delete_src_preset(p.get("name"))
                            st.success("已删除源库预设")
                            st.rerun()
                        except Exception as e:
                            st.error(f"删除失败：{e}")
                with cols_row[2]:
                    if st.button("🧹缓存", key=f"src_preset_cache_{p.get('name','')}"):
                        try:
                            from backend.db import clear_access_cache
                            conn_key = f"pg|{str(p.get('host') or '')}|{str(p.get('port') or '')}|{str(p.get('database') or '')}|{str(p.get('schema') or '')}"
                            n = clear_access_cache("db", conn_key)
                            st.success(f"已清除该源库缓存 {n} 项")
                        except Exception as e:
                            st.error(f"清除失败：{e}")
            if st.session_state.get("selected_src_preset_label"):
                st.caption(f"已选中：{st.session_state.get('selected_src_preset_label')}")
        else:
            st.info("暂无源库预设，请点击下方『添加』进行创建")
        ctrl_cols2 = st.columns([1,1])
        with ctrl_cols2[0]:
            if st.button("添加源库"):
                st.session_state["show_add_src_panel"] = True
        with ctrl_cols2[1]:
            if st.button("应用源库"):
                sel_name = st.session_state.get("selected_src_preset_name")
                if not sel_name:
                    st.warning("请先选择一个源库预设。")
                else:
                    src_presets = list_src_presets()
                    target = next((x for x in src_presets if x.get("name") == sel_name), None)
                    if not target:
                        st.warning("选中的源库预设不存在。")
                    else:
                        st.session_state.source_db_cfg = {
                            "host": target.get("host") or st.session_state.source_db_cfg.get("host"),
                            "port": int(target.get("port") or st.session_state.source_db_cfg.get("port")),
                            "user": target.get("user") or st.session_state.source_db_cfg.get("user"),
                            "password": target.get("password") or st.session_state.source_db_cfg.get("password"),
                            "database": target.get("database") or st.session_state.source_db_cfg.get("database"),
                            "schema": target.get("schema") or st.session_state.source_db_cfg.get("schema"),
                        }
                        try:
                            from backend.sql_utils import update_source_db
                            update_source_db("pg", st.session_state.source_db_cfg)
                            save_last_source("pg", st.session_state.source_db_cfg)
                            st.success("已应用源库预设")
                        except Exception as e:
                            st.error(f"应用失败：{e}")
                        st.rerun()
        if st.session_state.get("show_add_src_panel"):
            with st.form("add_src_preset_form"):
                st.subheader("添加源库连接")
                preset_name = st.text_input("名称", value="")
                host_inp = st.text_input("主机", value=st.session_state.source_db_cfg.get("host", "127.0.0.1"))
                port_inp = st.number_input("端口", value=int(st.session_state.source_db_cfg.get("port", 5432)), step=1)
                user_inp = st.text_input("用户", value=st.session_state.source_db_cfg.get("user", "postgres"))
                pwd_inp = st.text_input("密码", value=st.session_state.source_db_cfg.get("password", ""))
                db_inp = st.text_input("库/数据库", value=st.session_state.source_db_cfg.get("database", "postgres"))
                schema_inp = st.text_input("空间(schema)", value=st.session_state.source_db_cfg.get("schema", "public"))
                c1,c2 = st.columns([1,1])
                with c1:
                    do_save = st.form_submit_button("保存")
                with c2:
                    do_cancel = st.form_submit_button("取消")
                if do_cancel:
                    st.session_state["show_add_src_panel"] = False
                    st.rerun()
                if do_save:
                    name_norm = (preset_name or "").strip()
                    if not name_norm:
                        st.warning("请填写名称。")
                    elif not db_inp:
                        st.warning("请填写库/数据库名称。")
                    else:
                        try:
                            save_src_preset(name_norm, "pg", host_inp, int(port_inp or 0), user_inp, pwd_inp, db_inp, schema_inp)
                            st.session_state["show_add_src_panel"] = False
                            st.session_state["selected_src_preset_name"] = name_norm
                            st.session_state["selected_src_preset_label"] = f"{name_norm}-{(schema_inp or '').strip()}"
                            st.success("预设已保存")
                            st.rerun()
                        except Exception as e:
                            st.error(f"保存失败：{e}")
        
    st.header("库/空间目标")
    st.caption("列表：名称-sid（删除：❌）；支持添加与应用")
    try:
        from backend.sql_utils import get_conn
        cc = get_conn()
        try:
            cc.close()
        finally:
            pass
        cfg = st.session_state.db_cfg
        st.caption(f"目标库连接：{cfg.get('host')}:{cfg.get('port')}/{cfg.get('database')}@{cfg.get('schema')} 已连接")
    except Exception as e:
        st.error(f"目标库连接失败：{e}")

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
    if st.session_state.get("source_input_kind") == "db":
        from backend.sql_utils import get_source_conn
        try:
            conn = get_source_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM {table}")
                    cols = [d[0] for d in cur.description] if cur.description else []
                    rows = cur.fetchall() or []
                    try:
                        from backend.db import set_access_cache
                        cfg2 = st.session_state.source_db_cfg
                        conn_key2 = f"pg|{str(cfg2.get('host') or '')}|{str(cfg2.get('port') or '')}|{str(cfg2.get('database') or '')}|{str(cfg2.get('schema') or '')}"
                        set_access_cache("db", conn_key2, table, [dict(zip(cols, r)) for r in rows])
                    except Exception:
                        pass
                    return [dict(zip(cols, r)) for r in rows]
            finally:
                conn.close()
        except Exception as e:
            st.error(f"源库连接失败：{e}")
            return []
    p = detect_sql_path(table)
    if not p.exists():
        return []
    rows = _parse_sql_file(p)
    try:
        from backend.db import set_access_cache
        set_access_cache("file", "local", table, rows)
    except Exception:
        pass
    return rows

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
    if st.session_state.get("source_input_kind") == "db":
        rows = _read_sql_rows(table_name)
        return rows
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
    try:
        start_raw = (
            meta_info.get("startTime", "")
            or data.get("startTime", "")
            or ((parsed.get("nodes") or [{}])[0].get("start", "") if (parsed.get("nodes") or []) else "")
        )
        stxt = _fmt_time(start_raw)
        fields_obj["sqsj"] = (stxt.split(" ")[0] if stxt else "")
    except Exception:
        fields_obj["sqsj"] = ""
    fields_obj["lcbh"] = str(pid or "")
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
        def _fmt_flow_opt(x: str) -> str:
            if x == "全部" or not x:
                return x
            fm = get_flow_entity_map(x)
            tgt = fm.get("target_entity") or "-"
            src = fm.get("source_table") or "-"
            return f"{x}（目标:{tgt} 源表:{src}）"
        flow_filter = st.selectbox("按流程名称过滤（flowDefineName）", options=["全部"] + flow_names, index=0, key="form_conv_flowname", format_func=_fmt_flow_opt)
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
        def _fmt_flow_opt2(x: str) -> str:
            if not x:
                return x
            fm = get_flow_entity_map(x)
            tgt = fm.get("target_entity") or "-"
            src = fm.get("source_table") or "-"
            return f"{x}（目标:{tgt} 源表:{src}）"
        flow_sel = st.selectbox("流程类型(flowDefineName)", options=flow_names or [""], format_func=_fmt_flow_opt2)
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
                    "flow_md": flow_md,
                    "lcbh": fields_obj.get("lcbh", ""),
                    "sqsj": fields_obj.get("sqsj", "")
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
                        "flow_md": flow_md,
                        "lcbh": fields_obj.get("lcbh", ""),
                        "sqsj": fields_obj.get("sqsj", "")
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

        elif st.button("批量入库源表流程数据", key=f"import_src_flow_{flow_sel}"):
            fm = get_flow_entity_map(flow_sel)
            tbl = fm.get("source_table") or ""
            tgt_entity = fm.get("target_entity") or ""
            if not tbl:
                st.warning("未配置该流程对应的源表")
            else:
                rows_src = _read_sql_rows(tbl)
                pg = st.progress(0)
                total = len(rows_src)
                wrote_sum = 0
                for i, r in enumerate(rows_src or []):
                    pg.progress(int(((i) / (total or 1)) * 100))
                    script = get_table_script(tbl, tgt_entity or None) or ""
                    mapped, out_name, type_override = apply_record_mapping(tbl, r, script, target_entity=tgt_entity or "")
                    meta = _extract_entity_meta(mapped)
                    type_name = (type_override or tgt_entity or tbl or flow_sel or "flow_instance")
                    key_field = "id"
                    key_val = mapped.get("id") or r.get("id") or str(r.get("process_instance_id") or "")
                    final_name = mapped.get("__name__", "") or out_name or mapped.get("name", "")
                    pid0 = str(r.get("process_instance_id") or "")
                    if pid0:
                        data_bundle = _build_flow_import_bundle(pid0)
                        used_match = data_bundle.get("match")
                        flow_md = data_bundle.get("flow_md") or ""
                        fld = data_bundle.get("fields_obj") or {}
                        if used_match:
                            merged_obj = dict(mapped)
                            merged_obj["source_flow"] = fld.get("source_flow", "")
                            merged_obj["flow_md"] = flow_md
                            merged_obj["lcbh"] = (fld.get("lcbh", "") or pid0)
                            merged_obj["sqsj"] = fld.get("sqsj", "")
                            data_json = json.dumps(merged_obj, ensure_ascii=False)
                            import_mode = "upsert"
                        else:
                            mapped["lcbh"] = (fld.get("lcbh", "") or pid0)
                            mapped["sqsj"] = fld.get("sqsj", "")
                            data_json = json.dumps(mapped, ensure_ascii=False)
                            import_mode = "upsert"
                    else:
                        data_json = json.dumps(mapped, ensure_ascii=False)
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
    sid = st.session_state.get("current_sid", SID)
    st.subheader("文件映射管理")
    cols = st.columns([2,2,2,2])
    mapped_rows = list_mapped_tables()
    opts = []
    for r in mapped_rows:
        cnt = check_entity_status(r.get("target_entity",""), sid=sid)
        if cnt > 0:
            opts.append({"src": r.get("source_table",""), "ent": r.get("target_entity",""), "cnt": cnt})
    labels = [f"{o['src']} ({o['ent']},{o['cnt']})" for o in opts] or [p.stem for p in Path("source/sql").glob("*.sql")]
    with cols[0]:
        sel_label = st.selectbox("source_table", options=labels, index=0 if labels else None)
        src_table = (sel_label.split(" ")[0] if labels else sel_label)
    with cols[1]:
        src_field = st.text_input("source_field", value="upload_files")
    with cols[2]:
        # 从标签解析实体类型（严格取括号内第一个逗号前的内容）
        def_ent = "fund"
        try:
            if "(" in sel_label and ")" in sel_label:
                inner = sel_label.split("(", 1)[1].split(")", 1)[0]
                def_ent = inner.split(",", 1)[0].strip()
        except Exception:
            def_ent = next((o["ent"] for o in opts if sel_label.startswith(o["src"])), "fund") if labels else "fund"
        entity_type = st.text_input("entity", value=def_ent, key=f"filemap_entity_{sel_label}")
    with cols[3]:
        sql_field = st.text_input("sql_field", value="id")
    match_cols = st.columns([1,1])
    with match_cols[0]:
        match_entity_field = st.text_input("匹配的entity_field", value="id")
    with match_cols[1]:
        st.caption("匹配规则：entity.data[匹配的entity_field] == 记录[sql_field]")
    mode = st.radio("写入模式", options=["数据字段", "文档目录"], index=0, horizontal=True)
    ef_col = st.columns([2,2])
    if mode == "数据字段":
        with ef_col[0]:
            entity_field = st.text_input("entity_field", value="fjsc")
        doc_uuid = ""
        doc_name = ""
    else:
        with ef_col[0]:
            doc_uuid = st.text_input("doc_uuid", value="")
        with ef_col[1]:
            doc_name = st.text_input("doc_name", value="")
        entity_field = ""
    def _parse_files(val: str):
        items = []
        for part in [x.strip() for x in str(val or "").split(",") if x.strip()]:
            raw_name, raw_url = (part.split("@", 1) + [""])[:2]
            url = raw_url.strip()
            fname = ""
            if url:
                tail = url.rsplit("/", 1)[-1].strip()
                fname = tail or ""
            if not fname:
                fname = raw_name.strip() or "unnamed"
            if "." in fname:
                base = ".".join(fname.split(".")[:-1])
                ext = fname.split(".")[-1]
            else:
                base, ext = fname, ""
            m = re.search(r"/defaultFile/(\d{8})/([^/]+)/", url)
            ymd = (m.group(1) if m else "")
            token = (m.group(2) if m else "")
            yyyymm = (ymd[:6] if ymd else time.strftime("%Y%m"))
            items.append({"name": base, "url": url, "yyyymm": yyyymm, "ext": ext, "yyyymmdd": ymd, "token": token})
        return items
    def _infra_match_row(it):
        try:
            rows = _read_sql_rows("infra_file")
        except Exception:
            rows = []
        full_name = str(it.get("name","")) + (f".{it.get('ext','')}" if it.get("ext") else "")
        url = str(it.get("url",""))
        ymd = str(it.get("yyyymmdd",""))
        token = str(it.get("token",""))
        r = next((x for x in rows if str(x.get("url","")) == url), None)
        if r:
            return r
        if ymd:
            if token:
                r = next((x for x in rows if str(x.get("name","")) == full_name and str(x.get("path","")) .startswith(f"defaultFile/{ymd}/") and token in str(x.get("path",""))), None)
                if r:
                    return r
            r = next((x for x in rows if str(x.get("name","")) == full_name and str(x.get("path","")) .startswith(f"defaultFile/{ymd}/")), None)
            if r:
                return r
        r = next((x for x in rows if str(x.get("name","")) == full_name), None)
        return r or {}
    def _get_upload_root():
        try:
            v = st.session_state.get("upload_root")
            if v:
                return str(v).strip()
        except Exception:
            pass
        try:
            from pathlib import Path
            txt = Path("outer_packet/config_data.json").read_text(encoding="utf-8")
            cfg = json.loads(txt) if txt else {}
            if isinstance(cfg, dict):
                v = cfg.get("upload_root") or ""
                if v:
                    return str(v).strip()
        except Exception:
            pass
        return "/Users/songyihong/PEPM/lpp/upload/files"
    def _set_upload_root(v: str):
        p = str(v or "").strip()
        st.session_state["upload_root"] = p
        try:
            from pathlib import Path
            txt = Path("outer_packet/config_data.json").read_text(encoding="utf-8")
            cfg = json.loads(txt) if txt else {}
        except Exception:
            cfg = {}
        if not isinstance(cfg, dict):
            cfg = {}
        cfg["upload_root"] = p
        try:
            from pathlib import Path
            Path("outer_packet/config_data.json").write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
    def _build_file_row(eid: str, it: Dict[str, Any], fuid: str):
        rel = f"../upload/files/{sid}/{it['yyyymm']}/{fuid}/{fuid}"
        src_info = it.get("source_info") or {}
        data = json.dumps({"path": None, "source_id": src_info.get("id"), "source_url": src_info.get("url"), "source_path": src_info.get("path")}, ensure_ascii=False)
        def _resolve_config_uid() -> str:
            if mode != "文档目录":
                return "root"
            cid = ((doc_uuid or "").strip() or "mahndrn6w7")
            name = (doc_name or "").strip()
            if not name:
                return "root"
            uid_val = "root"
            try:
                from backend.sql_utils import get_conn
                conn = get_conn()
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT data FROM entity WHERE uuid=%s LIMIT 1", (cid,))
                        row = cur.fetchone()
                        if row and row[0]:
                            try:
                                cfg = json.loads(row[0])
                            except Exception:
                                cfg = {}
                            items = {}
                            if isinstance(cfg, dict):
                                items = cfg.get("item") or (cfg.get("data") or {}).get("item") or {}
                            if isinstance(items, dict):
                                for key, meta in items.items():
                                    nm = str((meta or {}).get("name", "")).strip()
                                    if nm == name:
                                        uid_val = str((meta or {}).get("key") or (meta or {}).get("id") or key)
                                        break
                finally:
                    conn.close()
            except Exception:
                pass
            if uid_val == "root":
                try:
                    from pathlib import Path
                    txt = Path("outer_packet/config_data.json").read_text(encoding="utf-8")
                    cfg2 = json.loads(txt)
                    items2 = (cfg2.get("item") or {}) if isinstance(cfg2, dict) else {}
                    for key, meta in items2.items():
                        nm = str((meta or {}).get("name", "")).strip()
                        if nm == name:
                            uid_val = str((meta or {}).get("key") or (meta or {}).get("id") or key)
                            break
                except Exception:
                    pass
            return uid_val
        uid = _resolve_config_uid()
        cid_val = (None if mode=="数据字段" else ((doc_uuid or "").strip() or "mahndrn6w7"))
        path = json.dumps({"config": {"cid": cid_val, "uid": uid}, "eid": eid}, ensure_ascii=False)
        quote = json.dumps({"mod": {eid: ([entity_field] if entity_field else [])}}, ensure_ascii=False) if mode=="数据字段" else ""
        now_ts = int(time.time())
        ext = it.get("ext", "")
        def _infra_find_by_filename(full_name: str):
            try:
                rows = _read_sql_rows("infra_file")
                for r in rows:
                    if str(r.get("name","")) == full_name:
                        return r
            except Exception:
                return None
            return None
        def _usr_uuid_by_source_id(src_id: str):
            from backend.sql_utils import get_conn, json_equals_clause
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT uuid FROM usr WHERE {json_equals_clause('data','source_id')} LIMIT 1",
                        (str(src_id or ""),)
                    )
                    row = cur.fetchone()
                    return (row[0] if row else None)
            except Exception:
                return None
            finally:
                conn.close()
        full_name = it.get("name","") + (f".{ext}" if ext else "")
        infra = _infra_find_by_filename(full_name)
        c_uuid = None
        u_uuid = None
        usr_update_epoch = None
        if infra:
            c_uuid = _usr_uuid_by_source_id(infra.get("creator"))
            u_uuid = _usr_uuid_by_source_id(infra.get("updater"))
            ts = str(infra.get("update_time") or "")
            try:
                from datetime import datetime
                usr_update_epoch = int(datetime.strptime(ts.split(".")[0], "%Y-%m-%d %H:%M:%S").timestamp())
            except Exception:
                usr_update_epoch = None
        return {
            "uuid": fuid, "name": it["name"], "file": rel, "doc_type": entity_type,
            "flag": "", "size": 0, "data": data, "path": path, "quote": quote,
            "sid": sid, "eid": eid, "type": ext, "uid": uid,
            "create_people": (c_uuid or "root"), "create_date": now_ts, "update_people": (u_uuid or "root"), "update_date": (usr_update_epoch or now_ts),
            "del": 0, "state": 0, "filecrypt": 0, "oss": 0, "privilege": ""
            , "_usr_update_epoch": usr_update_epoch
        }
    def _save_local_file(it: Dict[str, Any], fuid: str):
        import os, shutil
        base = Path(_get_upload_root()) / sid / it["yyyymm"] / fuid
        base.mkdir(parents=True, exist_ok=True)
        ext = it.get("ext", "")
        dst_path = base / fuid
        size = 0
        try:
            src_row = _infra_match_row(it)
            it["source_info"] = {"id": src_row.get("id"), "url": src_row.get("url"), "path": src_row.get("path"), "size": src_row.get("size", 0)}
            expect_size = int((it["source_info"] or {}).get("size") or _infra_expect_size(it.get("name",""), ext) or 0)
            src_path = _find_local_file(it.get("name",""), ext, expect_size)
            if src_path:
                shutil.copyfile(src_path, dst_path)
                size = os.path.getsize(dst_path)
            else:
                open(dst_path, "wb").close()
        except Exception:
            try:
                open(dst_path, "wb").close()
            except Exception:
                pass
        return int(size)
    def _write_files(rows: list):
        from backend.sql_utils import get_conn
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute(
                        """
                        INSERT INTO file (uuid,name,file,doc_type,flag,size,data,path,quote,sid,eid,type,uid,create_people,create_date,update_people,update_date,del,state,filecrypt,oss,privilege)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            r["uuid"], r["name"], r["file"], r["doc_type"], r["flag"], int(r["size"]), r["data"], r["path"], r["quote"],
                            r["sid"], r["eid"], r["type"], r["uid"], r["create_people"], int(r["create_date"]), r["update_people"], int(r["update_date"]),
                            int(r["del"]), int(r["state"]), int(r["filecrypt"]), int(r["oss"]), r["privilege"]
                        )
                    )
            conn.commit()
            return len(rows)
        except Exception as e:
            conn.rollback(); st.error(f"写入失败：{e}")
            return 0
        finally:
            conn.close()
    def _update_entity_files(eid: str, files: list):
        from backend.mapper_core import update_entity_data_by_uuid
        names = [((x.get("name") + ("." + x.get("type") if x.get("type") else ""))) for x in files]
        uids = [x["uuid"] for x in files]
        patch = {}
        if entity_field:
            patch[f"{entity_field}_upload"] = " ".join(names)
            patch[f"{entity_field}_label"] = names
            patch[f"{entity_field}"] = uids
        return update_entity_data_by_uuid(eid, patch)
    _LOCAL_FILE_INDEX = {}
    def _infra_expect_size(name: str, ext: str) -> int:
        try:
            full_name = str(name or "") + (f".{ext}" if ext else "")
            rows = _read_sql_rows("infra_file")
            for r in rows or []:
                if str(r.get("name","")) == full_name:
                    try:
                        return int(r.get("size") or 0)
                    except Exception:
                        return 0
        except Exception:
            return 0
        return 0
    def _find_local_file(name: str, ext: str, expected_size: int = 0) -> str:
        from pathlib import Path
        import os
        key = f"{name}.{ext}" if ext else name
        cache_key = f"{key}::{expected_size or 0}"
        if cache_key in _LOCAL_FILE_INDEX:
            return _LOCAL_FILE_INDEX[cache_key]
        base = Path("source/files")
        try:
            candidates = list(base.rglob(key))
            if expected_size and candidates:
                for p in candidates:
                    try:
                        if os.path.getsize(str(p)) == int(expected_size):
                            _LOCAL_FILE_INDEX[cache_key] = str(p.resolve())
                            return _LOCAL_FILE_INDEX[cache_key]
                    except Exception:
                        continue
            if candidates:
                _LOCAL_FILE_INDEX[cache_key] = str(candidates[0].resolve())
                return _LOCAL_FILE_INDEX[cache_key]
        except Exception:
            pass
        _LOCAL_FILE_INDEX[cache_key] = ""
        return ""
    def _show_file_preview(items: list):
        if not items:
            return
        rows = []
        for it in items:
            name = it.get("name") or ""
            ext = it.get("ext") or ""
            local_path = _find_local_file(name, ext)
            file_url = f"file://{local_path}" if local_path else ""
            rows.append({"name": name, "type": ext, "file_url": file_url})
        md = "| 文件名 | 类型 | 本地文件 |\n|---|---|---|\n"
        for r in rows:
            link = f"[{r['name']}]({r['file_url']})" if r["file_url"] else r["name"]
            md += f"| {r['name']} | {r['type']} | {link} |\n"
        st.markdown(md)
    rec_cols = st.columns([2,1,1,1])
    with rec_cols[0]:
        rec_id = st.text_input("记录ID（仅处理该记录）", value="")
    upload_default = _get_upload_root()
    upload_root_input = st.text_input("文件写入根路径", value=upload_default, key="upload_root_input")
    if st.button("保存写入路径", key="save_upload_root"):
        _set_upload_root(upload_root_input or upload_default)
        st.success("写入路径已保存")
    with rec_cols[1]:
        preview_btn = st.button("解析预览", key="file_map_preview")
    with rec_cols[2]:
        write_preview_btn = st.button("写入预览", key="file_map_write_preview")
    with rec_cols[3]:
        apply_btn = st.button("一键写入", key="file_map_apply")
    apply_by_entity_btn = st.button("按实体写入", key="file_map_apply_by_entity")
    save_cfg_btn = st.button("保存映射配置", key="file_map_save")
    if save_cfg_btn:
        cfg = {
            "source_table": src_table,
            "source_field": src_field,
            "entity": entity_type,
            "mode": mode,
            "entity_field": entity_field,
            "doc_uuid": (doc_uuid if mode=="文档目录" else None),
            "doc_name": (doc_name if mode=="文档目录" else None),
            "sql_field": sql_field,
            "match_entity_field": match_entity_field,
            "saved_at": int(time.time()),
            "status": "未入库",
        }
        from backend.db import upsert_file_map_cfg
        cfg_id = upsert_file_map_cfg(cfg)
        st.success(f"已保存到文件入库管理（配置ID：{cfg_id}）")

    if preview_btn:
        rows = _read_sql_rows(src_table)
        view = rows
        if rec_id:
            view = [r for r in rows if str(r.get("id","")) == str(rec_id)]
        show = []
        from custom_handler import fetch_field_uuid
        for r in view[:10]:
            key_val = r.get(sql_field, "")
            e_uuid = fetch_field_uuid(entity_type, match_entity_field, key_val)
            vs = _parse_files(r.get(src_field, ""))
            show.append({"id": r.get("id"), "match_val": key_val, "entity_uuid": e_uuid, "count": len(vs), "first": (vs[0] if vs else {})})
        st.dataframe(show, use_container_width=True)
        try:
            prv = []
            for r in view[:3]:
                prv.extend(_parse_files(r.get(src_field, "")))
            _show_file_preview(prv[:12])
        except Exception:
            pass
    if write_preview_btn:
        rows = _read_sql_rows(src_table)
        view = rows if not rec_id else [r for r in rows if str(r.get("id","")) == str(rec_id)]
        if not view:
            st.warning("未找到对应记录")
        else:
            from custom_handler import fetch_field_uuid
            rows_preview = []
            prv_items = []
            for r in view:
                key_val = r.get(sql_field, "")
                e_uuid = fetch_field_uuid(entity_type, match_entity_field, key_val)
                src_val = str(r.get(src_field, "") or "")
                items = _parse_files(src_val)
                prv_items.extend(items)
                for it in items:
                    fuid = gen_uuid10()
                    if e_uuid:
                        src_row = _infra_match_row(it)
                        it["source_info"] = {"id": src_row.get("id"), "url": src_row.get("url"), "path": src_row.get("path"), "size": src_row.get("size", 0)}
                        file_row = _build_file_row(e_uuid, it, fuid)
                        expect_size = int((it["source_info"] or {}).get("size") or _infra_expect_size(file_row["name"], file_row["type"]) or 0)
                        local_path = _find_local_file(file_row["name"], file_row["type"], expect_size) 
                        file_url = f"file://{local_path}" if local_path else ""
                        rows_preview.append({
                            "record_id": r.get("id"),
                            "entity_uuid": e_uuid,
                            "uuid": file_row["uuid"],
                            "name": file_row["name"],
                            "type": file_row["type"],
                            "uid": file_row["uid"],
                            "path": file_row["path"],
                            "doc_type": file_row["doc_type"],
                            "file_url": file_url,
                        })
                    else:
                        src_row2 = _infra_match_row(it)
                        it["source_info"] = {"id": src_row2.get("id"), "url": src_row2.get("url"), "path": src_row2.get("path"), "size": src_row2.get("size", 0)}
                        expect_size2 = int((it["source_info"] or {}).get("size") or _infra_expect_size(it.get("name",""), it.get("ext","")) or 0)
                        local_path = _find_local_file(it.get("name",""), it.get("ext",""), expect_size2)
                        file_url = f"file://{local_path}" if local_path else ""
                        rows_preview.append({
                            "record_id": r.get("id"),
                            "entity_uuid": None,
                            "uuid": fuid,
                            "name": it.get("name"),
                            "type": it.get("ext"),
                            "uid": None,
                            "path": "",
                            "doc_type": entity_type,
                            "file_url": file_url,
                        })
            md = "| 记录ID | 实体UUID | 文件名 | 类型 | 目录UID | 本地文件 |\n|---|---|---|---|---|---|\n"
            for r in rows_preview[:200]:
                link = f"[{r.get('name','')}]({r.get('file_url','')})" if r.get("file_url") else r.get("name","")
                md += f"| {r.get('record_id','')} | {r.get('entity_uuid','')} | {r.get('name','')} | {r.get('type','')} | {r.get('uid','')} | {link} |\n"
            st.markdown(md)
            try:
                _show_file_preview(prv_items[:12])
            except Exception:
                pass
    if apply_btn:
        rows = _read_sql_rows(src_table)
        view = rows if not rec_id else [r for r in rows if str(r.get("id","")) == str(rec_id)]
        if not view:
            st.warning("未找到对应记录")
        else:
            from custom_handler import fetch_field_uuid
            all_files = []
            files_by_eid = {}
            total_items = 0
            for r in view:
                key_val = r.get(sql_field, "")
                entity_uuid = fetch_field_uuid(entity_type, match_entity_field, key_val)
                if not entity_uuid:
                    continue
                src_val = str(r.get(src_field, "") or "")
                items = _parse_files(src_val)
                total_items += len(items)
            prog = st.progress(0)
            done = 0
            for r in view:
                key_val = r.get(sql_field, "")
                entity_uuid = fetch_field_uuid(entity_type, match_entity_field, key_val)
                if not entity_uuid:
                    continue
                src_val = str(r.get(src_field, "") or "")
                items = _parse_files(src_val)
                for it in items:
                    fuid = gen_uuid10()
                    size = _save_local_file(it, fuid)
                    row = _build_file_row(entity_uuid, it, fuid)
                    row["size"] = size
                    all_files.append(row)
                    files_by_eid.setdefault(entity_uuid, []).append(row)
                    done += 1
                    if total_items:
                        prog.progress(min(1.0, done/total_items))
            if not all_files:
                st.warning("无可写入的文件（可能实体未匹配或源字段为空）")
            else:
                n1 = _write_files(all_files)
                try:
                    from backend.sql_utils import get_conn
                    conn = get_conn()
                    try:
                        with conn.cursor() as cur:
                            for r in all_files:
                                ts_epoch = r.get("_usr_update_epoch")
                                if not ts_epoch:
                                    continue
                                for k in ("create_people","update_people"):
                                    uu = r.get(k)
                                    if uu and uu != "root":
                                        try:
                                            cur.execute("UPDATE usr SET update_date=%s WHERE uuid=%s", (int(ts_epoch), str(uu)))
                                        except Exception:
                                            continue
                        conn.commit()
                    finally:
                        conn.close()
                except Exception:
                    pass
                n2_total = 0
                for eid, files in files_by_eid.items():
                    n2_total += _update_entity_files(eid, files)
                st.success(f"写入 file {n1} 条，更新 entity {n2_total} 条（本地路径：{_get_upload_root()}/{sid}/<YYYYMM>/<uuid>/）")
    if apply_by_entity_btn:
        from backend.sql_utils import get_conn, json_equals_clause
        rows = _read_sql_rows(src_table)
        idx = {}
        for r in rows:
            val = str(r.get(sql_field, "") or "")
            items = _parse_files(str(r.get(src_field, "") or ""))
            if not items:
                continue
            idx.setdefault(val, []).extend(items)
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                ents = []
                if rec_id:
                    cur.execute(
                        f"SELECT uuid,data FROM entity WHERE type=%s AND {json_equals_clause('data', match_entity_field)}",
                        (entity_type, str(rec_id))
                    )
                else:
                    cur.execute("SELECT uuid,data FROM entity WHERE type=%s", (entity_type,))
                rows_ent = cur.fetchall() or []
                for e in rows_ent:
                    try:
                        eu, dj = e[0], e[1]
                        ents.append({"uuid": eu, "data": dj})
                    except Exception:
                        continue
        finally:
            conn.close()
        if not ents:
            st.info("无匹配实体")
        else:
            all_files = []
            files_by_eid = {}
            total_items = 0
            for e in ents:
                try:
                    data = json.loads(e.get("data") or "{}")
                except Exception:
                    data = {}
                mv = str((data or {}).get(match_entity_field, "") or "")
                total_items += len(idx.get(mv, []))
            prog = st.progress(0)
            done = 0
            for e in ents:
                eid = e.get("uuid")
                try:
                    data = json.loads(e.get("data") or "{}")
                except Exception:
                    data = {}
                mv = str((data or {}).get(match_entity_field, "") or "")
                for it in idx.get(mv, []):
                    fuid = gen_uuid10()
                    size = _save_local_file(it, fuid)
                    row = _build_file_row(eid, it, fuid)
                    row["size"] = size
                    all_files.append(row)
                    files_by_eid.setdefault(eid, []).append(row)
                    done += 1
                    if total_items:
                        prog.progress(min(1.0, done/total_items))
            if not all_files:
                st.info("无可写入文件")
            else:
                n1 = _write_files(all_files)
                n2_total = 0
                for eid, files in files_by_eid.items():
                    n2_total += _update_entity_files(eid, files)
                st.success(f"按实体写入完成：file {n1} 条，更新 entity {n2_total} 条")
    if st.button("刷新", key="file_map_refresh"):
        st.rerun()

    act_row2 = st.columns([1,1])
    with act_row2[0]:
        del_preview_btn = st.button("预览删除", key="file_del_preview")
    with act_row2[1]:
        del_apply_btn = st.button("一键删除", key="file_del_apply")
    def _delete_entity_files(eid: str):
        from backend.sql_utils import get_conn
        import json, shutil
        from pathlib import Path
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT uuid,file,sid FROM file WHERE eid=%s", (eid,))
                rows = cur.fetchall() or []
                uuids = []
                paths = []
                for r in rows:
                    try:
                        fuid = r[0]
                        rel = r[1] or ""
                        uuids.append(str(fuid))
                        parts = str(rel).split("/")
                        if len(parts) >= 7 and parts[0] == ".." and parts[1] == "upload" and parts[2] == "files":
                            sid_v = parts[3]
                            yyyymm_v = parts[4]
                            fdir = parts[5]
                            base = Path(_get_upload_root()) / sid_v / yyyymm_v / fdir
                            paths.append(base)
                    except Exception:
                        continue
                for p in paths:
                    try:
                        shutil.rmtree(p, ignore_errors=True)
                    except Exception:
                        pass
                cur.execute("DELETE FROM file WHERE eid=%s", (eid,))
                cur.execute("SELECT data FROM entity WHERE uuid=%s", (eid,))
                erow = cur.fetchone()
                if erow:
                    now_ts = int(time.time())
                    try:
                        data = json.loads(erow[0] or "{}")
                    except Exception:
                        data = {}
                    keys = list(data.keys())
                    for k in keys:
                        v = data.get(k)
                        if k.endswith("_upload") or k.endswith("_label"):
                            data.pop(k, None)
                        elif isinstance(v, list) and all(isinstance(x, str) and len(x) == 10 for x in v):
                            data.pop(k, None)
                    cur.execute("UPDATE entity SET data=%s, update_date=%s WHERE uuid=%s", (json.dumps(data, ensure_ascii=False), now_ts, eid))
            conn.commit()
            return len(rows), len(paths)
        except Exception as e:
            conn.rollback(); st.error(f"删除失败：{e}")
            return 0, 0
        finally:
            conn.close()
    force_del_btn = st.button("强制删除（清理垃圾）", key="file_force_del")
    if force_del_btn:
        from backend.sql_utils import get_conn
        import shutil
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT uuid,file,sid,doc_type,quote FROM file WHERE doc_type=%s", (entity_type,))
                rows = cur.fetchall() or []
                paths = []
                uuids = []
                for r in rows:
                    try:
                        fuid = str(r[0])
                        rel = str(r[1] or "")
                        uuids.append(fuid)
                        parts = rel.split("/")
                        if len(parts) >= 7 and parts[0] == ".." and parts[1] == "upload" and parts[2] == "files":
                            sid_v = parts[3]
                            yyyymm_v = parts[4]
                            fdir = parts[5]
                            base = Path(_get_upload_root()) / sid_v / yyyymm_v / fdir
                            paths.append(base)
                    except Exception:
                        continue
                for p in paths:
                    try:
                        shutil.rmtree(p, ignore_errors=True)
                    except Exception:
                        pass
                cur.execute("DELETE FROM file WHERE doc_type=%s", (entity_type,))
                cur.execute("SELECT uuid,data FROM entity WHERE type=%s", (entity_type,))
                rows_ent = cur.fetchall() or []
                updated = 0
                for e in rows_ent:
                    try:
                        eu, dj = e[0], e[1]
                        try:
                            data = json.loads(dj or "{}")
                        except Exception:
                            data = {}
                        if entity_field:
                            data.pop(entity_field, None)
                            data.pop(f"{entity_field}_upload", None)
                            data.pop(f"{entity_field}_label", None)
                        cur.execute("UPDATE entity SET data=%s WHERE uuid=%s", (json.dumps(data, ensure_ascii=False), str(eu)))
                        updated += 1
                    except Exception:
                        continue
            conn.commit()
            st.success("已强制清理：删除 file 条目并清除实体字段映射")
        except Exception as e:
            conn.rollback(); st.error(f"强制删除失败：{e}")
        finally:
            conn.close()
    if del_preview_btn:
        rows = _read_sql_rows(src_table)
        view = rows if not rec_id else [r for r in rows if str(r.get("id","")) == str(rec_id)]
        if not view:
            st.warning("未找到对应记录")
        else:
            from custom_handler import fetch_field_uuid
            summary = []
            from backend.sql_utils import get_conn
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    for r in view:
                        key_val = r.get(sql_field, "")
                        e_uuid = fetch_field_uuid(entity_type, match_entity_field, key_val)
                        if not e_uuid:
                            continue
                        cur.execute("SELECT COUNT(1) FROM file WHERE eid=%s", (e_uuid,))
                        row = cur.fetchone()
                        c = row[0] if row else 0
                        summary.append({"record_id": r.get("id"), "entity_uuid": e_uuid, "count": c})
                if summary:
                    md = "| 记录ID | 实体UUID | 待删除条数 |\n|---|---|---|\n"
                    for s in summary:
                        md += f"| {s['record_id']} | {s['entity_uuid']} | {s['count']} |\n"
                    st.markdown(md)
                else:
                    st.info("无可删除的匹配实体")
            finally:
                conn.close()
    if del_apply_btn:
        rows = _read_sql_rows(src_table)
        view = rows if not rec_id else [r for r in rows if str(r.get("id","")) == str(rec_id)]
        if not view:
            st.warning("未找到对应记录")
        else:
            from custom_handler import fetch_field_uuid
            total_db = 0
            total_fs = 0
            for r in view:
                key_val = r.get(sql_field, "")
                e_uuid = fetch_field_uuid(entity_type, match_entity_field, key_val)
                if not e_uuid:
                    continue
                n_db, n_fs = _delete_entity_files(e_uuid)
                total_db += n_db
                total_fs += n_fs
            if total_db or total_fs:
                st.success(f"已删除 file {total_db} 条，并清理本地目录 {total_fs} 个；已清空匹配实体中的文件字段")
            else:
                st.info("无可删除的匹配实体")

    st.subheader("📦 文件入库管理")
    from backend.db import list_file_map_cfgs
    cfgs = list_file_map_cfgs()
    if not cfgs:
        st.info("暂无保存的映射配置，可在上方点击‘保存映射配置’后使用此区域进行入库或删除。")
    else:
        def _cfg_label(c: dict) -> str:
            base = f"{c['source_table']}.{c['source_field']} → {c['entity']}"
            ef = str(c.get('entity_field') or '').strip()
            if ef:
                base += f".{ef}"
            else:
                dn = str(c.get('doc_name') or '').strip()
                du = str(c.get('doc_uuid') or '').strip()
                if dn and du:
                    base += f"@{dn}（{du}）"
                elif dn:
                    base += f"@{dn}"
                elif du:
                    base += f"@{du}"
            tail = f"（{c['mode']}）"
            sqlf = str(c.get('sql_field') or '').strip()
            matchf = str(c.get('match_entity_field') or '').strip()
            etype = str(c.get('entity') or '').strip()
            if sqlf or matchf:
                tail += f"【sql.{sqlf or '-'}={etype}.data.{matchf or '-'}】"
            return base + tail
        options = [
            {
                "label": _cfg_label(c),
                "id": c["id"],
            }
            for c in cfgs
        ]
        labels = [o["label"] for o in options]
        sel = st.selectbox("选择入库配置", options=labels, index=0)
        sel_id = next((o["id"] for o in options if o["label"] == sel), cfgs[0]["id"]) if options else 0
        rec_id2 = st.text_input("记录ID（为空则全表）", key="file_imp_rec_id")
        row1 = st.columns([1,1,1,1,1])
        with row1[0]:
            do_prev = st.button("入库预览", key="file_imp_prev")
        with row1[1]:
            do_apply = st.button("执行入库", key="file_imp_apply")
        with row1[2]:
            do_del = st.button("删除文件", key="file_imp_del")
        with row1[3]:
            do_rm_cfg = st.button("删除配置", key="file_imp_rm_cfg")
        with row1[4]:
            do_apply_by_entity = st.button("按实体写入", key="file_imp_apply_by_entity")

        if do_prev or do_apply or do_del or do_apply_by_entity:
            cfg = next((c for c in cfgs if c["id"] == sel_id), cfgs[0])
            # 注入上下文供已有逻辑复用
            mode = cfg.get("mode")
            entity_field = cfg.get("entity_field") or ""
            doc_uuid = cfg.get("doc_uuid") or ""
            doc_name = cfg.get("doc_name") or ""
            src_table = cfg.get("source_table")
            src_field = cfg.get("source_field")
            entity_type = cfg.get("entity")
            sql_field = cfg.get("sql_field")
            match_entity_field = cfg.get("match_entity_field")
            rows = _read_sql_rows(src_table)
            view = rows if not rec_id2 else [r for r in rows if str(r.get("id","")) == str(rec_id2)]
            from custom_handler import fetch_field_uuid
            if do_prev:
                rows_preview = []
                prv_items = []
                for r in view:
                    key_val = r.get(sql_field, "")
                    e_uuid = fetch_field_uuid(entity_type, match_entity_field, key_val)
                    src_val = str(r.get(src_field, "") or "")
                    items = _parse_files(src_val)
                    prv_items.extend(items)
                    for it in items:
                        fuid = gen_uuid10()
                        file_row = _build_file_row(e_uuid or "", it, fuid)
                        file_url = ""
                        local_path = _find_local_file(file_row["name"], file_row["type"]) 
                        if local_path:
                            file_url = f"file://{local_path}"
                        rows_preview.append({
                            "record_id": r.get("id"),
                            "entity_uuid": e_uuid,
                            "uuid": file_row["uuid"],
                            "name": file_row["name"],
                            "type": file_row["type"],
                            "uid": file_row["uid"],
                            "path": file_row["path"],
                            "doc_type": file_row["doc_type"],
                            "file_url": file_url,
                        })
                md = "| 记录ID | 实体UUID | 文件名 | 类型 | 目录UID | 本地文件 |\n|---|---|---|---|---|---|\n"
                for r in rows_preview[:200]:
                    link = f"[{r.get('name','')}]({r.get('file_url','')})" if r.get("file_url") else r.get("name","")
                    md += f"| {r.get('record_id','')} | {r.get('entity_uuid','')} | {r.get('name','')} | {r.get('type','')} | {r.get('uid','')} | {link} |\n"
                st.markdown(md)
            if do_apply:
                all_files = []
                files_by_eid = {}
                total_items = 0
                for r in view:
                    src_val = str(r.get(src_field, "") or "")
                    total_items += len(_parse_files(src_val))
                prog = st.progress(0)
                done = 0
                for r in view:
                    key_val = r.get(sql_field, "")
                    e_uuid = fetch_field_uuid(entity_type, match_entity_field, key_val)
                    if not e_uuid:
                        continue
                    src_val = str(r.get(src_field, "") or "")
                    for it in _parse_files(src_val):
                        fuid = gen_uuid10()
                        size = _save_local_file(it, fuid)
                        row = _build_file_row(e_uuid, it, fuid)
                        row["size"] = size
                        all_files.append(row)
                        files_by_eid.setdefault(e_uuid, []).append(row)
                        done += 1
                        if total_items:
                            prog.progress(min(1.0, done/total_items))
                if not all_files:
                    st.info("无可写入文件")
                else:
                    n1 = _write_files(all_files)
                    n2_total = 0
                    for eid, files in files_by_eid.items():
                        n2_total += _update_entity_files(eid, files)
                    from backend.db import update_file_map_status
                    update_file_map_status(sel_id, f"已入库({n1})")
                    st.success(f"入库完成：file {n1} 条，更新 entity {n2_total} 条")
            if do_apply_by_entity:
                idx = {}
                for r in rows:
                    val = str(r.get(sql_field, "") or "")
                    items = _parse_files(str(r.get(src_field, "") or ""))
                    if not items:
                        continue
                    idx.setdefault(val, []).extend(items)
                from backend.sql_utils import get_conn, json_equals_clause
                conn = get_conn()
                ents = []
                try:
                    with conn.cursor() as cur:
                        if rec_id2:
                            cur.execute(
                                f"SELECT uuid,data FROM entity WHERE type=%s AND {json_equals_clause('data', match_entity_field)}",
                                (entity_type, str(rec_id2))
                            )
                        else:
                            cur.execute("SELECT uuid,data FROM entity WHERE type=%s", (entity_type,))
                        rows_ent = cur.fetchall() or []
                        for e in rows_ent:
                            try:
                                ents.append({"uuid": e[0], "data": e[1]})
                            except Exception:
                                continue
                finally:
                    conn.close()
                if not ents:
                    st.info("无匹配实体")
                else:
                    all_files = []
                    files_by_eid = {}
                    total_items = 0
                    for e in ents:
                        try:
                            data = json.loads(e.get("data") or "{}")
                        except Exception:
                            data = {}
                        mv = str((data or {}).get(match_entity_field, "") or "")
                        total_items += len(idx.get(mv, []))
                    prog = st.progress(0)
                    done = 0
                    for e in ents:
                        eid = e.get("uuid")
                        try:
                            data = json.loads(e.get("data") or "{}")
                        except Exception:
                            data = {}
                        mv = str((data or {}).get(match_entity_field, "") or "")
                        for it in idx.get(mv, []):
                            fuid = gen_uuid10()
                            size = _save_local_file(it, fuid)
                            row = _build_file_row(eid, it, fuid)
                            row["size"] = size
                            all_files.append(row)
                            files_by_eid.setdefault(eid, []).append(row)
                            done += 1
                            if total_items:
                                prog.progress(min(1.0, done/total_items))
                    if not all_files:
                        st.info("无可写入文件")
                    else:
                        n1 = _write_files(all_files)
                        n2_total = 0
                        for eid, files in files_by_eid.items():
                            n2_total += _update_entity_files(eid, files)
                        from backend.db import update_file_map_status
                        update_file_map_status(sel_id, f"已入库({n1})")
                        st.success(f"按实体写入完成：file {n1} 条，更新 entity {n2_total} 条")
            if do_del:
                total_db, total_fs = 0, 0
                for r in view:
                    key_val = r.get(sql_field, "")
                    e_uuid = fetch_field_uuid(entity_type, match_entity_field, key_val)
                    if not e_uuid:
                        continue
                    n_db, n_fs = _delete_entity_files(e_uuid)
                    total_db += n_db; total_fs += n_fs
                st.success(f"已删除 file {total_db} 条，清理目录 {total_fs} 个")
        if do_rm_cfg:
            from backend.db import delete_file_map_cfg_by_id
            ok = delete_file_map_cfg_by_id(sel_id)
            if ok:
                st.success("已删除配置")
                st.rerun()
            else:
                st.info("未找到配置")


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

    st.subheader("用户入库")
    imp_cols = st.columns([2,1,1,1,1,1])
    with imp_cols[0]:
        rec_id = st.text_input("记录ID（仅处理该记录）", key="usr_imp_rec_id")
    with imp_cols[1]:
        btn_preview = st.button("解析预览", key="usr_imp_preview")
    with imp_cols[2]:
        btn_write_preview = st.button("写入预览", key="usr_imp_write_preview")
    with imp_cols[3]:
        btn_apply = st.button("一键入库", key="usr_imp_apply")
    with imp_cols[4]:
        btn_del = st.button("一键删除", key="usr_imp_delete")
    with imp_cols[5]:
        btn_del_preview = st.button("预览删除", key="usr_imp_delete_preview")

    def _gen_uuid11():
        import random
        chars = "0123456789abcdefghijklmnopqrstuvwxyz"
        try:
            base = gen_uuid10()
        except Exception:
            base = "".join(random.choice(chars) for _ in range(10))
        return base + random.choice(chars)

    def _build_usr_row(sys_row: dict):
        return {
            "uuid": _gen_uuid11(),
            "email": str(sys_row.get("user_name") or ""),
            "pwd": "02b45e1b907d4dbe89d72c01fb5a5f5bd004f944",
            "tel": None,
            "name": str(sys_row.get("nick_name") or ""),
            "sid": SID,
            "data": json.dumps({"bool_c_p":0,"freeze_account":"否","pwd_error":0, "source_id": sys_row.get("user_id")}, ensure_ascii=False),
            "belong_space": 1,
        }

    def _select_sys_users():
        all_rows = _parse_all_inserts("sys_user")
        if rec_id:
            return [r for r in all_rows if str(r.get("user_id","")) == str(rec_id)]
        return all_rows

    def _write_usr(rows: list):
        from backend.sql_utils import get_conn
        conn = get_conn()
        wrote = 0
        try:
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute(
                        """
                        INSERT INTO usr (uuid,email,pwd,tel,name,sid,data,belong_space)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (r["uuid"], r["email"], r["pwd"], r["tel"], r["name"], r["sid"], r["data"], r["belong_space"])
                    )
                    wrote += 1
            conn.commit()
            return wrote
        except Exception as e:
            conn.rollback(); st.error(f"入库失败：{e}")
            return wrote
        finally:
            conn.close()

    def _delete_usr_by_sys_users():
        from backend.sql_utils import get_conn
        emails = [str(r.get("user_name") or "") for r in _select_sys_users()]
        emails = [e for e in emails if e]
        if not emails:
            return 0
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                # 仅删除属于 sys_user 映射的邮箱
                fmt = ",".join(["%s"]*len(emails))
                cur.execute(f"DELETE FROM usr WHERE email IN ({fmt})", emails)
            conn.commit()
            return len(emails)
        except Exception as e:
            conn.rollback(); st.error(f"删除失败：{e}")
            return 0
        finally:
            conn.close()

    def _preview_delete_usr_by_sys_users():
        from backend.sql_utils import get_conn
        emails = [str(r.get("user_name") or "") for r in _select_sys_users()]
        emails = [e for e in emails if e]
        if not emails:
            st.info("无匹配邮箱")
            return
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                fmt = ",".join(["%s"]*len(emails))
                cur.execute(f"SELECT uuid,email,name FROM usr WHERE email IN ({fmt})", emails)
                rows = cur.fetchall() or []
            show = []
            for r in rows:
                try:
                    # pymysql 返回为元组；psycopg2 为元组；统一处理
                    uu, em, nm = r[0], r[1], (r[2] if len(r) > 2 else "")
                    show.append({"uuid": uu, "email": em, "name": nm})
                except Exception:
                    continue
            st.dataframe(show[:200], use_container_width=True)
            st.info(f"可删除 usr {len(show)} 条")
        finally:
            conn.close()

    if btn_preview:
        src = _select_sys_users()
        show = [{
            "user_id": r.get("user_id"),
            "user_name": r.get("user_name"),
            "nick_name": r.get("nick_name"),
            "phonenumber": r.get("phonenumber"),
        } for r in src][:200]
        st.dataframe(show, use_container_width=True)

    if btn_write_preview:
        src = _select_sys_users()
        rows_preview = [_build_usr_row(r) for r in src]
        st.dataframe([
            {"uuid": x["uuid"], "email": x["email"], "tel": x["tel"], "name": x["name"], "sid": x["sid"], "belong_space": x["belong_space"], "data": x["data"]}
            for x in rows_preview
        ][:200], use_container_width=True)

    if btn_apply:
        src = _select_sys_users()
        targets = [_build_usr_row(r) for r in src]
        total = len(targets)
        prog = st.progress(0)
        wrote = 0
        batch = []
        for i, row in enumerate(targets, 1):
            batch.append(row)
            if len(batch) >= 100:
                wrote += _write_usr(batch)
                batch = []
            prog.progress(min(1.0, i/max(1,total)))
        if batch:
            wrote += _write_usr(batch)
        st.success(f"入库 usr {wrote} 条")

    if btn_del:
        n = _delete_usr_by_sys_users()
        st.success(f"已删除 usr {n} 条（来自 sys_user 的邮箱匹配）")

    if btn_del_preview:
        _preview_delete_usr_by_sys_users()


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
