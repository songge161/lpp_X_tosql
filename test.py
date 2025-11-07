from datetime import datetime

from pyarrow import uuid

val = record.get('fund_record_time') or record.get('data', {}).get('lrb5u0an6w')
if val:
    try:
        dt = datetime.strptime(val.split('.')[0], "%Y-%m-%d %H:%M:%S")
        record['lrb5u0an6w'] = dt.strftime("%Y-%m-%d")
    except Exception as e:
        print("转换失败:", e)
py:{     '1':'创业投资类FOF基金',     '2':'创业投资基金',     '3':'私募股权投资类FOF基金',     '4':'私募股权投资基金'}.get(organization_form,'未知')


entity.fund(entity.fund.data.id=sql.ct_fund_firm_mid(sql.ct_fund_firm_mid.firm_id=record.id).fund_id).uuid
entity(import_fund_info:data.fund_id=record.id).department
sql.import_fund_info(sql.import_fund_info.fund_id=record.id).department
entity(import_fund_info:data.fund_id=record.id).manager
sql.import_fund_info(sql.import_fund_info.fund_id=record.id).manager
{'A股':'A股','港股':'港股','美股':'美股','新三板':'新三板','新四板':'新四板'}
py:{'1':'自主基金项目','2':'参股基金项目','3':'股权直投项目','4':'债权项目','5':'子基金项目','6':'其他'}.get(sproject_status,'其他')

record.fund_name||record.fund_name||entity.fund(sql.ct_fof_subfund_mid.subfund_id=record.id).uuid
entity.fund(entity.fund.data.id=sql.ct_fof_subfund_mid(sql.ct_fof_subfund_mid.subfund_id=record.id).fof_id).uuid
entity.fund(data.id = sql.ct_fof_subfund_mid(subfund_id = record.id).fof_id).uuid


entity.fund(data.id =record.id).data.fund_name|| record.fund_name||entity.fund(data.id=record.id).uuid
py:{'1':'集成电路','2':'电子信息','3':'高端软件','4':'人工智能','5':'汽车','6':'专用装备','7':'高端数控机床与机器人','8':'新能源装备','9':'空天信息','10':'先进材料','11':'钢铁','12':'现代医药','13':'食品与生物制造'}.get(sing_industry_line,'未分类')