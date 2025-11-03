from datetime import datetime

val = record.get('fund_record_time') or record.get('data', {}).get('lrb5u0an6w')
if val:
    try:
        dt = datetime.strptime(val.split('.')[0], "%Y-%m-%d %H:%M:%S")
        record['lrb5u0an6w'] = dt.strftime("%Y-%m-%d")
    except Exception as e:
        print("转换失败:", e)
py:{     '1':'创业投资类FOF基金',     '2':'创业投资基金',     '3':'私募股权投资类FOF基金',     '4':'私募股权投资基金'}.get(organization_form,'未知')