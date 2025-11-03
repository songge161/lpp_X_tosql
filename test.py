from datetime import datetime

val = record.get('fund_record_time') or record.get('data', {}).get('lrb5u0an6w')
if val:
    try:
        dt = datetime.strptime(val.split('.')[0], "%Y-%m-%d %H:%M:%S")
        record['lrb5u0an6w'] = dt.strftime("%Y-%m-%d")
    except Exception as e:
        print("转换失败:", e)
py:{     '1':'其他型',     '2':'契约型',     '3':'合伙企业型',     '4':'公司型'}.get(organization_form,'未知')