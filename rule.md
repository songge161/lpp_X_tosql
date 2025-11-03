# Rule 表达式语法手册

> 本文档适用于 `mapper_core.py` 模块中 `_eval_rule()` 的规则字段语法。
> 所有 `rule` 作用于单条源记录 `record`，可在 `field_map` 表中配置。
> 每个字段支持以下几类规则表达式，可自由组合嵌套。

---

## 1️⃣ 基础取值

| 写法 | 说明 | 示例 | 结果 |
|------|------|------|------|
| `record.xxx` | 从当前源记录中取字段 `xxx` | `record.name` | 返回当前行 name 值 |
| `xxx` | 简写（同上） | `name` | 返回当前行 name 值 |
| `'常量字符串'` | 常量文本 | `'基金A'` | 输出 `基金A` |
| 空字符串 | 透传源字段值 | *(留空)* | 相当于 `record.source_field` |

---

## 2️⃣ SQL式函数：`concat()` 与 `coalesce()`

### ▶ `concat(a, b, c, …)`
字符串拼接，可用于组合多个字段。

```python
rule = "concat(record.code, '-', record.name)"
结果："001-示例基金"

▶ coalesce(a, b, c, …)
依次返回第一个非空值（类似 SQL COALESCE）。

python
复制代码
rule = "coalesce(record.short_name, record.full_name, '未知')"
若 short_name 为空 → 返回 full_name；若都空 → "未知"

3️⃣ 跨表引用：entity(...)
用于在目标 entity 表中按条件查找另一实体的字段。

✅ 简单模式
python
复制代码
entity(fund).uuid
等价于：

在 entity 表中查找 type='fund' 且 data.id = record.id 的记录，返回其 uuid。

✅ 指定源字段模式
python
复制代码
entity(fund, src=fund_id).uuid
在 entity 表中查找 type='fund' 且 data.id = record.fund_id 的记录。

✅ Join模式（跨表匹配）
python
复制代码
entity(ct_fund_firm_mid:data.fund_id=ct_fund_base_info.data.id).uuid
在表 ct_fund_firm_mid 中查找 data.fund_id = 当前记录的 ct_fund_base_info.data.id 的行，返回该行的 uuid。

可用于多级依赖映射（例如：基金→管理人→机构）。

4️⃣ Python字典求值：py:{...}
语法
python
复制代码
py:{ <Python字典>.get(键表达式, 默认值) }
示例：枚举映射（字符串 key）
python
复制代码
rule = "py:{'1':'基金储备','2':'投资决策','3':'工商登记','4':'中基协备案','5':'投后管理','6':'基金退出'}.get(record.fund_status, '未知')"
若 fund_status = '3' → 输出 工商登记

说明
直接写 record.fund_status 即可访问源字段；

支持整型与字符串 key；

若 key 不存在则返回默认值。

5️⃣ 规则组合嵌套
规则可层层嵌套，示例：

python
复制代码
rule = "concat('基金-', coalesce(record.short_name, record.full_name, '未知'))"
或跨表结合：

python
复制代码
rule = "concat(entity(fund).uuid, '-', record.name)"
6️⃣ 取值优先顺序
当 rule 为空：

若字段有源字段 → 使用源字段值；

若无源字段（自定义字段） → 空字符串。

7️⃣ 特殊应用场景
场景	示例	说明
映射上级 entity 的 UUID	entity(org, src=org_id).uuid	当前表记录的 org_id 指向 entity(org)
取子表数据	entity(ct_fund_investor:data.fund_id=record.id).data.name	取投资人表中关联基金名
枚举转换	py:{'Y':'是','N':'否'}.get(record.enabled,'否')	Y/N 转中文
拼接标识	concat(record.id, '-', record.name)	组合字段

8️⃣ 调试建议
在 页面脚本模块 末尾加入调试打印：

python
复制代码
print('当前record:', record)
在后台运行时可查看实际 rule 解析后结果。

9️⃣ 小结
类型	格式	用途
常量	'文本'	固定值
字段	record.xxx / xxx	当前表字段
函数	concat(...) / coalesce(...)	拼接与容错
跨表	entity(...)	引用其他 entity
Python求值	py:{...}	字典映射、枚举转换

🔖 推荐实践
保持 rule 简洁、可读；

跨表取值优先使用 entity(...)；

枚举推荐用 py:{}；

若逻辑复杂 → 放入“表级 Python 脚本”中。

版本兼容：

所有语法适用于当前 mapper_core.py 实现；

py:{} 在 _eval_rule() 内通过安全字典解析执行；

支持 UTF-8 中文常量；

不区分大小写（函数名部分）。