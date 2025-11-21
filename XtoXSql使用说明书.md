# XtoXSql 面向PEPM的数据迁移的配置化处理工具
快速启动：
streamlit run app.py 

版本支持：

- Python：3.9+
- 必需库：streamlit，pymysql，psycopg2-binary（或 psycopg2）
- 数据库：MySQL 5.7+/8.0+，PostgreSQL 12+；瀚高 PEPM（兼容 PG）
- 操作系统：macOS/Linux/Windows

## 1.设计目标

​	为了便捷将新合作企业的数据进行迁移，进行的差异化比对并实现迁移的工具。

- [ ] 从其他SQL转为PEPM的SQL格式
- [ ] PEPM格式转为其他
- [x] 支持多向读取数据填充
- [x] 优化入库
- [x] 目标库、源库更换





## 2.设计架构

### 1  主页面

​	管理基本的映射内容

![image-20251121171746708](/Users/songyihong/Library/Application Support/typora-user-images/image-20251121171746708.png)



### 2 详细配置界面

- 表配置（管理表的映射entity的type和优先级，保证入库顺序和入库目标）![image-20251121173256885](/Users/songyihong/Library/Application Support/typora-user-images/image-20251121173256885.png)
- 表级脚本（当rule无法生效时，此为最后的解决方案，支持基本的py语法）![image-20251121173312085](/Users/songyihong/Library/Application Support/typora-user-images/image-20251121173312085.png)
- 字段映射（最核心的功能，包括映射源、映射目标、映射规则，辅助信息非常详细，*rule*相关内容见3）![image-20251121173330982](/Users/songyihong/Library/Application Support/typora-user-images/image-20251121173330982.png)
- 新增映射（为额外字段提供支持，可以辅助实现数值条件等计算后映射）![image-20251121173343037](/Users/songyihong/Library/Application Support/typora-user-images/image-20251121173343037.png)
- 模拟打印（从源数据拉取一条或者多条进行打印模拟映射后的结果，支持多条件筛选后获取这个模拟数据，解决需要看特殊条件的需求）![image-20251121173406604](/Users/songyihong/Library/Application Support/typora-user-images/image-20251121173406604.png)
- 字段专注（对指定输出字段进行打印，支持大批量打印，直观看出目前的数据）![image-20251121173440534](/Users/songyihong/Library/Application Support/typora-user-images/image-20251121173440534.png)

### 3 映射结构管理

​	是入库相关操作的管理站，支持

- 基本的入库和删除，单个或一键入库删除
- 条件入库（插入，更新，更新插入）
- 状态查看

![image-20251121172649868](/Users/songyihong/Library/Application Support/typora-user-images/image-20251121172649868.png)

### 4 多映射管理中心

​	解决单个表需要映射到多个目标上或者多个表需要映射到单个表的情况，或者更复杂的情况
![image-20251121172928738](/Users/songyihong/Library/Application Support/typora-user-images/image-20251121172928738.png)

### 5目标库切换

​	实现对目标库的切换支持，目前支持mysql和pgsql的目标库
未来将会对源库修改支持（包括本地sql和远程连接库）

![image-20251121173059036](/Users/songyihong/Library/Application Support/typora-user-images/image-20251121173059036.png)

## 3.规则语法（rule）

- 1 透传（空规则）：rule 为空时，直接把 `source_field` 的值写到 `target_paths`
  示例：`source_field=fund_name(源字段名)`，`target_paths=data.fund_name（目标字段名，可以用,分割进行多向赋值，赋值对象，见11）`，`rule=`（空）

- 2 coalesce(a, b, ...)：按顺序取第一个非空值
  示例：`coalesce(sql.ct_company_info(usci=record.usci).stock_type, '未知')` → `data.stock_type`
  示例：`coalesce(record.fund_code, record.fund_number, '')` → `data.fund_code`

- 3 concat(a, b, ...)：字符串拼接
  示例：`concat(record.fund_name, '（', record.fund_code, '）')` → `name`
  示例：`concat('规模', record.fund_scale, '万元')` → `data.scale_label`

- 4 py:{...}.get(key[, default])：字典映射（支持逗号分割的多值）
  示例：`py:{'1':'是','0':'否'}.get(record.get('data', {}).get('is_go_public',''))` → `data.is_go_public_label`
  示例：`py:{'A股':'A股','港股':'港股','美股':'美股','新三板':'新三板'}.get(record.get('data', {}).get('stock_type',''),'未知')` → `data.comType`
  示例：`py:{'1':'电子信息','2':'高端软件'}.get(sing_industry_line,'未分类')` → `data.industry_label`

- 5 常规 py 表达式：`py:...`（内置 `str/int/float/len/round/re/json/__date_ts__`，上下文 `record`）
  示例：`py:' '.join(t.get('name','') for t in json.loads(record.get('data', {}).get('tags','[]') or '[]'))` → `data.tags_text`
  示例：`py:__date_ts__('2025-08-21')` → `data.qcc_update_ts`

- 6 sql.table(cond).field：从源 SQL 文件查字段（大小写与空格不敏感）
  示例：`sql.ct_company_info(usci=record.usci).stock_number` → `data.i5gpfj8y88`
  示例：`sql.ct_company_info(usci=record.usci).stock_type` → `data.stock_type`

- 7 sql.table(sql.table2.where=expr).field：跨 SQL 直接查找
  示例：`sql.ct_company_info(sql.ct_company_info.usci=record.usci).scope` → `data.businessRange`

- 8 entity(typ[,by=field][,src=field]).path：查实体表 JSON 字段
  示例：`entity(fund,by=id,src=id).data.usci` → `data.usci_from_entity`
  示例：`entity(fund).uuid`（默认 `by=id`、`src=fund_id 或 id`）→ `data.fund_uuid`

- 9 entity(target:data.key=expr).path：按 data.key 做 JOIN
  示例：`entity(fund:data.usci=record.usci).uuid` → `data.fund_uuid_by_usci`

- 10 rel(typ[,by=field][,src=field])：按关联获取目标实体 uuid
  示例：`rel(project,by=id,src=project_id)` → `data.project_uuid`

- 11 多目标分发：`target_paths` 写多个，`rule` 用 `||` 分发到各目标
  示例：`target_paths=data.fund_name,data.fund_code`，`rule=record.fund_name || record.fund_code`

- 12 source(table.field=expr).target：调试用，返回引用提示
  示例：`source(ct_company_info.usci=record.usci).stock_type` → `data.debug_ref`（仅标注，不取值）
  
- 13 date(format,table.field):时间规范化示例：`date(%Y-%m-%d,fund_record_time)`

## Example:针对瀚高的操作

### 实现步骤：

1.从瀚高的数据库中利用迁移工具导出到pgsql中

2.将pgsql导出为sql

3.将sql放入到该项目文件中，且做好映射目标的基底

4.开始对数据进行映射管理

5.完成后进行入库操作

6.入库后的mysql可以根据自己的需求转出到其他地方，目前的思路是通过导出为sql文件，然后经过py脚本对差异字段进行处理后，入库到瀚高的pepm数据库中。