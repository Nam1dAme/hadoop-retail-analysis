# 08 API与服务契约

UBA-DOC-1.0.0 / FROZEN。FastAPI只读，前缀`/api/v1`；绑定127.0.0.1，默认端口8010，UI8510，避开旧项目端口。所有业务响应JSON，除健康/版本发现外必须指定release_id。

## 通用协议

成功响应：`{data, meta:{release_id,baseline_version,request_id,next_cursor}}`；无分页next_cursor=null。错误：`{error:{code,message,request_id}}`，不泄露SQL、凭据或内部绝对路径。ID为字符串，所有BIGINT计数作为十进制字符串；比例/分数为有限JSON number或null；不输出NaN/Infinity。业务日期YYYY-MM-DD；时间ISO8601 UTC带Z，meta可说明business_timezone=Asia/Shanghai。

查询start/end为整日左闭右开，范围必须在04观察期内且start<end；未知参数、无效枚举、超过范围均422。缺省日期为全观察期。behavior为pv/buy/cart/fav；entity_type为ITEM/CATEGORY。列表limit默认20最大100，不使用深OFFSET。

## 端点与字段

| GET路径 | 查询/键 | data内容 |
|---|---|---|
| /health | 无 | status、database_available、baseline；只健康，不泄漏配置 |
| /releases | cursor/limit | release_id、status、source_start/end、published_at、selected_model_id、is_current |
| /releases/current | 无 | 当前release完整元信息及manifest摘要；无current返回404 |
| /overview | release_id,start,end | pv_count/buy_count/cart_count/fav_count、uv、pv_users、pv_buy_users、pv_buy_ratio；分母0比率null |
| /trends | release_id,start,end,granularity=day/hour | bucket_start、behavior_type、event_count；固定补0规则见04 |
| /entities | release_id,start,end,entity_type,behavior,limit,cursor | entity_id、event_count、rank；计数降序ID升序 |
| /entities/{entity_id}/trend | release_id,start,end,entity_type | date、pv/buy/cart/fav_count；无实体返回空数组 |
| /activity | release_id,start,end | date、active_users、first_observed_users、return_next_users、return_next_rate、pv_users、pv_buy_users、pv_buy_ratio |
| /users/{user_id}/activity | release_id,start,end | date与四类次数、first_ts、last_ts；无该用户404 |
| /predictions | release_id,mode=TEST/FORECAST,model_run_id,cursor,limit,user_id可选 | user_id、score、predicted_positive、observed_label、label_status、history_event_count、as_of/window_end、model_run_id |
| /experiments | release_id | experiment_id、split、selected_model_id、selection_reason、各集合样本/正例数、models数组 |
| /models/{model_run_id} | release_id | algorithm、config、device、best_epoch、threshold、metrics、history、benchmarks、hardware脱敏摘要 |
| /quality | release_id | 原始/拒绝/窗口内外计数、重复候选、拒绝原因、来源状态及质量门禁 |
| /tasks | release_id,cursor,limit | run_id、stage、attempt、status、application_id、起止时间、error_code |
| /reports/summary | release_id | 与页面同口径的JSON汇总报告，无原始行为数据 |

FORECAST的model_run_id缺省selected，指定其他模型422；TEST必须属于该release实验，可查三模型，缺省selected。不存在用户预测返回空数组，不自动创建新用户。label_status=OBSERVED或UNOBSERVED，后者observed_label=null。预测窗口由release/mode确定，禁止请求任意日期假装有评分。模型不属该版本返回404。

游标为服务签名的不透明值，包含release、筛选hash、最后排序键和schema；变更筛选或release拒绝422。prediction排序score降序,user_id升序；tasks按created_at降序,run_id升序；releases按published_at降序,release_id升序。排序平局用二进制ID。entity游标包含计数和ID；rank相对当前筛选，从1起。总量不必每次COUNT全表，只返回has_more和next_cursor（has_more放meta）。

HTTP 404=NO_ACTIVE_RELEASE/NOT_FOUND，409=RELEASE_NOT_READY，410=RELEASE_RETIRED，422=INVALID_QUERY，503=DEPENDENCY_UNAVAILABLE，500=INTERNAL_ERROR。数据库暂不可用不返回空成功。只公开READY版本；STAGING不能查询业务数据。

缓存键必须含release及全部规范化参数；TTL默认60秒，不缓存current超过5秒。业务release不可变使旧缓存安全；退休版本先鉴权/状态检查再读缓存。查询超时5秒，连接池上限10，响应上限2MiB；超大返回明确错误并要求缩小范围/分页，不截断而不告知。数据库预聚合和索引用于NF-05，不能在请求时扫描HDFS或加载模型。

正式接口契约实现阶段形成OpenAPI文件，并逐项验证本表字段、NULL语义、排序和错误。不得新增触发训练、发布、文件任意下载或SQL执行的HTTP接口。
