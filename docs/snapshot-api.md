# 结果发布接口（schema_version = 1）

GDELTDataServer下载并计算数据，DSI只获取计算结果。以下三个GET接口不触发采集、回填或计算；它们读取已经发布的快照。所有地址相对于本地结果服务的根地址，访问令牌通过`Authorization: Bearer …`发送，不放进URL，不交给网页浏览器。

## 配置与权限

- `api_token`：管理令牌，可以操作本地工作台。不能交给DSI作为同步凭据。
- `snapshot_read_token`：同步专用只读令牌，仅能使用本文三个快照接口，不能使用指标、状态或管理接口。
- 开启只读令牌时，两种令牌均须至少24字符且不同，避免通过本机反向代理绕过管理鉴权。支持`GDELT_API_TOKEN`和`GDELT_SNAPSHOT_READ_TOKEN`环境变量覆盖文件配置。
- 未配置新字段的旧配置继续按原方式运行；配置同步访问前不应向公网暴露该服务。首次配置可使用README中的`configure-sync`命令。

## 1. 检查最新版本

`GET /api/snapshots/latest`

成功返回JSON，包含：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 快照数据结构版本，目前为整数1 |
| `snapshot_id` | 本次发布的唯一编号，不能用最新数据日期代替 |
| `sha256` | 压缩文件原始字节的SHA256，64位十六进制字符串 |
| `bytes` | 压缩文件字节数 |
| `created_at` | 生成快照时的时间，包含时区 |
| `data_version` | 聚合数据版本，回填、清理等也会更新 |
| `parameter_version` | 计算参数版本 |
| `parser_version` | 解析口径版本（旧包缺失时可为空） |
| `time_ranges` | 此快照包含的时间范围，例如`[7,30,90,365]` |
| `view_names` | `overview`、`attitude`、`country-risk`、`enterprise-risk` |
| `download_path` | 本版本的下载路径，必须与配置好的服务根地址组合 |
| `coverage_by_range` | 各范围的Events/GKG覆盖情况，包括`last_file_ts`、`last_file_at`、`completed_files`、`expected_files`等 |
| `filename` | 发布端内部文件名，接收端不要用它决定本地保存路径 |

响应同时包含`ETag: "<sha256>"`和`Cache-Control: no-cache`。下一次检查带上`If-None-Match`；相同版本返回304且没有响应体。304仍要求有效令牌。没有快照返回404，不应将它当作一个空版本覆盖已有结果。

版本变化时同步完整压缩快照，不依赖`data_version`或最新文件日期单独判断：参数、自然日窗口变化和历史回填，都可能产生需要更新的发布结果。快照中覆盖率和窗口表示发布时的状态；展示新鲜度时还须结合当前时间，不能长期照搬快照中的`stale`值。

## 2. 下载指定版本

`GET /api/snapshots/versions/{snapshot_id}/download`

下载路径由版本信息给出。返回`application/gzip`，响应头包含`X-Snapshot-ID`、`X-SHA256`、`Content-Length`和`ETag`。先校验压缩字节的长度和SHA256，再解压并校验快照结构及快照ID；只有全部通过才替换云端当前结果。

发布新版本不会把此地址悄悄改成另一个版本。下载已经开始后，文件被清理也不影响已打开的下载流。正常保留最近两份结果；正在下载的文件在Windows上可能暂时多保留，后续发布时再清理。

请求已经过保留期的版本返回404。DSI应重新检查最新版本并有限次重试，而不是改为盲目下载“最新文件”。ID只接受1至128位字母、数字、下划线和连字符，非法ID返回400。

## 3. 兼容旧下载方式

`GET /api/snapshots/download`

仍支持本地工作台“下载快照”和旧调用方，返回请求开始时的最新版本。DSI新同步逻辑应使用上面的指定版本接口，不能将一次检查得到的校验值直接用于随后无版本约束的下载。

## 快照内容

压缩内容为UTF-8 JSON。顶层保留`schema_version`、`snapshot_id`、`created_at`、数据与参数版本、口径参数、指标说明、代码表和`views`。

`views["30"]["country-risk"]`等记录中的`common`包含完整排名、统计、窗口及覆盖情况；`series_by_country`提供各国趋势。态度记录还包括`event_types_by_country`。新版总览也提供按国家的趋势；空国家选择保留全体总览。旧schema-1包的总览国家趋势可以回退读取态度趋势。

接收端可使用`validate_snapshot`和`snapshot_view`适配网页查询，无需原始文件或聚合数据库。AI需要的排名、分项和事件大类已经包含在快照中，DSI接入阶段将从这些结果组装AI查询。

字段增加仍兼容schema 1。消费者应忽略不认识的附加字段；不支持的`schema_version`必须拒绝替换并继续使用上一份有效结果。未知格式不能静默按旧口径解释。

## 推荐的DSI同步流程

1. 启动先读取已保存的有效结果，后台检查一次发布版本。
2. 使用只读令牌检查manifest；304或版本与校验值相同则结束。
3. 检查支持的schema、合理的大小限制，再从配置好的源获取指定版本。
4. 下载临时文件，限制压缩和解压大小；参考现有接收器的64MB压缩/256MB解压上限，实际项目可调整。
5. 校验长度、SHA256、JSON结构、ID及数据/参数版本与manifest一致。
6. 原子保存并切换缓存，保留上一份有效结果，然后通知页面版本变化。
7. 任何步骤失败都保留原结果；记录错误并重试。手动与定时同步共用一把锁。

DSI项目已实现定时拉取、管理页面与AI结果缓存适配。frp需要两端分别手动执行deploy/setup-frp.py安装，下载显示进度条；具体步骤见本项目README和DSI的docs/gdelt-sync.md。frp配置应只转发快照读取入口，优先使用HTTPS或受保护的隧道；不要把整个管理工作台直接暴露为同步入口。
