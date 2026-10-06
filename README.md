# GDELT 独立数据服务器

把原DSI网站的GDELT采集与计算独立到本地服务器。支持对华关系总览、对华态度、国家风险、企业经营风险，以及7／30／90／365天结果快照。

默认关闭持续监控。没有新闻明细表，不保存报道标题、机构名或原文链接；处理账本与统计数据保存在SQLite中。腾讯云接入可在本地测试完成后进行，本项目没有修改现有DSI网站。

## 存储和更新方式

```
GDELT 15分钟批次
  → 一次下载一个ZIP到临时目录（默认最多128MB）
  → 流式读取压缩包内的行，累加统计量
  → 同一事务更新小时统计、日统计和批次账本
  → 删除临时ZIP（失败也删除）
  → 计算各时间范围的排名、分项、国家趋势与事件类型
  → 发布压缩结果快照
```

- 下载和解析串行进行，单轮默认32个文件，避免并发占用带宽、内存和磁盘。
- 首次增量调度覆盖最近72小时；按批次游标逐段补齐。开监控后会连续处理积压，追平后默认每15分钟检查一次。
- 手动回填会把全部目标批次加入持久队列，分批续跑。回填72小时应包含576个文件，处理上限只限制每轮，不截断整个回填范围。
- 重启沿用批次进度与监控开关。失败批次指数退避重试，最长间隔6小时；未完成队列先处理未尝试批次，再处理到期重试。
- 完成账本不会按小时保留期删除，同一批次不会重复累加。保留期外待处理批次标记为expired；若要补更久历史，请先扩大保留期，再重新加入对应范围。
- 小时聚合默认保留60天，日聚合730天；日统计直接按文件累加，绝不从残缺小时表重建。
- 快照只保留最新两份；成功或失败下载均清理临时文件，进程意外退出遗留的临时ZIP在下次独占启动时清理。
- 默认磁盘余量低于5GB或数据库超过250GB时暂停下载。250GB是可配置的停止阈值，存在单文件及SQLite写入开销，不是精确硬配额。

只有400多GB存储时，这种聚合方案比保留原始新闻节省很多空间，但实际增长速度取决于历史跨度、国家/事件类别数量。运行几天后观察控制页面占用，再调整保留期。删除旧记录后SQLite会重用空页，配置了增量回收；不做持续全库VACUUM，以免需要额外同等磁盘空间。

**增量的是下载和新闻解析。** 滚动窗口过期、国家间百分位变化和公式调整，仍需要从聚合表重新计算；不需要重复解析已经完成的原始文件。

## Windows 本机测试

需要Python 3.11或更高版本。当前开发机已创建 `.venv` 并安装依赖；部署包不包含该环境，请在新服务器重新安装。

在PowerShell中：

```powershell
cd C:\Users\workm\Desktop\GDELTDataServer
powershell -ExecutionPolicy Bypass -File .\start.ps1
```

打开 `http://127.0.0.1:8800`。启动脚本首次复制 `config.example.json` 为 `config.local.json`。默认仅允许本机访问，监控关闭，不会自动下载大量历史文件。

推荐首次测试顺序：

1. 单独运行真实文件自检，确认本机访问GDELT正常。
2. 启动控制页面，回填1至3小时，等待队列完成。
3. 检查“数据截至”“采集覆盖率”和四种视图。刚采集的少量数据不会立即提供30天完整覆盖。
4. 生成并下载快照。
5. 确认磁盘占用和网络稳定后再开持续监控、扩大回填范围。

真实文件自检命令（使用临时目录，仅测试少量文件，结束后清理）：

```powershell
.\.venv\Scripts\python.exe -m gdelt_server --config config.local.json selftest
```

自动化测试：

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[test]"
.\.venv\Scripts\python.exe -m pytest -q
```

## Linux 本地服务器

把源代码解压到 `/opt/GDELTDataServer`（也可使用其他目录），安装Python 3.11+及venv组件，然后：

```bash
cd /opt/GDELTDataServer
bash start.sh
```

启动脚本会创建虚拟环境、安装项目、创建本地配置，然后运行服务。命令也可分开执行：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
cp config.example.json config.local.json
.venv/bin/python -m pytest -q
.venv/bin/python -m gdelt_server --config config.local.json selftest
.venv/bin/python -m gdelt_server --config config.local.json serve
```

若要从局域网其他电脑访问，把配置里的 `host` 改为 `0.0.0.0`，并设置至少24字符的随机 `api_token`。打开 `http://本地服务器IP:8800`，在控制页面输入令牌。实际网络环境再设置相应防火墙规则；公网访问建议由HTTPS反向代理提供。

`deploy/gdelt-data-server.service` 提供systemd模板。安装前创建运行用户 `gdelt`，给予项目读取权限与data目录写入权限，并按真实路径修改WorkingDirectory、ExecStart、ReadWritePaths；若data目录不在项目内，修改ReadWritePaths。模板要求只启动1个worker。配置monitor_enabled只影响首次初始化，随后以数据库里保存的开关为准。

项目用进程锁保护同一data目录，不支持对同一目录开启多个服务进程或worker。数据库损坏或意外删除会失去去重账本，因此应定期备份聚合数据库：暂停服务后备份 `data/gdelt.db`，或者使用SQLite在线backup接口。不要只复制正在写入的主文件而漏掉WAL。

## 配置

相对 `data_dir` 按配置文件所在目录解析。修改容量预算、保留期、端口后重启服务。

| 参数 | 默认值 | 用途 |
|---|---:|---|
| poll_seconds | 900 | 追平后轮询间隔 |
| initial_hours | 72 | 首次启动的增量起点 |
| batch_files | 32 | 每轮处理文件数 |
| hour_retention_days | 60 | 小时统计保留期，最低8天 |
| day_retention_days | 730 | 日统计保留期，最低365天 |
| min_free_gb | 5 | 下载前检查的磁盘余量 |
| max_database_gb | 250 | 数据库与WAL容量预算 |
| max_download_mb | 128 | 单个ZIP下载大小上限 |
| max_uncompressed_mb | 1024 | 单个ZIP声明的解压大小上限 |
| request_timeout | 60 | 网络请求超时秒数 |
| api_token | 空 | 本机模式可留空；远程访问需配置 |

可通过 `GDELT_API_TOKEN` 环境变量覆盖配置中的令牌。原始GKG单字段上限16MB，超过会拒绝该文件并记入失败账本。若官方未来格式改变，未知列布局或不合法核心数值会报错，不会静默记为完成。

官方Events文件偶尔包含事件码为 `---`、大类为 `--`、Goldstein缺失的未分类占位行。这些行不参与分数，计入skipped_rows；其他未知类型或非法核心数值仍拒绝整批。skipped_rows也包含排除地区及无可用地理国家的行，不能全部理解为格式错误。

## 给腾讯云提供结果

第一阶段只运行本地 `serve`，无需frp。计算结果可从下列接口拉取：

- `GET /api/snapshots/latest`：当前快照版本、生成时间、压缩大小及SHA256。
- `GET /api/snapshots/download`：压缩JSON快照。没有原始新闻或数据库。

这些接口使用同一Bearer令牌。云端通过frp访问本地时，只需定期拉取快照并保存；用户访问网站时读取云端已有结果，避免页面请求依赖本地服务器在线。

项目还提供可选的云端结果接收器和主动上传命令，方便之后采用本地推送方式。

云端接收器测试：使用单独的配置文件、端口和data_dir，运行：

```bash
python -m gdelt_server --config cloud-config.json receiver
```

它提供 `PUT /api/snapshots`，校验令牌、大小、SHA256和快照结构，完整接收后原子切换；拒绝旧版本覆盖新版本，并保留最近两份。接口读取时只拆分已计算结果，不下载GDELT，不进行指标计算。本地离线时仍可提供已保存的结果，并按读取时刻标记是否过时。

本地主动上传：

```powershell
# 在安全的本地环境中设置接收端令牌，不要把令牌写入命令参数或上传代码库。
$env:GDELT_PUSH_TOKEN = '<接收端令牌>'
.\.venv\Scripts\python.exe -m gdelt_server --config config.local.json push --url https://你的域名/api/snapshots
```

远程上传要求HTTPS且验证证书。自签证书可使用 `--ca-file` 指定信任的CA。仅本机接收器联调允许HTTP。当前push是单次命令，可在联调后使用本地服务器任务计划定期运行；尚未配置真实腾讯云上传地址，也未创建定时上传任务。

现有DSI接入时，需要替换原GDELT接口数据来源，保持 `/api/gdelt/*` 地址；浏览器通过DSI后端读取结果，不向浏览器暴露同步令牌。原页面的自动刷新条件、最后更新时间和AI工具调用也需要一起接入。**本地服务已独立完成，现有DSI网站的接入属于下一阶段，不能直接宣称现在已联通。**

## 接口

数据接口采用与DSI相近的响应结构，均需Bearer令牌（仅本机模式例外）：

| 接口 | 功能 |
|---|---|
| GET /health | 进程健康检查，不含运行细节 |
| GET /api/gdelt/status | 队列、失败、容量、最后批次、快照状态 |
| GET /api/gdelt/config | 时间范围、是否已有数据 |
| GET /api/gdelt/overview?days=30 | 对华关系总览 |
| GET /api/gdelt/attitude?days=30&country=USA | 对华态度及该国趋势 |
| GET /api/gdelt/country-risk?days=30&country=US | 国家风险及该国趋势 |
| GET /api/gdelt/enterprise-risk?days=30&country=US | 企业风险及该国趋势 |
| GET /api/gdelt/metrics | 当前参数和指标说明 |
| GET /api/gdelt/ai-snapshot?days=30&iso3=USA&fips=US | 仅聚合数字的AI快照 |
| POST /api/admin/monitor | `{"enabled":true}` 开启，false停止 |
| POST /api/admin/sync | 处理一批增量任务，不开启持续监控 |
| POST /api/admin/backfill | `{"hours":72}` 分批处理完整回填范围 |
| PUT /api/admin/params | 修改并校验计算参数 |
| POST /api/admin/export | 后台生成完整快照 |

后台任务请求立即返回accepted，进度通过status查询。停止监控会请求取消当前下载；已提交入库的数据不回滚，临时文件随后清理。遇到超时需要等待当前网络读结束。

## 计算口径和限制

- 沿用原DSI的默认权重。国家风险采用绝对饱和曲线，企业分项采用横截面百分位；企业总分是分项百分位加权平均，不是综合总分自身的排名百分位。
- 风险排名及企业百分位参照集合排除未达到最小样本门槛的国家，默认20；对华态度保留小样本提示。
- 动量只用近期连续且已完整采集的UTC自然日。基线只纳入窗口内完整日，近期日或基线不足时以50中性值参与总分，返回momentum_available=false。
- 缺采集的趋势点为null；完整采集后没有该国事件才为0；采集不完整的桶保留观察值并标记complete=false。
- 每个数据源独立报告截至批次、完成文件数、预期文件数、覆盖率及过时状态。最后批次只表示最新收到的文件，不能证明前面的所有批次已补齐。
- 时间窗口使用含当前未完整桶的自然日/小时桶：30天返回30个日桶，7天返回168个小时桶。不是精确到秒的滚动时间区间；查询过程的一致性由发布快照保证，对在线查询不提供跨多个SQL语句的事务快照。
- 批次时间以官方文件名为准，避免损坏或空时间字段把历史数据计入当前时间。
- 港澳台排除规则沿用原站；海外风险排名另排除中国大陆。GKG同一报道涉及多个国家时分别计数，各国篇数不能相加作为全球去重报道数。
- 信源是引用次数之和，不是窗口内去重媒体数。样本充分度与时间覆盖率分别解释，充分度100不能表示30天采集完整。
- 删除原始数据后，查询权重、事件大类归组、饱和尺度可直接重算；修改GKG主题识别、提及权重截断、语调截断或去重方式需要重新下载解析历史。当前不提供自动删除并重建历史账本的按钮，避免误操作造成重复计数。
- 快照预计算固定四种视图及时间范围、排名、分项、各国趋势和对华事件类型；云端只读快照不支持任意自定义时间范围或非对华partner总览。本地聚合接口保留partner查询能力。
- 容量、性能测试包含小规模真实文件；未在用户的本地服务器做长期365天数据满量测试，实际初次回填耗时与存储需部署后观察。

## 项目结构

```
gdelt_server/
  parser.py       流式ZIP解析，仅输出聚合量
  store.py        原子入库、去重账本、双粒度存储、覆盖率、保留期
  ingest.py       批次游标、断点续跑、临时下载、大小与容量限制
  metrics.py      四种视图、参数校验、动量与覆盖信息
  snapshot.py     批量趋势预计算、完整快照、原子发布
  service.py      单后台任务调度与监控状态
  app.py          独立API及控制页面
  receiver.py     可选云端快照接收器
  cli.py          启动、自检和推送命令
tests/            隔离数据库、模拟下载及两端联调测试
deploy/           Linux服务模板与Dockerfile
```
