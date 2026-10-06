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
- 重启沿用批次进度与监控开关。失败批次指数退避重试，最长间隔6小时；每批为到期重试和最近一天文件预留处理名额，其余处理历史，避免多年回填饿死更新或失败重试。
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

## Linux 本地服务器（系统Python）

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

使用uv部署请优先看文末“Ubuntu完整部署（uv）”；上面的start.sh使用系统Python。

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
| POST /api/admin/retry | 失败文件设为立即可重试；已有回填等待重试时唤醒原任务 |
| POST /api/admin/backfill | `{"hours":72}` 或 `{"start_date":"2020-01-01"}`，分批回填至今 |
| PUT /api/admin/params | 修改并校验计算参数 |
| POST /api/admin/export | 后台生成完整快照 |

后台任务请求立即返回accepted，进度通过status查询。停止监控会请求取消当前下载；已提交入库的数据不回滚，临时文件随后清理。遇到超时需要等待当前网络读结束。

## 从2020年开始回填与Ubuntu更新（uv）

管理页面的“历史起始日期（UTC）”默认是 `2020-01-01`。点击“从此日期回填至今”后，服务按十五分钟时段下载 Events 与 GKG，处理后删除原始文件，已完成批次跳过。日期从当天 UTC 零点开始，范围最多3650天。小时回填和日期回填不能在同一次请求中同时指定。

日期回填自动把实际日聚合保留期扩大至3650天，并写入数据库。即使服务器的旧 `config.local.json` 仍是730天，重启后也采用较大的已保存保留期，无需修改服务器配置。小时保留期保持原值；3650天是滚动保留期。管理页的状态栏显示实际日保留期。当前查询与快照仍只支持7、30、90、365天窗口，保存多年日聚合不代表已经支持任意历史日期查询。

未完成的回填任务写入数据库，服务重启后自动续跑（即使持续监控关闭）。在页面关闭监控会取消当前任务；再次提交同一起始日期可续补。任务结束后仍可能存在等待重试的失败文件，不能仅凭任务结束判断历史完整，需检查失败记录并开启监控或重新提交回填。

多年首次回填有大量下载和解析工作。建议先测试一天的实际吞吐，再启动多年范围，并监测磁盘和失败数量。数据库预算或磁盘余量不足时，程序暂停下载并报告错误。

在本地将修改提交并推送到 GitHub，然后在按本项目 `/opt` 方案部署的 Ubuntu 服务器执行：

```bash
GDELT_UV_BIN="$(command -v uv)"
sudo systemctl stop gdelt-data-server
cd /opt/GDELTDataServer
sudo git pull --ff-only
sudo env UV_PYTHON_INSTALL_DIR=/opt/gdelt-python \
  "$GDELT_UV_BIN" pip install \
  --python /opt/GDELTDataServer/.venv/bin/python -e '.[test]'
sudo .venv/bin/python -m pytest -q
# 测试通过后更新模板；本次disk I/O修复必须执行以下两行
sudo cp deploy/gdelt-data-server.service /etc/systemd/system/gdelt-data-server.service
sudo systemctl daemon-reload
sudo systemctl start gdelt-data-server
curl --noproxy '*' http://127.0.0.1:8800/health
```

保留 `data/`、`.venv/` 和 `config.local.json`。私有仓库拉取时仍需 GitHub 认证。更新后刷新管理页面，选择起始日期并提交回填；拉取代码和重启本身不会自动启动多年回填。

## 计算口径和限制

- 沿用原DSI的默认权重。国家风险采用绝对饱和曲线，企业分项采用横截面百分位；企业总分是分项百分位加权平均，不是综合总分自身的排名百分位。
- 风险排名及企业百分位参照集合排除未达到最小样本门槛的国家，默认20；对华态度保留小样本提示。
- 动量只用近期连续且已完整采集的UTC自然日。基线只纳入窗口内完整日，近期日或基线不足时以50中性值参与总分，返回momentum_available=false。
- 缺采集的趋势点为null；完整采集后没有该国事件才为0；采集不完整的桶保留观察值并标记complete=false。
- 每个数据源独立报告截至批次、完成文件数、预期文件数、覆盖率及过时状态。最后批次只表示最新收到的文件，不能证明前面的所有批次已补齐。
- 时间窗口使用含当前未完整桶的自然日/小时桶：30天返回30个日桶，7天返回168个小时桶。不是精确到秒的滚动时间区间；在线指标查询使用只读事务保证一次计算的一致性，切换国家复用最多15秒的查询缓存，刷新按钮强制重算。
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

## Ubuntu完整部署（uv）

已有部署的日常更新只需要一条命令，见文末“一条命令更新”。

以下采用 `/opt/GDELTDataServer` 项目目录、`/opt/gdelt-python` Python目录、专用 `gdelt` 服务用户。Python在 `/opt` 下，避免服务的 `ProtectHome=true` 阻止读取个人目录里的uv解释器。不要复制Windows的 `.venv` 到Ubuntu。

### 1. 下载项目

在有sudo权限的Ubuntu登录用户终端执行：

```bash
uv --version
sudo apt update
sudo apt install -y git curl ca-certificates
GDELT_UV_BIN="$(command -v uv)"
sudo git clone https://github.com/xiangzhong26/GDELTDataServer.git /opt/GDELTDataServer
cd /opt/GDELTDataServer
```

已有目录时跳过克隆，按前文“从2020年开始回填与Ubuntu更新（uv）”更新。公开仓库克隆不需要认证；私有仓库在HTTPS的Password提示中输入有该仓库Contents只读权限的GitHub Token，不能使用账号密码。不要把Token写入URL或提交到仓库。

### 2. 准备环境

```bash
sudo env UV_PYTHON_INSTALL_DIR=/opt/gdelt-python \
  "$GDELT_UV_BIN" python install 3.12
sudo env UV_PYTHON_INSTALL_DIR=/opt/gdelt-python \
  "$GDELT_UV_BIN" venv --python 3.12 .venv
sudo env UV_PYTHON_INSTALL_DIR=/opt/gdelt-python \
  "$GDELT_UV_BIN" pip install \
  --python /opt/GDELTDataServer/.venv/bin/python -e '.[test]'
sudo chmod -R a+rX /opt/gdelt-python
```

### 3. 创建用户和配置

```bash
if ! id gdelt >/dev/null 2>&1; then
  sudo useradd --system --user-group \
    --home-dir /opt/GDELTDataServer --shell /usr/sbin/nologin gdelt
fi
sudo mkdir -p /opt/GDELTDataServer/data
sudo chown gdelt:gdelt /opt/GDELTDataServer/data
sudo chmod 750 /opt/GDELTDataServer/data
if [ ! -f config.local.json ]; then
  sudo cp config.example.json config.local.json
fi
sudo chown root:gdelt config.local.json
sudo chmod 640 config.local.json
```

默认监听 `127.0.0.1:8800`，首次监控关闭。配置文件不会被Git更新覆盖。

### 4. 测试并安装服务

```bash
sudo .venv/bin/python -m pytest -q
sudo -u gdelt .venv/bin/python -m gdelt_server --config config.local.json selftest
```

两项都成功后执行：

```bash
sudo cp deploy/gdelt-data-server.service /etc/systemd/system/gdelt-data-server.service
sudo systemctl daemon-reload
sudo systemctl enable --now gdelt-data-server
sudo systemctl status gdelt-data-server --no-pager
curl --noproxy '*' http://127.0.0.1:8800/health
```

预期服务为 `active (running)`，健康接口返回 `{"ok":true,"service":"gdelt-data-server"}`。模板的 `PrivateTmp=true` 为只读系统保护下的SQLite排序、分组提供可写临时目录。只运行一个worker。

### 5. 从Windows访问

在你自己的Windows PowerShell里替换用户名和Ubuntu地址：

```powershell
ssh -N -L 18800:127.0.0.1:8800 你的用户名@Ubuntu服务器IP
```

保持窗口打开，浏览器访问 [管理页面](http://127.0.0.1:18800/)。先回填1小时，检查成功、失败、四个指标和时间范围切换，再扩大历史范围或开启监控。SSH断开仅关闭访问通道，后台服务继续运行。

### 本地修改后推送

在Windows的GDELTDataServer项目中执行；确认变更列表后提交：

```powershell
cd C:\Users\workm\Desktop\GDELTDataServer
git status --short
git add README.md 测试记录.md gdelt_server tests deploy/gdelt-data-server.service
git commit -m "Fix dashboard queries, storage permissions and retry handling"
git push
```

然后执行前文Ubuntu更新步骤，包括重新安装systemd模板。保留 `data/`、`.venv/` 和 `config.local.json`。更新完成后强制刷新管理页面（Ctrl+F5）。

## 页面响应、失败重试与disk I/O故障排查

### 页面选择没有反应或切换慢

新版在指标/时间范围旁显示加载状态和耗时。连续切换取消旧请求，只显示最后选择的结果；请求失败在查询区显示错误并清除旧结果。超过30秒提示超时。

同一视图和时间范围的排名、各国趋势批量计算，最多缓存15秒、最多8组；切换国家复用结果。点击“刷新数据”绕过缓存。在线指标的一次计算使用只读事务，保持排名、趋势和覆盖率的一致性。采集写入、首次长窗口查询和快照生成仍有负载，实际耗时取决于硬件和数据规模。浏览器取消请求不保证服务器已开始的计算立即停止。

“对华关系总览”初始显示全部国家总览，点击国家行后右侧显示该国对华趋势，左侧排名保持总览。切换指标重置国家。缺采集显示空白；刚采集少量数据，30/90/365天覆盖率低是正常现象。

### 失败待重试

页面显示最近失败文件的来源、时间、错误、尝试次数和下次重试时间。成功文件不重复累加。自动重试首次等待约2分钟，之后指数退避，最长6小时；持续监控或未结束回填会按到期时间唤醒。修复问题后点击“立即重试失败”跳过等待；正在处理文件时，等当前批次完成后再点击。

- 超时、断网、连接错误：检查服务器到GDELT的网络，恢复后重试。
- HTTP404：文件可能尚未发布，也可能历史源缺失。等待重试并核对日志中的文件URL；持续404不能标记为完成。
- 文件过大或解析错误：核对文件时间和格式；按原因调整大小上限或更新解析规则。
- 磁盘余量或数据库预算：查看配置和占用，腾出空间或扩大预算，不要删除数据库或账本清零。

### disk I/O error

这是SQLite无法完成I/O，不等于磁盘满，不能仅凭截图判定硬盘损坏。旧服务模板的 `ProtectSystem=strict` 没有可写隔离临时目录；较大排序和分组可能需要临时文件，这是部署缺陷。新版加入 `PrivateTmp=true`，必须按更新步骤重新复制模板、daemon-reload并重启，单纯git pull不会更新已安装的模板。

更新后先在页面切换四个视图和一年窗口，再检查：

```bash
sudo journalctl -u gdelt-data-server -n 200 --no-pager
sudo systemctl show gdelt-data-server -p PrivateTmp -p ProtectSystem -p ReadWritePaths
df -h /opt/GDELTDataServer/data /tmp
df -i /opt/GDELTDataServer/data /tmp
namei -l /opt/GDELTDataServer/data/gdelt.db
```

确认 `PrivateTmp=yes`。数据目录、数据库及 `gdelt.db-wal`/`gdelt.db-shm`（若存在）必须允许gdelt用户读写。如果文件因先前sudo手动启动归属root，修正明确的数据目录归属：

```bash
sudo systemctl stop gdelt-data-server
sudo chown -R gdelt:gdelt /opt/GDELTDataServer/data
sudo -u gdelt /opt/GDELTDataServer/.venv/bin/python -m gdelt_server \
  --config /opt/GDELTDataServer/config.local.json doctor
```

`doctor` 测试目录写入，并用只读连接执行数据库 `quick_check`，不删除或修复数据库。首次未初始化时会报告数据库不存在，检查大数据库可能耗时。手动doctor在服务隔离环境外运行，不能代替管理页面的真实查询测试。检查通过后再启动：

```bash
sudo systemctl start gdelt-data-server
```

如果错误持续，检查系统磁盘日志和挂载状态：

```bash
sudo journalctl -k -n 100 --no-pager
findmnt -T /opt/GDELTDataServer/data
```

核对文件系统是否只读、硬盘错误、网络盘兼容性和剩余inode。保留日志中的 `SQLITE_IOERR_*` 扩展错误码。如果quick_check未通过，先停止写入、备份数据库和残留WAL/SHM，再按具体损坏恢复；不要直接删除数据库、WAL或覆盖回填。

官方依据：[systemd文件系统保护与PrivateTmp](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html)、[SQLite临时文件](https://www.sqlite.org/tempfiles.html)、[SQLite扩展错误码](https://www.sqlite.org/rescode.html)。

## 一条命令更新

在本地提交并推送GitHub后，在Ubuntu登录用户的终端执行：

```bash
bash /opt/GDELTDataServer/deploy/update.sh
```

脚本自动定位当前用户的uv并请求sudo权限，依次完成：获取远程更新、检查可快进、停止服务、更新代码、安装依赖、运行全部测试、安装新版systemd模板、daemon-reload、启动与健康检查。无需手动激活环境、修改配置或逐条执行部署命令。即使代码已是最新，也会重新验证依赖和服务模板。

如果服务器还是没有update.sh的旧版本，首次执行下面这一整行获取脚本并完成更新：

```bash
sudo systemctl stop gdelt-data-server && sudo git -C /opt/GDELTDataServer pull --ff-only && bash /opt/GDELTDataServer/deploy/update.sh
```

此后只使用第一条命令。首次引导命令先停止服务，如果拉取失败，修复网络或认证后重新运行；不要把失败当作更新完成。

适用范围是本文的 `/opt/GDELTDataServer`、uv解释器在 `/opt/gdelt-python`、服务名 `gdelt-data-server`、本机健康接口端口8800的部署方案。更改目录、端口或服务名时需要对应调整脚本。

私有仓库仍可能询问GitHub Token；一条命令不会绕过GitHub认证。服务器工作目录存在未提交或未跟踪内容、分支分叉时，脚本停止更新以保留内容，不执行reset或clean。获取更新在停止服务之前进行；后续安装、测试或健康检查失败时服务保持停止，并显示错误，修复后重跑命令。数据和本地配置保留，不自动创建可能占满磁盘的全量数据库副本，也不自动回退数据库。

脚本检查了Bash语法，项目测试在Windows通过；完整systemd更新流程需在Ubuntu实际执行验证。

### GitHub拉取出现GnuTLS recv error (-110)

该错误表示TLS连接异常中断，可能与网络、代理或连接协议有关，不能仅凭错误确定具体原因。更新脚本使用HTTP/1.1并最多尝试3次，获取失败时不停止正在运行的服务。

如果旧版首次引导命令已经停止服务而拉取失败，先恢复服务：

```bash
sudo systemctl start gdelt-data-server
```

然后仅获取远程提交，不更改正在运行的源码，尝试HTTP/1.1：

```bash
sudo git -c http.version=HTTP/1.1 -C /opt/GDELTDataServer fetch origin
```

fetch成功后再执行首次引导命令，pull也使用HTTP/1.1：

```bash
sudo systemctl stop gdelt-data-server && sudo git -c http.version=HTTP/1.1 -C /opt/GDELTDataServer pull --ff-only && bash /opt/GDELTDataServer/deploy/update.sh
```

若pull再次失败，恢复旧服务并继续排查连接。不要关闭SSL证书验证。持续失败时检查服务器访问GitHub的网络与代理；普通用户能连接但sudo不能连接时，核对sudo环境中的代理和Git配置差异。
