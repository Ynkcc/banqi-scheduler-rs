# banqi-scheduler-rs

分布式自对弈训练的**中心调度器**（Go 版 `banqi-scheduler` 的 Rust 重构版）：任务分发
（selfplay / rating / eval / reanalysis / derive）、网络登记与晋级、episode 元数据登记 +
对象存储预签名直传、六桶 GSPRT 判停、绝对强度评估与停机判据、生成-消费平衡。

调度器**不执行任何棋类计算**，只做「编排 + 元数据 + 签名」：生产端是 Rust 的
banqi-collector（worker），训练端是 Python 的 banqi-training（trainer），三者通过
gRPC + 对象存储交互。**R2 / MinIO 凭据只在调度器持有**，worker/trainer 零存储配置，
全部经预签名 URL 上下行。

## 架构（Ports & Adapters）

```
crates/
  bqs-proto/      gRPC 契约层：proto/scheduler.proto 代码生成（tonic/prost，无 TLS）
  bqs-config/     类型化配置树：未知键/未知 section 拒绝启动（serde deny_unknown_fields）
  bqs-judgment/   判定引擎（纯函数，无 I/O）：六桶 GSPRT + 停机/回滚/对打判据
  bqs-core/       领域内核：ports（端口 trait）+ 8 个独立持锁子系统 + RPC 编排门面
  bqs-store/      适配器：SQLite（rusqlite/WAL）+ 版本化迁移链 v1..v9（可直接打开 Go 版库）
  bqs-storage/    适配器：S3 SigV4 预签名（手写实现 + 已知答案向量单测）+ 存储Profile 热切换
  bqs-api/        WebUI JSON API（axum，字段名与 Go 版逐字段一致，前端零改动可用）
  bqs-server/     二进制：装配根 + tonic 传输层 + 双监听 + 优雅停机
```

依赖面刻意收窄：**SQLite + 对象存储预签名 + gRPC**，无 Redis、无消息队列、无重量级
SDK。调度器与对象存储之间没有数据面流量（预签名是纯字符串运算）。

### 关键设计约定

- **端口同步、传输层阻塞池化**：内核全部为同步方法（`std::sync::Mutex`，各子系统
  独立持锁，锁序 `evalc.mu → disp.mu`），tonic 层统一 `spawn_blocking`。
- **无后台任务**：超时回收、平衡评估、评估补齐都是请求路径上的惰性计算（带节流），
  时间一律取自注入的 `Clock`（可测试）。
- **业务拒绝 ≠ 错误**：面向客户端的拒绝走正常回包（`accepted=false` + message），
  `CoreError` 只表示真故障 → gRPC `Status::internal`。
- **先落库、后置终态**：上报管线铁律（CAS 抢占上报权 + 失败回滚终态），聚合/落库
  失败可由 worker 重试重新抢占。
- **契约兼容承诺**：gRPC 消息 = `proto/scheduler.proto`（唯一真源）；SQLite schema =
  Go 版迁移链终态（v1..v8 逐字段一致）；WebUI JSON 字段名与 Go 版 `/api/*` 一致
  （契约快照测试锁定）；存储 profile JSON 键名与 Go 版 camelCase 对齐（库双向兼容）。
- **常量集中**：全部 magic number 收敛 `bqs-core/src/consts.rs`。

### 与 Go 版的刻意差异

| 项 | Go 版 | Rust 版 |
|---|---|---|
| TLS 监听 | 可选第二 gRPC TLS 口 | **整体剔除（按要求）** |
| `min_client_version` | 字符串字典序（`0.10.0 < 0.9.0` 缺陷） | 语义化版本感知比较 |
| magic number | 散落各处、部分为 var | `consts.rs` 唯一定义 |
| 配置解析 | 手写 TOML 风格解析器 | `toml` crate 权威解析（不支持 `;` 注释） |
| gatekeeper 自动对打 | 无开关 | 请求级 `RegisterNetworkRequest.skip_match`（单次跳过）+ 全局默认 `[gatekeeper] auto_match`；HTTP 等价入口 `POST /api/networks` |
| eval 启动恢复失败 | 上抛拒启 | 告警降级（作业在 DB，可再恢复） |
| trainconfig float | `ParseFloat` 放行 inf/NaN | 拒绝 |
| WebUI 静态资源 | `go:embed` 内嵌 | `server.webui_dist` 目录托管（构建产物解耦） |
| 回放端点 | `/api/episodes/{id}/replay` | 暂未实现（需对象下载 + proto 解码依赖） |

## 快速开始

```bash
# 构建
cargo build --release -p bqs-server    # 产物 target/release/scheduler

# 配置
cp configs/config.example.conf configs/config.conf   # 按需修改，config.conf 不提交
# 前端（可选）：从 banqi-scheduler/webui 构建产物拷贝到 webui/dist 并配置 server.webui_dist

# 运行
./target/release/scheduler --config configs/config.conf
# 覆盖项：--listen / --db / --http-addr / --bucket（优先于配置文件）
# 停止：SIGINT/SIGTERM → gRPC 优雅排空 + WebUI 优雅关闭（宽限 5s）
```

不读环境变量；配置文件不存在或含未知键直接拒绝启动。存储后端（R2/MinIO/RustFS）
在 `[storage]` 段仅作首次引导，之后以 settings 表与 WebUI「存储」页为准。

## 测试

```bash
cargo test --workspace        # 全部单测（判定向量 / 迁移链 / 子系统 fake / JSON 契约快照）
```

## WebUI 路由摘要

只读：`/api/status|networks|matches|eval|eval-config|workers|trainers|episodes|data-stats|tasks|task-stats|train-config|storage(/active|/profiles/{name})`、
`GET /api/jobs?state=&kind=&jobId=&limit=&offset=`（统一 Job 分页列表，响应含 `total/limit/offset`，limit 缺省 100、0 = 全量）、
`GET /api/jobs/{id}`（单 job 状态查询）
控制：`POST /api/networks`（手动登记网络，`skipMatch=true` 不发起对打）、`POST /api/networks/{sha}/promote`、`/api/eval/trigger`、`/api/control`、
`/api/storage/activate`、`POST /api/jobs`（提交 job）、`POST /api/jobs/{id}/{pause|resume|cancel}`；
`PUT /api/eval-config|train-config|workers/{id}/note|storage/profiles/{name}`；
`DELETE /api/storage/profiles/{name}`。

## 统一 Job 机制（collect / selfplay / eval 三类 job）

契约见 `proto/scheduler.proto` 文件头「统一 Job 机制」注释段（`SubmitJob /
ListJobs / ControlJob`、`JobStatus` 规格回显、`HeartbeatReply.cancel_task_ids`）。
调度器侧核心语义：

- **训练闭环全部走 job**：selfplay 由 `[selfplay]` 配置在启动时引导一个持续型
  job（total_games=0）；eval 由 evalcoord 在评估轮开始时为每个对手自动建 job
  （total 取 `[eval] games`），也可手动 `SubmitJob`。rating / reanalysis /
  derive 保持既有通道。
- **完成口径不含在飞**：completed_games 只按已收到的真实上报聚合（collect /
  selfplay 按 `ReportEpisode.game_count`，eval 按 `ReportMatchResult.games`），
  task→job 映射落库（`job_tasks` 表），先落库后更新计数——重启后迟到上报仍聚合。
- **派发不受在飞封顶**：可派发 job（running 且 completed<total 或 total=0）按
  优先级（priority DESC、created_at ASC）选源，按 worker 线程缩放局数（collect
  向上取偶成对），允许在飞远超剩余需求。
- **完成收敛 + 在途取消**：completed ≥ total（total>0）即置 completed，并把该
  job 全部在飞任务经 `HeartbeatReply.cancel_task_ids` 下发取消（已上报部分不回滚）。
- **优先级抢占**：更高优先级 job 变为可派发（提交/恢复）时立即取消全部更低
  优先级 job 的在飞任务；GetTask 路径按 `JOB_PREEMPT_INTERVAL` 节流惰性兜底。
  默认优先级 selfplay=0 / collect=50 / eval=100（`[jobs]` 段可配）。
- **暂停/恢复/取消**：pause 停派发在飞做完；resume 恢复派发；cancel 停派发 +
  取消全部在飞 + 终态 cancelled；终态不可再控制。
- **提交校验**：variant ∈ {4x8, 4x8-fast, 4x4}；collect / eval 的 total_games
  ≥2 偶数；player kind 白名单；模型类棋手 / eval 对手 network_sha 必须已在
  网络存储登记。
- **种子策略**（collect job）：fixed / random / incremental / range 逐任务由
  job 级单调任务序号确定性解析（`bqs_core::jobs::resolve_seed`），经
  `collect_seed` 下发，保证可复现。

## 跨端耦合备忘（改动必须三端同步）

- `RULE_SENTINEL_SHA = "rule"` ↔ collector 的 `RULE_TEACHER_SHA`（逐字节）；
- 对象键布局：`episodes/<sha>/<id>.epb.gz`、`episodes/<sha>/<id>.pack.v<n>.fpb.gz`、
  `networks/<sha>.<onnx|pt|nnue>`（三端各有一份派生实现）；
- 评估对手标识 ↔ collector `rule_opponents.rs`；
- 训练超参白名单 ↔ `banqi_training/config.py` 的 `TRAIN_CONFIG_OVERRIDABLE`；
- 变体元数据经跨仓库 path 依赖直取 `banqi-core/crates/bq-env`（编译期构建，
  无手工拷贝；`bqs-core` 的 `variants.rs` 是唯一消费点），新增变体后重编译即生效。
