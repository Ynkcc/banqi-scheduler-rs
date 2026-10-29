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
  bqs-store/      适配器：SQLite（rusqlite/WAL）+ 版本化迁移链 v1..v8（可直接打开 Go 版库）
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

只读：`/api/status|networks|matches|eval|eval-config|workers|trainers|episodes|data-stats|tasks|task-stats|train-config|storage(/active|/profiles/{name})`
控制：`POST /api/networks/{sha}/promote`、`/api/eval/trigger`、`/api/control`、
`/api/storage/activate`；`PUT /api/eval-config|train-config|workers/{id}/note|storage/profiles/{name}`；
`DELETE /api/storage/profiles/{name}`。

## 跨端耦合备忘（改动必须三端同步）

- `RULE_SENTINEL_SHA = "rule"` ↔ collector 的 `RULE_TEACHER_SHA`（逐字节）；
- 对象键布局：`episodes/<sha>/<id>.epb.gz`、`episodes/<sha>/<id>.pack.v<n>.fpb.gz`、
  `networks/<sha>.<onnx|pt|nnue>`（三端各有一份派生实现）；
- 评估对手标识 ↔ collector `rule_opponents.rs`；
- 训练超参白名单 ↔ `banqi_training/config.py` 的 `TRAIN_CONFIG_OVERRIDABLE`；
- `variants.json` 由 `banqi-core` 生成（`cargo run -p banqi-core --bin variants_meta`），
  新增变体后须重新生成到 `crates/bqs-core/resources/variants.json`。
