//! 运行时常量：全仓 magic number 的唯一定义处（Go 版散落各处、var 可被测试改写的教训）。

use std::time::Duration;

/// 规则哨兵伪 sha：`selfplay.best = "rule"` 时下发的教师网络标识。
/// **跨仓耦合**：必须与 banqi-collector 的 `RULE_TEACHER_SHA` 逐字节一致。
pub const RULE_SENTINEL_SHA: &str = "rule";

/// 权重格式白名单（决定对象键扩展名 `networks/<sha>.<format>`，worker 据此分派加载器）。
pub const WEIGHT_FORMATS: [&str; 3] = ["onnx", "pt", "nnue"];

/// 评估：队首连续下发未上报的最大次数（超限丢弃作业，防旧版 collector 饿死自对弈）。
pub const EVAL_MAX_ATTEMPTS: i64 = 3;
/// 评估：单任务局数上限（流式派发按净剩余截断）。
pub const EVAL_MAX_GAMES_PER_TASK: i64 = 64;
/// 评估：在飞任务失效窗口（超期未上报可重下发）。
pub const EVAL_TASK_STALE_AFTER: Duration = Duration::from_secs(15 * 60);

/// 内存任务表：已上报结束（Completed）记录的保留时长（供重复上报明确报错）。
pub const DONE_TASK_RETENTION: Duration = Duration::from_secs(30 * 60);
/// 内存任务表：未上报记录的最长保留时长（超期回收）。
pub const TASK_MAX_RETENTION: Duration = Duration::from_secs(24 * 3600);
/// 内存任务表：prune 节流间隔（挂在 GetTask 路径上）。
pub const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

/// 生成-消费平衡：惰性评估节流间隔（挂在 GetTask 路径上；心跳到达即时重估）。
pub const BALANCE_EVAL_INTERVAL: Duration = Duration::from_secs(10);

/// 统一 Job 机制：TASK_COLLECT 任务的「已下发 → 超期」期限
///（[collect] deadline_seconds 可覆盖；默认 30 分钟——采集对打含搜索更慢）。
pub const COLLECT_TASK_DEADLINE: Duration = Duration::from_secs(30 * 60);

/// 统一 Job 机制：抢占评估惰性节流间隔（挂在 GetTask 路径上；提交/恢复时即时重估）。
pub const JOB_PREEMPT_INTERVAL: Duration = Duration::from_secs(10);

/// 统一 Job 机制：棋手规格 kind 词表（proto CollectPlayerSpec.kind 词汇表）。
/// 单一真源：bq-policy::player_kind（含全部棋手类型与模型类标记），此处仅重导出。
pub const COLLECT_PLAYER_KINDS: [&str; bq_policy::player_kind::PLAYER_KINDS.len()] =
    bq_policy::player_kind::PLAYER_KINDS;

/// 统一 Job 机制：需要登记网络文件的棋手 kind（模型类）。单一真源同上。
pub const COLLECT_MODEL_PLAYER_KINDS: [&str; bq_policy::player_kind::MODEL_PLAYER_KINDS.len()] =
    bq_policy::player_kind::MODEL_PLAYER_KINDS;

/// 统一 Job 机制：job 规格合法变体词表（提交校验）。
/// 单一真源：bq-env::config::VARIANT_IDS（banqi-core 全部暗棋变体），此处仅重导出。
pub const JOB_VARIANTS: [&str; bq_env::env::config::VARIANT_IDS.len()] =
    bq_env::env::config::VARIANT_IDS;

/// 统一 Job 机制：默认优先级（SubmitJob priority=0 时按 kind 回落）。
pub const JOB_PRIORITY_SELFPLAY: i64 = 0;
pub const JOB_PRIORITY_COLLECT: i64 = 50;
pub const JOB_PRIORITY_EVAL: i64 = 100;

/// 特征包补派队列上限。
pub const DERIVE_QUEUE_MAX: usize = 512;

/// 预签名 URL 有效期。
pub const PRESIGN_TTL: Duration = Duration::from_secs(3600);

/// GSPRT：样本方差为 0（全胜/全负）时的经验下限，避免除零。
pub const LLR_VARIANCE_FLOOR: f64 = 0.1;

/// WebUI HTTP 读头超时。
pub const HTTP_READ_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
/// 优雅停机宽限期。
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
