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
