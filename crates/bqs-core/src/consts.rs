use std::time::Duration;

pub const WEIGHT_FORMATS: [&str; 3] = ["onnx", "pt", "bqnq"];

pub const DONE_TASK_RETENTION: Duration = Duration::from_secs(30 * 60);

pub const TASK_MAX_RETENTION: Duration = Duration::from_secs(24 * 3600);

pub const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

pub const COLLECT_TASK_DEADLINE: Duration = Duration::from_secs(30 * 60);

pub const JOB_PREEMPT_INTERVAL: Duration = Duration::from_secs(10);

pub const COLLECT_PLAYER_KINDS: [&str; bq_policy::player_kind::PLAYER_KINDS.len()] =
    bq_policy::player_kind::PLAYER_KINDS;

pub const COLLECT_MODEL_PLAYER_KINDS: [&str; bq_policy::player_kind::MODEL_PLAYER_KINDS.len()] =
    bq_policy::player_kind::MODEL_PLAYER_KINDS;

pub const JOB_VARIANTS: [&str; bq_env::env::config::VARIANT_IDS.len()] =
    bq_env::env::config::VARIANT_IDS;

pub const JOB_PRIORITY_SELFPLAY: i64 = 0;
pub const JOB_PRIORITY_COLLECT: i64 = 50;

pub const DERIVE_QUEUE_MAX: usize = 512;

pub const PRESIGN_TTL: Duration = Duration::from_secs(3600);

pub const LLR_VARIANCE_FLOOR: f64 = 0.1;

pub const HTTP_READ_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
