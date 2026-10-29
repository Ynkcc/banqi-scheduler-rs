//! 【所有权：core-judgment 成员】
//!
//! BalanceMonitor：生成-消费平衡（trainer 消费心跳 + 停滞自动暂停）。
//!
//! 参考实现：`banqi-scheduler/internal/scheduler/balance.go` + `trainer.go`
//! （心跳登记）。要点：
//! - **惰性评估**：GetTask 路径按 `BALANCE_EVAL_INTERVAL` 节流、心跳到达即时重估，
//!   无后台定时器；
//! - 三条触发路径：离线（心跳超 `trainer_online_seconds`）/ 在线停摆（水位停滞
//!   超 `stalled_seconds` **且有未消费批次**，经 `TrainerStore::kind_watermark`）/
//!   积压超阈（`backlog_pause_batches`，0 = 关闭）；
//! - **全部 trainer 停滞才触发**；无任何 trainer 心跳 = 无观测数据，绝不触发；
//! - warn（只记日志沿）/ pause（`ctl.set_auto_paused`）两档；恢复自动解除
//!   （只清 auto，人工暂停不受影响）；
//! - **eval 恒不暂停**（由 GetTask 编排侧保证：eval 源不读 pause）。
//!
//! 公共 API（供 pipeline 门面调用）：
//! - [`BalanceMonitor::evaluate`]（GetTask 路径，节流）/
//!   [`BalanceMonitor::evaluate_now`]（强制重估）；
//! - [`BalanceMonitor::register_trainer_heartbeat`]——TrainerHeartbeat RPC 的
//!   业务体：变体校验拒绝（防串变体）→ consumed_key 映射 episodes.id →
//!   UPSERT 心跳 → 进度观测 → 即时重估。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bqs_config::{BalanceMode, Config};

use crate::{
    consts::BALANCE_EVAL_INTERVAL,
    control::Control,
    ports::{Clock, EpisodeStore, Ports, TrainerRegistryStore, TrainerStore},
    types::Trainer,
    CoreResult,
};

/// TrainerHeartbeat RPC 的业务输入（pipeline 从 proto 请求逐字段拷贝）。
#[derive(Debug, Clone, Default)]
pub struct TrainerHeartbeatInput {
    pub trainer_id: String,
    pub variant: String,
    /// 消费的数据类别（proto DataKind as i32）
    pub kind: i64,
    /// 已消费水位：最后一条被训练线程取走的记录所属的 episode 对象键
    pub consumed_key: String,
    pub samples_consumed: i64,
    pub backlog: i64,
    pub buffered: i64,
    /// ok | degraded | fatal（空 = 旧客户端，按 ok 语义）
    pub health_status: String,
    pub last_error: String,
    /// 训练节奏快照 JSON（透传落库，WebUI 展示用）
    pub progress_json: String,
}

/// 心跳登记结果：`registered=false` 为业务拒绝（变体不符），正常回包不报错。
#[derive(Debug, Clone, PartialEq)]
pub struct TrainerHeartbeatOutcome {
    pub registered: bool,
    pub message: String,
}

impl TrainerHeartbeatOutcome {
    fn registered() -> Self {
        Self { registered: true, message: String::new() }
    }
}

struct Inner {
    /// 每个 trainer 最近一次「消费有推进」的时间（trainer_id → Unix 秒）。
    /// 内存态：重启后按首次见到的时间重算（宽限窗口），水位本身在 trainers 表持久化。
    progress: HashMap<String, i64>,
    /// GetTask 路径惰性评估的节流相位。
    last_eval: Option<i64>,
    /// 上次评估的停滞原因（状态转换日志节流的依据）。
    last_reason: String,
}

pub struct BalanceMonitor {
    cfg: Arc<Config>,
    ctl: Arc<Control>,
    clock: Arc<dyn Clock>,
    trainers: Arc<dyn TrainerStore>,
    registry: Arc<dyn TrainerRegistryStore>,
    episodes: Arc<dyn EpisodeStore>,
    inner: Mutex<Inner>,
}

impl BalanceMonitor {
    pub fn new(ports: &Ports, cfg: Arc<Config>, clock: Arc<dyn Clock>, ctl: Arc<Control>) -> Self {
        Self {
            cfg,
            ctl,
            clock,
            trainers: ports.trainers.clone(),
            registry: ports.trainer_registry.clone(),
            episodes: ports.episodes.clone(),
            inner: Mutex::new(Inner {
                progress: HashMap::new(),
                last_eval: None,
                last_reason: String::new(),
            }),
        }
    }

    /// 记录「消费有推进」的时间（progressed=false 且已有时保留原值；
    /// 首次见到该 trainer 以 now 起算，给重启/首报一个宽限窗口）。
    pub fn note_trainer_progress(&self, id: &str, progressed: bool, now: i64) {
        let mut inner = self.inner.lock().unwrap();
        if inner.progress.contains_key(id) && !progressed {
            return; // 保留原进度时间
        }
        inner.progress.insert(id.to_string(), now);
    }

    /// GetTask 路径的惰性评估入口（按 `BALANCE_EVAL_INTERVAL` 节流）。
    pub fn evaluate(&self, now: i64) {
        if !self.cfg.balance.enabled {
            return;
        }
        let due = {
            let mut inner = self.inner.lock().unwrap();
            let due = inner
                .last_eval
                .map_or(true, |t| now - t >= BALANCE_EVAL_INTERVAL.as_secs() as i64);
            if due {
                inner.last_eval = Some(now);
            }
            due
        };
        if due {
            self.evaluate_now(now);
        }
    }

    /// 强制重估策略（心跳到达时调用，绕过节流）。
    ///
    /// 判定口径：全部已登记 trainer 都停滞（或离线）才算消费停滞——只要还有一个
    /// trainer 在正常消费，就不暂停生成。没有任何 trainer 心跳 = 无观测数据，不动。
    pub fn evaluate_now(&self, now: i64) {
        let trainers = match self.trainers.list_trainers() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("[balance] ⚠️ 读取 trainer 心跳失败（本轮跳过评估）: {e}");
                return;
            }
        };
        let mut reasons: Vec<String> = Vec::new();
        if !trainers.is_empty() {
            let online_window = self.cfg.balance.trainer_online_seconds;
            let stalled_window = self.cfg.balance.stalled_seconds;
            for t in &trainers {
                if let Some(r) = self.trainer_stall_reason(t, now, online_window, stalled_window) {
                    reasons.push(r);
                }
            }
        }
        let stalled = !trainers.is_empty() && reasons.len() == trainers.len();
        self.apply_decision(stalled, reasons.join("; "));
    }

    /// 评估单个 trainer 的停滞状态；健康时返回 None。
    fn trainer_stall_reason(
        &self,
        t: &Trainer,
        now: i64,
        online_window: i64,
        stalled_window: i64,
    ) -> Option<String> {
        let age = now - t.last_seen;
        if age > online_window {
            return Some(format!("trainer={} offline {age}s（心跳超时）", t.id));
        }
        // 水位停滞检查需要「已登记未消费」佐证——水位不动但也没有新数据登记时，
        // 是生成侧没活干，不是消费侧停摆。
        let (_, beyond) = match self.trainers.kind_watermark(t.kind, t.consumed_id) {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("[balance] ⚠️ trainer={} 读取登记水位失败（跳过该 trainer）: {e}", t.id);
                return None;
            }
        };
        let last_progress = {
            let mut inner = self.inner.lock().unwrap();
            match inner.progress.get(&t.id) {
                Some(v) => *v,
                None => {
                    inner.progress.insert(t.id.clone(), now);
                    now
                }
            }
        };
        let idle = now - last_progress;
        if idle > stalled_window && beyond > 0 {
            return Some(format!(
                "trainer={} 消费停摆 {idle}s（在线但水位未推进，{beyond} 批未消费）",
                t.id
            ));
        }
        if self.cfg.balance.backlog_pause_batches > 0
            && beyond >= self.cfg.balance.backlog_pause_batches
        {
            return Some(format!(
                "trainer={} 积压超阈 {beyond} 批（阈值 {}）",
                t.id, self.cfg.balance.backlog_pause_batches
            ));
        }
        None
    }

    /// 依据评估结果置位/解除自动暂停，并做状态转换日志（节流：只在原因变化或
    /// 状态翻转时打日志，避免每 10s 刷屏）。
    fn apply_decision(&self, stalled: bool, reason: String) {
        let prev_reason = {
            let mut inner = self.inner.lock().unwrap();
            let prev = inner.last_reason.clone();
            inner.last_reason = reason.clone();
            prev
        };
        if stalled {
            if reason != prev_reason {
                tracing::warn!("[balance] ⚠️ trainer 消费停滞（全部 trainer）: {reason}");
            }
            if self.cfg.balance.mode == BalanceMode::Pause {
                self.ctl.set_auto_paused(true, &reason);
                tracing::info!(
                    "[balance] ⛔ 已自动暂停 selfplay 派发（mode=pause）；恢复后自动解除，人工暂停不受影响"
                );
            }
            return;
        }
        // 恢复路径：无任何停滞原因 → 解除自动暂停（若在）。warn 模式下清除也无害。
        let was_auto = self.ctl.auto_paused();
        self.ctl.set_auto_paused(false, "");
        if was_auto {
            tracing::info!("[balance] ✅ trainer 消费恢复，自动暂停已解除");
        } else if !prev_reason.is_empty() {
            tracing::info!("[balance] ✅ trainer 消费恢复");
        }
    }

    // ---- TrainerHeartbeat RPC 业务体（pipeline 门面调用）--------------------

    /// 登记 trainer 消费心跳并即时重估平衡策略。
    ///
    /// 消费水位口径：consumed_key 是「训练线程最后取走的记录」所属 episode 对象键
    /// （真实消费进度），而非 ListEpisodes 翻页游标。服务端把键映射为 episodes.id，
    /// 结合该 kind 的登记水位计算权威积压（kind_watermark）。
    ///
    /// 变体不符时拒绝登记（防串变体，与 GetTrainConfig 同语义），但正常回包不报错
    /// ——旧配置的 trainer 轮询不该被打断。
    pub fn register_trainer_heartbeat(&self, input: TrainerHeartbeatInput) -> CoreResult<TrainerHeartbeatOutcome> {
        if input.variant != self.cfg.selfplay.variant {
            return Ok(TrainerHeartbeatOutcome {
                registered: false,
                message: format!(
                    "variant_mismatch: got {:?}, server {:?}（未登记心跳）",
                    input.variant, self.cfg.selfplay.variant
                ),
            });
        }
        let prev = self.registry.get_trainer(&input.trainer_id)?;
        // 水位映射：对象键 → episodes.id。键无法识别（极端新对象尚未登记 / 历史键
        // 被清理）时沿用上次水位，避免积压被误判成爆表；键为空 = fresh-start。
        let mut consumed_id = 0i64;
        if !input.consumed_key.is_empty() {
            consumed_id = self.episodes.episode_id_by_key(&input.consumed_key)?.unwrap_or(0);
        }
        if consumed_id == 0 {
            if let Some(p) = &prev {
                consumed_id = p.consumed_id;
            }
        }
        let now = self.clock.now();
        self.registry.touch_trainer(Trainer {
            id: input.trainer_id.clone(),
            last_seen: now,
            variant: input.variant,
            kind: input.kind,
            consumed_key: input.consumed_key,
            consumed_id,
            samples_consumed: input.samples_consumed,
            backlog: input.backlog,
            buffered: input.buffered,
            health_status: input.health_status,
            last_error: input.last_error,
            progress: input.progress_json,
        })?;
        // 进度观测：水位推进或训练线程取走记录数变化都算「消费在推进」。
        // samples_consumed 回落 = trainer 重启归零，按推进处理给宽限。
        let progressed = prev
            .as_ref()
            .map(|p| {
                consumed_id > p.consumed_id
                    || input.samples_consumed > p.samples_consumed
                    || input.samples_consumed < p.samples_consumed
            })
            .unwrap_or(false);
        self.note_trainer_progress(&input.trainer_id, progressed, now);
        // 心跳路径强制重估策略（绕过 GetTask 路径的节流），恢复/触发即时生效。
        self.evaluate_now(now);
        Ok(TrainerHeartbeatOutcome::registered())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::Fakes;
    use crate::types::Trainer;

    use super::*;

    fn env(config_extra: &str) -> (Fakes, Arc<BalanceMonitor>, Arc<Control>) {
        let f = Fakes::new();
        let cfg = Arc::new(Config::parse(config_extra, "test").unwrap());
        let ports = f.ports();
        let ctl = Arc::new(Control::new(&ports, cfg.clone()));
        let bal = Arc::new(BalanceMonitor::new(&ports, cfg, f.clock.clone(), ctl.clone()));
        (f, bal, ctl)
    }

    const PAUSE_CFG: &str = "[balance]\nenabled = true\nmode = \"pause\"\n";

    fn trainer(id: &str, last_seen: i64, consumed_id: i64) -> Trainer {
        Trainer {
            id: id.to_string(),
            last_seen,
            kind: 0,
            consumed_id,
            ..Default::default()
        }
    }

    #[test]
    fn disabled_balance_never_pauses() {
        let (f, bal, _ctl) = env("");
        f.trainers.set_trainer(trainer("t1", 0, 0));
        bal.evaluate_now(10_000);
        assert!(!_ctl.auto_paused());
    }

    #[test]
    fn no_trainers_means_no_observation_no_pause() {
        let (f, bal, ctl) = env(PAUSE_CFG);
        bal.evaluate_now(10_000);
        assert!(!ctl.auto_paused());
        // 未启用节流的 GetTask 路径同理
        bal.evaluate(10_010);
        assert!(!ctl.auto_paused());
        let _ = f;
    }

    #[test]
    fn offline_trainer_triggers_auto_pause_and_recovery() {
        let (f, bal, ctl) = env(PAUSE_CFG); // trainer_online_seconds 默认 120
        f.trainers.set_trainer(trainer("t1", 0, 0));
        bal.evaluate_now(1_000); // age=1000s > 120s → 离线
        assert!(ctl.auto_paused());
        assert!(ctl.auto_pause_reason().contains("offline"));
        // 恢复：心跳新鲜 → 自动解除（人工暂停不受影响的用例见 control 测试）
        f.trainers.set_trainer(trainer("t1", 990, 0));
        bal.evaluate_now(1_000);
        assert!(!ctl.auto_paused());
    }

    #[test]
    fn stalled_requires_unconsumed_batches() {
        let raw = "[balance]\nenabled = true\nmode = \"pause\"\nstalled_seconds = 100\n";
        let (f, bal, ctl) = env(raw);
        // 在线但水位长期未动，且没有未消费批次 = 生成侧没活干，不算停摆
        f.trainers.set_trainer(trainer("t1", 990, 1));
        f.trainers.set_episode_ids(0, vec![1]); // 全部已消费（consumed_id=1 时 beyond=0）
        bal.evaluate_now(1_000); // 首见 t1，进度时间起算 now=1000
        assert!(!ctl.auto_paused(), "无未消费批次时水位停滞不算停摆");
        // 时间推进：idle = 100 未超阈值
        f.trainers.set_trainer(trainer("t1", 1_090, 1));
        bal.evaluate_now(1_100);
        assert!(!ctl.auto_paused());
        // idle = 200 超阈值但 beyond 仍为 0 → 仍不算停摆
        f.trainers.set_trainer(trainer("t1", 1_190, 1));
        bal.evaluate_now(1_200);
        assert!(!ctl.auto_paused());
        // 有未消费批次 → 停摆触发（idle = 300 > 100）
        f.trainers.set_episode_ids(0, vec![1, 2, 3]);
        f.trainers.set_trainer(trainer("t1", 1_290, 1));
        bal.evaluate_now(1_300);
        assert!(ctl.auto_paused());
        assert!(ctl.auto_pause_reason().contains("消费停摆"));
    }

    #[test]
    fn backlog_threshold_triggers() {
        let raw = "[balance]\nenabled = true\nmode = \"pause\"\nbacklog_pause_batches = 2\n";
        let (f, bal, ctl) = env(raw);
        f.trainers.set_trainer(trainer("t1", 0, 0));
        f.trainers.set_episode_ids(0, vec![1]); // beyond=1 < 2
        bal.evaluate_now(10);
        assert!(!ctl.auto_paused());
        f.trainers.set_episode_ids(0, vec![1, 2, 3]); // beyond=3 >= 2
        bal.evaluate_now(20);
        assert!(ctl.auto_paused());
        assert!(ctl.auto_pause_reason().contains("积压超阈"));
    }

    #[test]
    fn one_healthy_trainer_blocks_pause() {
        let (f, bal, ctl) = env(PAUSE_CFG);
        // t1 离线，t2 健康
        f.trainers.set_trainer(trainer("t1", 0, 0));
        f.trainers.set_trainer(trainer("t2", 1_000, 0));
        bal.evaluate_now(1_000);
        assert!(!ctl.auto_paused(), "只要还有一个 trainer 在正常消费就不暂停");
    }

    #[test]
    fn warn_mode_logs_but_never_pauses() {
        let (f, bal, ctl) = env("[balance]\nenabled = true\nmode = \"warn\"\n");
        f.trainers.set_trainer(trainer("t1", 0, 0));
        bal.evaluate_now(10_000);
        assert!(!ctl.auto_paused());
        let _ = f;
    }

    #[test]
    fn evaluate_throttled_on_get_task_path() {
        let (f, bal, ctl) = env(PAUSE_CFG);
        // 第一次评估：离线触发
        f.trainers.set_trainer(trainer("t1", 0, 0));
        bal.evaluate(1_000);
        assert!(ctl.auto_paused());
        // 恢复健康，但 3s 后的 GetTask 评估被节流跳过 → 仍是暂停态
        f.trainers.set_trainer(trainer("t1", 1_003, 0));
        bal.evaluate(1_003);
        assert!(ctl.auto_paused(), "节流窗口内不应重估");
        // 10s 后节流到期 → 重估解除
        bal.evaluate(1_010 + 1);
        assert!(!ctl.auto_paused());
    }

    #[test]
    fn heartbeat_registration_and_progress_semantics() {
        let (f, bal, _ctl) = env(PAUSE_CFG);
        // 变体不符：拒绝登记但正常回包
        let out = bal
            .register_trainer_heartbeat(TrainerHeartbeatInput {
                trainer_id: "t1".into(),
                variant: "wrong".into(),
                ..Default::default()
            })
            .unwrap();
        assert!(!out.registered);
        assert!(out.message.contains("variant_mismatch"));
        assert!(f.registry.get_trainer("t1").unwrap().is_none());
        // 正常心跳（fresh-start，consumed_key 空）
        let out = bal
            .register_trainer_heartbeat(TrainerHeartbeatInput {
                trainer_id: "t1".into(),
                variant: "4x8".into(),
                kind: 0,
                consumed_key: String::new(),
                samples_consumed: 10,
                ..Default::default()
            })
            .unwrap();
        assert!(out.registered);
        let t = f.registry.get_trainer("t1").unwrap().unwrap();
        assert_eq!(t.consumed_id, 0);
        // 键无法识别 → 沿用上次水位 0；再心跳推进水位 → progressed
        let out = bal
            .register_trainer_heartbeat(TrainerHeartbeatInput {
                trainer_id: "t1".into(),
                variant: "4x8".into(),
                kind: 0,
                consumed_key: "unknown-key".into(),
                samples_consumed: 10,
                ..Default::default()
            })
            .unwrap();
        assert!(out.registered);
        assert_eq!(f.registry.get_trainer("t1").unwrap().unwrap().consumed_id, 0);
        f.episodes.seed_episode(7, "episodes/n/7.epb.gz", 0);
        let _ = bal
            .register_trainer_heartbeat(TrainerHeartbeatInput {
                trainer_id: "t1".into(),
                variant: "4x8".into(),
                kind: 0,
                consumed_key: "episodes/n/7.epb.gz".into(),
                samples_consumed: 20,
                ..Default::default()
            })
            .unwrap();
        let t = f.registry.get_trainer("t1").unwrap().unwrap();
        assert_eq!(t.consumed_id, 7);
        let _ = out;
    }

    #[test]
    fn heartbeat_immediately_reevaluates_balance() {
        let (f, bal, ctl) = env(PAUSE_CFG);
        // 先登记一个会离线的 trainer 并触发暂停
        f.trainers.set_trainer(trainer("t1", 0, 0));
        bal.evaluate_now(1_000);
        assert!(ctl.auto_paused());
        // 心跳到达（即时重估绕过节流）→ 恢复
        bal.register_trainer_heartbeat(TrainerHeartbeatInput {
            trainer_id: "t1".into(),
            variant: "4x8".into(),
            kind: 0,
            ..Default::default()
        })
        .unwrap();
        assert!(!ctl.auto_paused(), "心跳路径强制重估应立即恢复");
        let _ = f;
    }
}
