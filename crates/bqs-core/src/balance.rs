use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bqs_config::{BalanceMode, Config};

use crate::{
    CoreResult,
    consts::BALANCE_EVAL_INTERVAL,
    control::Control,
    ports::{Clock, EpisodeStore, Ports, TrainerRegistryStore, TrainerStore},
    types::Trainer,
};

#[derive(Debug, Clone, Default)]
pub struct TrainerHeartbeatInput {
    pub trainer_id: String,
    pub variant: String,

    pub kind: i64,

    pub consumed_key: String,
    pub samples_consumed: i64,
    pub backlog: i64,
    pub buffered: i64,

    pub health_status: String,
    pub last_error: String,

    pub progress_json: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TrainerHeartbeatOutcome {
    pub registered: bool,
    pub message: String,
}

impl TrainerHeartbeatOutcome {
    fn registered() -> Self {
        Self {
            registered: true,
            message: String::new(),
        }
    }
}

struct Inner {
    progress: HashMap<String, i64>,

    last_eval: Option<i64>,

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

    pub fn note_trainer_progress(&self, id: &str, progressed: bool, now: i64) {
        let mut inner = self.inner.lock().unwrap();
        if inner.progress.contains_key(id) && !progressed {
            return;
        }
        inner.progress.insert(id.to_string(), now);
    }

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

    fn trainer_stall_reason(
        &self,
        t: &Trainer,
        now: i64,
        online_window: i64,
        stalled_window: i64,
    ) -> Option<String> {
        let age = now - t.last_seen;

        if !crate::is_online(t.last_seen, now, online_window) {
            return Some(format!("trainer={} offline {age}s（心跳超时）", t.id));
        }

        let (_, beyond) = match self.trainers.kind_watermark(t.kind, t.consumed_id) {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(
                    "[balance] ⚠️ trainer={} 读取登记水位失败（跳过该 trainer）: {e}",
                    t.id
                );
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

        let was_auto = self.ctl.auto_paused();
        self.ctl.set_auto_paused(false, "");
        if was_auto {
            tracing::info!("[balance] ✅ trainer 消费恢复，自动暂停已解除");
        } else if !prev_reason.is_empty() {
            tracing::info!("[balance] ✅ trainer 消费恢复");
        }
    }

    pub fn register_trainer_heartbeat(
        &self,
        input: TrainerHeartbeatInput,
    ) -> CoreResult<TrainerHeartbeatOutcome> {
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

        let mut consumed_id = 0i64;
        if !input.consumed_key.is_empty() {
            consumed_id = self
                .episodes
                .episode_id_by_key(&input.consumed_key)?
                .unwrap_or(0);
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

        let progressed = prev
            .as_ref()
            .map(|p| {
                consumed_id > p.consumed_id
                    || input.samples_consumed > p.samples_consumed
                    || input.samples_consumed < p.samples_consumed
            })
            .unwrap_or(false);
        self.note_trainer_progress(&input.trainer_id, progressed, now);

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
        let bal = Arc::new(BalanceMonitor::new(
            &ports,
            cfg,
            f.clock.clone(),
            ctl.clone(),
        ));
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

        bal.evaluate(10_010);
        assert!(!ctl.auto_paused());
        let _ = f;
    }

    #[test]
    fn offline_trainer_triggers_auto_pause_and_recovery() {
        let (f, bal, ctl) = env(PAUSE_CFG);
        f.trainers.set_trainer(trainer("t1", 0, 0));
        bal.evaluate_now(1_000);
        assert!(ctl.auto_paused());
        assert!(ctl.auto_pause_reason().contains("offline"));

        f.trainers.set_trainer(trainer("t1", 990, 0));
        bal.evaluate_now(1_000);
        assert!(!ctl.auto_paused());
    }

    #[test]
    fn stalled_requires_unconsumed_batches() {
        let raw = "[balance]\nenabled = true\nmode = \"pause\"\nstalled_seconds = 100\n";
        let (f, bal, ctl) = env(raw);

        f.trainers.set_trainer(trainer("t1", 990, 1));
        f.trainers.set_episode_ids(0, vec![1]);
        bal.evaluate_now(1_000);
        assert!(!ctl.auto_paused(), "无未消费批次时水位停滞不算停摆");

        f.trainers.set_trainer(trainer("t1", 1_090, 1));
        bal.evaluate_now(1_100);
        assert!(!ctl.auto_paused());

        f.trainers.set_trainer(trainer("t1", 1_190, 1));
        bal.evaluate_now(1_200);
        assert!(!ctl.auto_paused());

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
        f.trainers.set_episode_ids(0, vec![1]);
        bal.evaluate_now(10);
        assert!(!ctl.auto_paused());
        f.trainers.set_episode_ids(0, vec![1, 2, 3]);
        bal.evaluate_now(20);
        assert!(ctl.auto_paused());
        assert!(ctl.auto_pause_reason().contains("积压超阈"));
    }

    #[test]
    fn one_healthy_trainer_blocks_pause() {
        let (f, bal, ctl) = env(PAUSE_CFG);

        f.trainers.set_trainer(trainer("t1", 0, 0));
        f.trainers.set_trainer(trainer("t2", 1_000, 0));
        bal.evaluate_now(1_000);
        assert!(
            !ctl.auto_paused(),
            "只要还有一个 trainer 在正常消费就不暂停"
        );
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

        f.trainers.set_trainer(trainer("t1", 0, 0));
        bal.evaluate(1_000);
        assert!(ctl.auto_paused());

        f.trainers.set_trainer(trainer("t1", 1_003, 0));
        bal.evaluate(1_003);
        assert!(ctl.auto_paused(), "节流窗口内不应重估");

        bal.evaluate(1_010 + 1);
        assert!(!ctl.auto_paused());
    }

    #[test]
    fn heartbeat_registration_and_progress_semantics() {
        let (f, bal, _ctl) = env(PAUSE_CFG);

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
        assert_eq!(
            f.registry.get_trainer("t1").unwrap().unwrap().consumed_id,
            0
        );
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

        f.trainers.set_trainer(trainer("t1", 0, 0));
        bal.evaluate_now(1_000);
        assert!(ctl.auto_paused());

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
