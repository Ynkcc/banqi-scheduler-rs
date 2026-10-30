use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bqs_config::{Config, DataKind};

use crate::{
    CoreError, CoreResult,
    evalcoord::{EvalOverride, SETTING_EVAL_PROMOTIONS},
    evalopponents::validate_eval_opponents,
    ports::{Ports, SettingsStore},
    trainconfig::{SETTING_TRAIN_CONFIG, TrainConfig},
};

const SETTING_PAUSE_SELF_PLAY: &str = "pause_self_play";
const SETTING_INITIAL_REVEALED: &str = "initial_revealed";
const SETTING_DATA_KIND: &str = "data_kind";

const SETTING_EVAL_SHOULD_STOP: &str = "eval_should_stop";
const SETTING_EVAL_STOP_REASON: &str = "eval_stop_reason";

const SETTING_EVAL_OVERRIDE: &str = "eval_override";

pub type HotDataKind = DataKind;

fn parse_data_kind(s: &str) -> Result<DataKind, String> {
    crate::data_kind::parse(s)
}

fn validation_error(msg: impl Into<String>) -> CoreError {
    CoreError::Internal(msg.into())
}

struct PauseInner {
    paused: bool,
    auto_paused: bool,
    auto_pause_reason: String,
}

pub struct PauseState {
    mu: Mutex<PauseInner>,
    store: Arc<dyn SettingsStore>,
}

impl PauseState {
    fn new(store: Arc<dyn SettingsStore>, paused: bool) -> Self {
        Self {
            mu: Mutex::new(PauseInner {
                paused,
                auto_paused: false,
                auto_pause_reason: String::new(),
            }),
            store,
        }
    }

    fn paused(&self) -> bool {
        self.mu.lock().unwrap().paused
    }

    fn auto_paused(&self) -> bool {
        self.mu.lock().unwrap().auto_paused
    }

    fn auto_pause_reason(&self) -> String {
        self.mu.lock().unwrap().auto_pause_reason.clone()
    }

    fn effective_paused(&self) -> bool {
        let g = self.mu.lock().unwrap();
        g.paused || g.auto_paused
    }

    fn set_auto_paused(&self, paused: bool, reason: &str) {
        let mut g = self.mu.lock().unwrap();
        g.auto_paused = paused;
        g.auto_pause_reason = if paused {
            reason.to_string()
        } else {
            String::new()
        };
    }

    fn set_manual_paused(&self, paused: bool) -> CoreResult<()> {
        let value = if paused { "1" } else { "0" };
        self.store.set_setting(SETTING_PAUSE_SELF_PLAY, value)?;
        self.mu.lock().unwrap().paused = paused;
        Ok(())
    }
}

struct StopInner {
    stopped: bool,
    stop_reason: String,
}

pub struct StopSignal {
    mu: Mutex<StopInner>,
    store: Arc<dyn SettingsStore>,
}

impl StopSignal {
    fn new(store: Arc<dyn SettingsStore>, stopped: bool, stop_reason: String) -> Self {
        Self {
            mu: Mutex::new(StopInner {
                stopped,
                stop_reason,
            }),
            store,
        }
    }

    fn should_stop(&self) -> (bool, String) {
        let g = self.mu.lock().unwrap();
        (g.stopped, g.stop_reason.clone())
    }

    fn set_stop(&self, reason: &str) -> CoreResult<()> {
        self.store.set_setting(SETTING_EVAL_SHOULD_STOP, "1")?;
        self.store.set_setting(SETTING_EVAL_STOP_REASON, reason)?;
        let mut g = self.mu.lock().unwrap();
        g.stopped = true;
        g.stop_reason = reason.to_string();
        Ok(())
    }

    fn clear_stop(&self) -> CoreResult<()> {
        self.store.set_setting(SETTING_EVAL_SHOULD_STOP, "0")?;
        self.store.set_setting(SETTING_EVAL_STOP_REASON, "")?;
        let mut g = self.mu.lock().unwrap();
        g.stopped = false;
        g.stop_reason = String::new();
        Ok(())
    }
}

struct HotInner {
    initial_revealed: i32,
    data_kind: DataKind,
    train_config: BTreeMap<String, String>,
    eval_override: Option<EvalOverride>,
}

pub struct HotConfig {
    mu: Mutex<HotInner>,
    store: Arc<dyn SettingsStore>,
}

impl HotConfig {
    fn try_load(
        store: &Arc<dyn SettingsStore>,
        default_initial_revealed: i32,
        default_data_kind: DataKind,
    ) -> Result<Self, String> {
        if store
            .get_setting(SETTING_INITIAL_REVEALED)
            .map_err(|e| e.to_string())?
            .is_none()
        {
            store
                .set_setting(
                    SETTING_INITIAL_REVEALED,
                    &default_initial_revealed.to_string(),
                )
                .map_err(|e| e.to_string())?;
        }
        let kind_raw = store
            .get_setting(SETTING_DATA_KIND)
            .map_err(|e| e.to_string())?;
        if kind_raw.as_deref().unwrap_or("").is_empty() {
            store
                .set_setting(SETTING_DATA_KIND, default_data_kind.as_str())
                .map_err(|e| e.to_string())?;
        }

        let initial_revealed = store
            .get_int_setting(SETTING_INITIAL_REVEALED, default_initial_revealed as i64)
            .map_err(|e| format!("setting {SETTING_INITIAL_REVEALED}: {e}"))?
            as i32;
        let data_kind = match store
            .get_setting(SETTING_DATA_KIND)
            .map_err(|e| e.to_string())?
        {
            Some(k) if !k.is_empty() => parse_data_kind(&k)
                .map_err(|e| format!("setting {SETTING_DATA_KIND}={k:?}: {e}"))?,
            _ => default_data_kind,
        };

        let train_config = match store
            .get_setting(SETTING_TRAIN_CONFIG)
            .map_err(|e| e.to_string())?
        {
            Some(raw) if !raw.is_empty() => TrainConfig::load_from_setting(&raw)?,
            _ => BTreeMap::new(),
        };

        let eval_override = match store
            .get_setting(SETTING_EVAL_OVERRIDE)
            .map_err(|e| e.to_string())?
        {
            Some(raw) if !raw.is_empty() => {
                Some(serde_json::from_str::<EvalOverride>(&raw).map_err(|e| {
                    format!(
                        "setting {SETTING_EVAL_OVERRIDE}={raw:?} 不是合法的 EvalOverride JSON: {e}"
                    )
                })?)
            }
            _ => None,
        };
        Ok(Self {
            mu: Mutex::new(HotInner {
                initial_revealed,
                data_kind,
                train_config,
                eval_override,
            }),
            store: store.clone(),
        })
    }

    fn initial_revealed(&self) -> i32 {
        self.mu.lock().unwrap().initial_revealed
    }

    fn set_initial_revealed(&self, n: i32) -> CoreResult<()> {
        if n < 0 {
            return Err(validation_error(format!(
                "initial_revealed 必须 >= 0，得到 {n}"
            )));
        }
        self.store
            .set_setting(SETTING_INITIAL_REVEALED, &n.to_string())?;
        self.mu.lock().unwrap().initial_revealed = n;
        Ok(())
    }

    fn data_kind(&self) -> DataKind {
        self.mu.lock().unwrap().data_kind
    }

    fn set_data_kind(&self, kind: DataKind) -> CoreResult<()> {
        self.store.set_setting(SETTING_DATA_KIND, kind.as_str())?;
        self.mu.lock().unwrap().data_kind = kind;
        Ok(())
    }

    fn train_config(&self) -> BTreeMap<String, String> {
        self.mu.lock().unwrap().train_config.clone()
    }

    fn set_train_config(&self, overrides: BTreeMap<String, String>) -> CoreResult<()> {
        let normalized = TrainConfig::normalize(&overrides)
            .map_err(validation_error)
            .map_err(|e| CoreError::Internal(format!("{e}")))?;
        let encoded = serde_json::to_string(&normalized)
            .map_err(|e| CoreError::Internal(format!("序列化训练配置: {e}")))?;
        self.store.set_setting(SETTING_TRAIN_CONFIG, &encoded)?;
        self.mu.lock().unwrap().train_config = normalized;
        Ok(())
    }

    fn eval_override(&self) -> Option<EvalOverride> {
        self.mu.lock().unwrap().eval_override.clone()
    }

    fn set_eval_override(&self, o: EvalOverride) -> CoreResult<()> {
        if let Some(opp) = &o.opponents {
            if !opp.is_empty() {
                validate_eval_opponents(opp).map_err(validation_error)?;
            }
        }
        if let Some(sw) = o.stop_win_rate {
            if !(0.0..=1.0).contains(&sw) {
                return Err(validation_error(format!(
                    "stopWinRate 须在 [0,1]（0 = 关闭达标停机），实际 {sw}"
                )));
            }
        }
        let encoded = serde_json::to_string(&o)
            .map_err(|e| CoreError::Internal(format!("序列化评估覆盖: {e}")))?;
        self.store.set_setting(SETTING_EVAL_OVERRIDE, &encoded)?;
        self.mu.lock().unwrap().eval_override = Some(o);
        Ok(())
    }
}

#[derive(Default)]
pub struct Control {
    pause: Option<PauseState>,
    stop: Option<StopSignal>,
    hot: Option<HotConfig>,
}

impl std::fmt::Debug for Control {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Control").finish_non_exhaustive()
    }
}

impl Control {
    pub fn new(ports: &Ports, cfg: Arc<Config>) -> Self {
        match Self::try_new(ports, cfg) {
            Ok(c) => c,
            Err(e) => panic!("Control 引导失败，拒绝启动: {e}"),
        }
    }

    pub fn try_new(ports: &Ports, cfg: Arc<Config>) -> Result<Self, String> {
        let store = ports.settings.clone();
        let paused = matches!(
            store
                .get_setting(SETTING_PAUSE_SELF_PLAY)
                .map_err(|e| e.to_string())?
                .as_deref(),
            Some("1")
        );
        let pause = PauseState::new(store.clone(), paused);
        let stopped = matches!(
            store
                .get_setting(SETTING_EVAL_SHOULD_STOP)
                .map_err(|e| e.to_string())?
                .as_deref(),
            Some("1")
        );
        let stop_reason = store
            .get_setting(SETTING_EVAL_STOP_REASON)
            .map_err(|e| e.to_string())?
            .unwrap_or_default();
        let stop = StopSignal::new(store.clone(), stopped, stop_reason);
        let hot = HotConfig::try_load(
            &store,
            cfg.selfplay.initial_revealed,
            cfg.selfplay.data_kind,
        )?;
        Ok(Self {
            pause: Some(pause),
            stop: Some(stop),
            hot: Some(hot),
        })
    }

    fn pause(&self) -> &PauseState {
        self.pause.as_ref().expect("Control 已引导")
    }

    fn stop(&self) -> &StopSignal {
        self.stop.as_ref().expect("Control 已引导")
    }

    fn hot(&self) -> &HotConfig {
        self.hot.as_ref().expect("Control 已引导")
    }

    pub fn effective_paused(&self) -> bool {
        self.pause().effective_paused()
    }

    pub fn manual_paused(&self) -> bool {
        self.pause().paused()
    }

    pub fn auto_paused(&self) -> bool {
        self.pause().auto_paused()
    }

    pub fn auto_pause_reason(&self) -> String {
        self.pause().auto_pause_reason()
    }

    pub fn set_manual_paused(&self, paused: bool) -> CoreResult<()> {
        self.pause().set_manual_paused(paused)
    }

    pub fn set_auto_paused(&self, paused: bool, reason: &str) {
        self.pause().set_auto_paused(paused, reason);
    }

    pub fn should_stop(&self) -> (bool, String) {
        self.stop().should_stop()
    }

    pub fn set_stop(&self, reason: &str) -> CoreResult<()> {
        self.stop().set_stop(reason)
    }

    pub fn clear_stop(&self) -> CoreResult<()> {
        self.stop().clear_stop()
    }

    pub fn initial_revealed(&self) -> i32 {
        self.hot().initial_revealed()
    }

    pub fn set_initial_revealed(&self, n: i32) -> CoreResult<()> {
        self.hot().set_initial_revealed(n)
    }

    pub fn data_kind(&self) -> HotDataKind {
        self.hot().data_kind()
    }

    pub fn set_data_kind(&self, kind: HotDataKind) -> CoreResult<()> {
        self.hot().set_data_kind(kind)
    }

    pub fn train_config_overrides(&self) -> BTreeMap<String, String> {
        self.hot().train_config()
    }

    pub fn set_train_config_overrides(
        &self,
        overrides: BTreeMap<String, String>,
    ) -> CoreResult<()> {
        self.hot().set_train_config(overrides)
    }

    pub fn eval_override(&self) -> Option<EvalOverride> {
        self.hot().eval_override()
    }

    pub fn set_eval_override(&self, o: EvalOverride) -> CoreResult<()> {
        self.hot().set_eval_override(o)
    }

    pub fn eval_promotions(&self) -> CoreResult<i64> {
        self.settings().get_int_setting(SETTING_EVAL_PROMOTIONS, 0)
    }

    pub fn set_eval_promotions(&self, n: i64) -> CoreResult<()> {
        self.settings()
            .set_setting(SETTING_EVAL_PROMOTIONS, &n.to_string())
    }

    pub fn settings(&self) -> Arc<dyn SettingsStore> {
        self.pause().store.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Fakes;

    fn control(f: &Fakes, cfg: Arc<Config>) -> Control {
        Control::try_new(&f.ports(), cfg).unwrap()
    }

    fn cfg() -> Arc<Config> {
        Arc::new(Config::parse("", "test").unwrap())
    }

    #[test]
    fn bootstrap_writes_config_defaults_when_settings_empty() {
        let f = Fakes::new();
        let raw = "[selfplay]\ninitial_revealed = 6\ndata_kind = \"nnue\"\n";
        let cfg = Arc::new(Config::parse(raw, "test").unwrap());
        let c = control(&f, cfg);
        assert_eq!(c.initial_revealed(), 6);
        assert_eq!(c.data_kind(), DataKind::Nnue);

        assert_eq!(
            f.settings
                .get_setting("initial_revealed")
                .unwrap()
                .as_deref(),
            Some("6")
        );
        assert_eq!(
            f.settings.get_setting("data_kind").unwrap().as_deref(),
            Some("nnue")
        );
    }

    #[test]
    fn existing_settings_take_priority_over_config_file() {
        let f = Fakes::new();
        f.settings.set_setting("initial_revealed", "2").unwrap();
        f.settings.set_setting("data_kind", "resnet").unwrap();
        let raw = "[selfplay]\ninitial_revealed = 9\ndata_kind = \"nnue\"\n";
        let cfg = Arc::new(Config::parse(raw, "test").unwrap());
        let c = control(&f, cfg);
        assert_eq!(c.initial_revealed(), 2);
        assert_eq!(c.data_kind(), DataKind::Resnet);
    }

    #[test]
    fn pause_manual_and_auto_strictly_separated() {
        let f = Fakes::new();
        let c = control(&f, cfg());
        assert!(!c.effective_paused());

        c.set_manual_paused(true).unwrap();
        assert!(c.effective_paused() && c.manual_paused() && !c.auto_paused());

        c.set_auto_paused(true, "trainer_stalled");
        assert!(c.effective_paused() && c.auto_paused());
        assert_eq!(c.auto_pause_reason(), "trainer_stalled");

        c.set_manual_paused(false).unwrap();
        assert!(c.effective_paused() && c.auto_paused() && !c.manual_paused());

        c.set_manual_paused(true).unwrap();
        c.set_auto_paused(false, "");
        assert!(c.effective_paused() && c.manual_paused() && !c.auto_paused());
        assert_eq!(c.auto_pause_reason(), "");

        let c2 = Control::try_new(&f.ports(), cfg()).unwrap();
        assert!(c2.manual_paused());
    }

    #[test]
    fn stop_signal_persists_across_restart() {
        let f = Fakes::new();
        let c = control(&f, cfg());
        assert_eq!(c.should_stop(), (false, String::new()));
        c.set_stop("eval_no_progress: test").unwrap();
        assert_eq!(
            c.should_stop(),
            (true, "eval_no_progress: test".to_string())
        );

        c.set_stop("second").unwrap();
        assert_eq!(c.should_stop(), (true, "second".to_string()));

        let c2 = Control::try_new(&f.ports(), cfg()).unwrap();
        assert_eq!(c2.should_stop(), (true, "second".to_string()));

        c2.clear_stop().unwrap();
        assert_eq!(c2.should_stop(), (false, String::new()));
    }

    #[test]
    fn train_config_full_replace_and_validation() {
        let f = Fakes::new();
        let c = control(&f, cfg());
        assert!(c.train_config_overrides().is_empty());
        let mut m = BTreeMap::new();
        m.insert("LEARNING_RATE".to_string(), "0.05".to_string());
        c.set_train_config_overrides(m.clone()).unwrap();
        assert_eq!(
            c.train_config_overrides().get("LEARNING_RATE").unwrap(),
            "0.05"
        );
        assert_eq!(
            f.settings.get_setting("train_config").unwrap().unwrap(),
            r#"{"LEARNING_RATE":"0.05"}"#
        );

        let mut bad = m.clone();
        bad.insert("NOT_IN_WHITELIST".to_string(), "1".to_string());
        assert!(c.set_train_config_overrides(bad).is_err());
        assert_eq!(c.train_config_overrides().len(), 1);

        c.set_train_config_overrides(BTreeMap::new()).unwrap();
        assert!(c.train_config_overrides().is_empty());
    }

    #[test]
    fn corrupted_train_config_rejects_startup() {
        let f = Fakes::new();
        f.settings
            .set_setting("train_config", r#"{"TRAIN_BATCH":"zero"}"#)
            .unwrap();
        let err = Control::try_new(&f.ports(), cfg()).unwrap_err();
        assert!(err.contains("train_config"), "{err}");
    }

    #[test]
    fn eval_override_roundtrip_keeps_empty_ladder() {
        let f = Fakes::new();
        let c = control(&f, cfg());
        assert!(c.eval_override().is_none());

        let mut o = EvalOverride::default();
        o.opponents = Some(vec![]);
        o.stop_win_rate = Some(0.9);
        c.set_eval_override(o).unwrap();
        let stored = f.settings.get_setting("eval_override").unwrap().unwrap();
        assert!(stored.contains(r#""opponents":[]"#), "{stored}");
        let back = c.eval_override().unwrap();
        assert_eq!(back.opponents, Some(vec![]));
        assert_eq!(back.stop_win_rate, Some(0.9));

        let mut bad = EvalOverride::default();
        bad.opponents = Some(vec!["rule:bogus".to_string()]);
        assert!(c.set_eval_override(bad).is_err());

        let mut bad = EvalOverride::default();
        bad.stop_win_rate = Some(1.5);
        assert!(c.set_eval_override(bad).is_err());
    }

    #[test]
    fn eval_promotions_counter_via_settings() {
        let f = Fakes::new();
        let c = control(&f, cfg());
        assert_eq!(c.eval_promotions().unwrap(), 0);
        c.set_eval_promotions(7).unwrap();
        assert_eq!(c.eval_promotions().unwrap(), 7);
        assert_eq!(f.settings.get_int_setting("eval_promotions", 0).unwrap(), 7);
    }
}
