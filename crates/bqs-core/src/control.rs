//! 【所有权：core-judgment 成员】
//!
//! Control：运行时可控状态的组合门面——三个独立热更子对象：
//! - `PauseState`：人工暂停 + 自动暂停（balance 触发，内存态）；生效暂停 = manual || auto，
//!   人工 resume 只清 manual、自动恢复只清 auto，互不覆盖；
//! - `StopSignal`：绝对强度判据置位的停机信号（落库 settings，跨重启保留，
//!   清除需显式操作，防「重启即绕过判据」）；
//! - `HotConfig`：课程阶段（initial_revealed）/ 数据类别 / 训练超参覆盖 / 评估
//!   覆盖（opponents、阈值等；落库 settings，优先于配置文件——**配置文件值仅在
//!   库中无记录时生效**）。
//!
//! 参考实现：`banqi-scheduler/internal/scheduler/control.go`。
//! 注意 serde 语义坑：`EvalOverride.opponents` 的「空列表」必须与「未设置」区分
//! （空列表 = 清空阶梯；未设置 = 回落配置文件）——用 `Option<Vec<String>>` 且
//! `Some(vec![])` 序列化不得省略。
//!
//! **下方公共 API 是其他成员（pipeline / edge）依赖的契约，签名不得改动**；
//! 实现细节（内部子对象结构、settings 键名以 Go 版为准）由所有者填充。
//!
//! 启动语义（与 Go 版 `loadControl` 一致）：
//! - settings 有记录 → 读回；无记录 → 用配置文件值初始化并落库（bootstrap）；
//! - `train_config` 读回时重新归一校验，坏数据**拒绝启动**——`Control::new` 在
//!   引导失败时 panic（锁定签名无法返回 Err；需要可恢复错误路径的测试/工具用
//!   [`Control::try_new`]）。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bqs_config::{Config, DataKind};

use crate::{
    evalcoord::{EvalOverride, SETTING_EVAL_PROMOTIONS},
    evalopponents::validate_eval_opponents,
    ports::{Ports, SettingsStore},
    trainconfig::{TrainConfig, SETTING_TRAIN_CONFIG},
    CoreError, CoreResult,
};

const SETTING_PAUSE_SELF_PLAY: &str = "pause_self_play";
const SETTING_INITIAL_REVEALED: &str = "initial_revealed";
const SETTING_DATA_KIND: &str = "data_kind";
// 绝对强度评估判停（由 EvalCoordinator 的判据写入，跨重启保留）
const SETTING_EVAL_SHOULD_STOP: &str = "eval_should_stop";
const SETTING_EVAL_STOP_REASON: &str = "eval_stop_reason";
// 绝对强度评估在线覆盖（WebUI 写入，JSON 落库；None 字段 = 不覆盖）
const SETTING_EVAL_OVERRIDE: &str = "eval_override";

/// 数据类别覆盖的载体（与 proto DataKind 对齐由调用方转换）。
pub type HotDataKind = DataKind;

/// 把 settings 字符串解析为数据类别（落库口径与 `DataKind::as_str` 一致）。
fn parse_data_kind(s: &str) -> Result<DataKind, String> {
    match s {
        "resnet" => Ok(DataKind::Resnet),
        "nnue" => Ok(DataKind::Nnue),
        other => Err(format!("未知数据类别 {other:?}（可选 resnet / nnue）")),
    }
}

fn validation_error(msg: impl Into<String>) -> CoreError {
    CoreError::Internal(msg.into())
}

// ---------------------------------------------------------------------------
// PauseState：人工 + 自动暂停
// ---------------------------------------------------------------------------

struct PauseInner {
    paused: bool,
    auto_paused: bool,
    auto_pause_reason: String,
}

/// 暂停状态子对象。`auto_paused` 是 balance 子系统派生的内存信号（trainer 消费
/// 停滞置位，恢复自动解除；重启后由 evaluate 依据 trainers 表即时重算，不落库），
/// 与人工 `paused` 严格分离——生效暂停 = paused || auto_paused。
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

    /// 置位/解除自动暂停（只动 auto，人工 paused 不受影响）。
    fn set_auto_paused(&self, paused: bool, reason: &str) {
        let mut g = self.mu.lock().unwrap();
        g.auto_paused = paused;
        g.auto_pause_reason = if paused { reason.to_string() } else { String::new() };
    }

    fn set_manual_paused(&self, paused: bool) -> CoreResult<()> {
        let value = if paused { "1" } else { "0" };
        self.store.set_setting(SETTING_PAUSE_SELF_PLAY, value)?;
        self.mu.lock().unwrap().paused = paused;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// StopSignal：绝对强度停机信号
// ---------------------------------------------------------------------------

struct StopInner {
    stopped: bool,
    stop_reason: String,
}

/// 停机信号子对象。它不是人工开关，而是由绝对强度判据（evalcoord）写入的
/// **派生停机信号**。跨重启保留是有意的——重启不该成为绕过判据的手段。
pub struct StopSignal {
    mu: Mutex<StopInner>,
    store: Arc<dyn SettingsStore>,
}

impl StopSignal {
    fn new(store: Arc<dyn SettingsStore>, stopped: bool, stop_reason: String) -> Self {
        Self {
            mu: Mutex::new(StopInner { stopped, stop_reason }),
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

// ---------------------------------------------------------------------------
// HotConfig：可热更的训练会话参数与覆盖项
// ---------------------------------------------------------------------------

struct HotInner {
    initial_revealed: i32,
    data_kind: DataKind,
    train_config: BTreeMap<String, String>,
    eval_override: Option<EvalOverride>,
}

/// 热更配置子对象：课程阶段、数据类别、训练超参覆盖（字段名 → 值字符串；
/// 只含显式设置过的项）与评估在线覆盖。各值与 settings 表落库同步。
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
        // bootstrap：settings 无记录 → 用配置文件值初始化并落库（库中已有即以库为准）
        if store
            .get_setting(SETTING_INITIAL_REVEALED)
            .map_err(|e| e.to_string())?
            .is_none()
        {
            store
                .set_setting(SETTING_INITIAL_REVEALED, &default_initial_revealed.to_string())
                .map_err(|e| e.to_string())?;
        }
        let kind_raw = store.get_setting(SETTING_DATA_KIND).map_err(|e| e.to_string())?;
        if kind_raw.as_deref().unwrap_or("").is_empty() {
            store
                .set_setting(SETTING_DATA_KIND, default_data_kind.as_str())
                .map_err(|e| e.to_string())?;
        }
        // 读回：整型设置统一走 get_int_setting（键缺省回落默认值，格式非法即失败）
        let initial_revealed = store
            .get_int_setting(SETTING_INITIAL_REVEALED, default_initial_revealed as i64)
            .map_err(|e| format!("setting {SETTING_INITIAL_REVEALED}: {e}"))?
            as i32;
        let data_kind = match store.get_setting(SETTING_DATA_KIND).map_err(|e| e.to_string())? {
            Some(k) if !k.is_empty() => {
                parse_data_kind(&k).map_err(|e| format!("setting {SETTING_DATA_KIND}={k:?}: {e}"))?
            }
            _ => default_data_kind,
        };
        // 训练配置覆盖项：重启后重新归一校验一次——若库里被手改成非法值
        // （字段名写错 / 超范围），必须在这里拦住而不是带病下发。
        let train_config = match store.get_setting(SETTING_TRAIN_CONFIG).map_err(|e| e.to_string())? {
            Some(raw) if !raw.is_empty() => TrainConfig::load_from_setting(&raw)?,
            _ => BTreeMap::new(),
        };
        // 绝对强度评估在线覆盖：库中为 JSON 对象；为空 = 无覆盖。
        let eval_override = match store.get_setting(SETTING_EVAL_OVERRIDE).map_err(|e| e.to_string())? {
            Some(raw) if !raw.is_empty() => Some(serde_json::from_str::<EvalOverride>(&raw).map_err(
                |e| format!("setting {SETTING_EVAL_OVERRIDE}={raw:?} 不是合法的 EvalOverride JSON: {e}"),
            )?),
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
            return Err(validation_error(format!("initial_revealed 必须 >= 0，得到 {n}")));
        }
        self.store.set_setting(SETTING_INITIAL_REVEALED, &n.to_string())?;
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

    /// 全量替换训练配置覆盖项。先归一校验再落库，避免把半套非法配置写进 settings。
    /// 空映射 = 清空覆盖，回落 trainer 本地配置。
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

    /// 全量替换评估在线覆盖。全 None 字段 ≈ 清除覆盖（各字段全部回落配置文件）。
    /// 合法性：对手标识逐个校验（与 validate_eval_opponents 同一白名单）；
    /// `opponents = Some(vec![])` 是合法值（清空阶梯），序列化不得省略。
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

// ---------------------------------------------------------------------------
// Control 组合门面
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct Control {
    pause: Option<PauseState>,
    stop: Option<StopSignal>,
    hot: Option<HotConfig>,
}

// 手写 Debug：子对象持有 trait object（dyn SettingsStore），无法派生；
// 热更状态值经各自查询方法读取，这里不展开内部字段（防泄漏 settings 通道句柄）。
impl std::fmt::Debug for Control {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Control").finish_non_exhaustive()
    }
}

impl Control {
    /// 构造并从 settings 引导（bootstrap / 读回语义见文件头）。
    ///
    /// 引导失败（存储故障 / settings 被污染为非法值）时 panic 拒绝启动——与 Go 版
    /// `loadControl` 返回 error 由 main 拒绝启动同语义；可恢复错误路径用
    /// [`Control::try_new`]。
    pub fn new(ports: &Ports, cfg: Arc<Config>) -> Self {
        match Self::try_new(ports, cfg) {
            Ok(c) => c,
            Err(e) => panic!("Control 引导失败，拒绝启动: {e}"),
        }
    }

    /// 可恢复错误路径的构造（测试 / 运维工具用）。
    pub fn try_new(ports: &Ports, cfg: Arc<Config>) -> Result<Self, String> {
        let store = ports.settings.clone();
        let paused = matches!(
            store.get_setting(SETTING_PAUSE_SELF_PLAY).map_err(|e| e.to_string())?.as_deref(),
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

    // ---- PauseState -------------------------------------------------------

    /// 生效暂停（人工 || 自动）：GetTask 拦 selfplay、Heartbeat/TrainerHeartbeat 回传口径。
    pub fn effective_paused(&self) -> bool {
        self.pause().effective_paused()
    }

    pub fn manual_paused(&self) -> bool {
        self.pause().paused()
    }

    pub fn auto_paused(&self) -> bool {
        self.pause().auto_paused()
    }

    /// 自动暂停原因（未自动暂停时为空）。
    pub fn auto_pause_reason(&self) -> String {
        self.pause().auto_pause_reason()
    }

    /// 人工暂停（WebUI 控制；落库 settings，跨重启保留）。
    pub fn set_manual_paused(&self, paused: bool) -> CoreResult<()> {
        self.pause().set_manual_paused(paused)
    }

    /// 自动暂停置位/解除（BalanceMonitor 调用；内存态）。解除时只清 auto。
    pub fn set_auto_paused(&self, paused: bool, reason: &str) {
        self.pause().set_auto_paused(paused, reason);
    }

    // ---- StopSignal -------------------------------------------------------

    pub fn should_stop(&self) -> (bool, String) {
        self.stop().should_stop()
    }

    /// 判据置位（EvalCoordinator 调用；落库 settings 跨重启保留，幂等，重复调用覆盖原因）。
    pub fn set_stop(&self, reason: &str) -> CoreResult<()> {
        self.stop().set_stop(reason)
    }

    /// 显式清除（WebUI clearStop；防「重启即绕过判据」）。
    pub fn clear_stop(&self) -> CoreResult<()> {
        self.stop().clear_stop()
    }

    // ---- HotConfig --------------------------------------------------------

    /// 课程阶段：库中无记录时回落配置文件值。
    pub fn initial_revealed(&self) -> i32 {
        self.hot().initial_revealed()
    }

    pub fn set_initial_revealed(&self, n: i32) -> CoreResult<()> {
        self.hot().set_initial_revealed(n)
    }

    /// 数据类别：库中无记录时回落配置文件值（经 proto DataKind 转换）。
    pub fn data_kind(&self) -> HotDataKind {
        self.hot().data_kind()
    }

    pub fn set_data_kind(&self, kind: HotDataKind) -> CoreResult<()> {
        self.hot().set_data_kind(kind)
    }

    /// 训练超参覆盖（全量替换语义；启动时从 settings 读取并重新校验）。
    pub fn train_config_overrides(&self) -> BTreeMap<String, String> {
        self.hot().train_config()
    }

    pub fn set_train_config_overrides(
        &self,
        overrides: BTreeMap<String, String>,
    ) -> CoreResult<()> {
        self.hot().set_train_config(overrides)
    }

    /// 评估覆盖（对手阶梯 + 阈值；None 字段 = 回落配置文件）。
    pub fn eval_override(&self) -> Option<EvalOverride> {
        self.hot().eval_override()
    }

    pub fn set_eval_override(&self, o: EvalOverride) -> CoreResult<()> {
        self.hot().set_eval_override(o)
    }

    /// 评估节流计数器（eval_promotions；与 EvalCoordinator 共用 settings 通道）。
    pub fn eval_promotions(&self) -> CoreResult<i64> {
        self.settings().get_int_setting(SETTING_EVAL_PROMOTIONS, 0)
    }

    pub fn set_eval_promotions(&self, n: i64) -> CoreResult<()> {
        self.settings().set_setting(SETTING_EVAL_PROMOTIONS, &n.to_string())
    }

    // ---- 通用 -------------------------------------------------------------

    /// 直接访问 settings（子系统热更落库共用通道；公开给可信赖的子系统）。
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
        // bootstrap 落库
        assert_eq!(
            f.settings.get_setting("initial_revealed").unwrap().as_deref(),
            Some("6")
        );
        assert_eq!(f.settings.get_setting("data_kind").unwrap().as_deref(), Some("nnue"));
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
        // 人工暂停
        c.set_manual_paused(true).unwrap();
        assert!(c.effective_paused() && c.manual_paused() && !c.auto_paused());
        // 自动暂停叠加（原因独立）
        c.set_auto_paused(true, "trainer_stalled");
        assert!(c.effective_paused() && c.auto_paused());
        assert_eq!(c.auto_pause_reason(), "trainer_stalled");
        // 人工 resume 只清 manual，auto 仍生效
        c.set_manual_paused(false).unwrap();
        assert!(c.effective_paused() && c.auto_paused() && !c.manual_paused());
        // 自动恢复只清 auto，manual 不受影响
        c.set_manual_paused(true).unwrap();
        c.set_auto_paused(false, "");
        assert!(c.effective_paused() && c.manual_paused() && !c.auto_paused());
        assert_eq!(c.auto_pause_reason(), "");
        // 人工落库跨重启
        let c2 = Control::try_new(&f.ports(), cfg()).unwrap();
        assert!(c2.manual_paused());
    }

    #[test]
    fn stop_signal_persists_across_restart() {
        let f = Fakes::new();
        let c = control(&f, cfg());
        assert_eq!(c.should_stop(), (false, String::new()));
        c.set_stop("eval_no_progress: test").unwrap();
        assert_eq!(c.should_stop(), (true, "eval_no_progress: test".to_string()));
        // 重复置位覆盖原因（幂等）
        c.set_stop("second").unwrap();
        assert_eq!(c.should_stop(), (true, "second".to_string()));
        // 重启不绕过
        let c2 = Control::try_new(&f.ports(), cfg()).unwrap();
        assert_eq!(c2.should_stop(), (true, "second".to_string()));
        // 显式清除
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
        assert_eq!(c.train_config_overrides().get("LEARNING_RATE").unwrap(), "0.05");
        assert_eq!(f.settings.get_setting("train_config").unwrap().unwrap(), r#"{"LEARNING_RATE":"0.05"}"#);
        // 非白名单字段整批拒绝，既有覆盖不受影响
        let mut bad = m.clone();
        bad.insert("NOT_IN_WHITELIST".to_string(), "1".to_string());
        assert!(c.set_train_config_overrides(bad).is_err());
        assert_eq!(c.train_config_overrides().len(), 1);
        // 全量替换：新 map 不含旧键即删除
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
        // Some(vec![]) = 清空阶梯：序列化不得省略
        let mut o = EvalOverride::default();
        o.opponents = Some(vec![]);
        o.stop_win_rate = Some(0.9);
        c.set_eval_override(o).unwrap();
        let stored = f.settings.get_setting("eval_override").unwrap().unwrap();
        assert!(stored.contains(r#""opponents":[]"#), "{stored}");
        let back = c.eval_override().unwrap();
        assert_eq!(back.opponents, Some(vec![]));
        assert_eq!(back.stop_win_rate, Some(0.9));
        // 非法对手拒绝
        let mut bad = EvalOverride::default();
        bad.opponents = Some(vec!["rule:bogus".to_string()]);
        assert!(c.set_eval_override(bad).is_err());
        // stopWinRate 越界拒绝
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
        assert_eq!(
            f.settings.get_int_setting("eval_promotions", 0).unwrap(),
            7
        );
    }
}
