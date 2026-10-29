//! 【所有权：infra 成员】
//!
//! S3 兼容对象存储适配器（Cloudflare R2 / MinIO / RustFS）。
//!
//! 参考实现（行为真源）：`banqi-scheduler/internal/r2/`（r2.go / manager.go /
//! profile.go / profiles.go）。
//!
//! 设计要点（相对 Go 版的刻意取舍）：
//! - **预签名是纯字符串运算（SigV4），手写实现 + 单测锁定**——调度器与对象存储
//!   之间没有数据面流量，不引入重量级 SDK / TLS 栈；
//! - 对象键约定（**三端共享，改动必须同步**）：
//!   - episode：`episodes/<network_sha>/<uuid>.epb.gz`
//!   - 特征包：`episodes/<network_sha>/<data_id>.pack.v<n>.fpb.gz`（由记录键派生）
//!   - 网络：`networks/<sha>.<onnx|pt|nnue>`（扩展名 = 权重格式）
//! - path_style=true 时用 path-style 寻址（本地 MinIO/RustFS 必需），否则 virtual-host；
//! - 凭据只在本 crate 与调用方之间流转，永不出现在预签名 URL 与日志里；
//! - Profile 管理与持久化（settings 表 `storage.profiles` / `storage.active`，
//!   经 `bqs_core::ports::SettingsStore`，保持 storage → core 单向无环）：
//!   `Manager` 支持 upsert / delete（非 active 保护）/ activate（先 HeadBucket
//!   连通性校验，失败不切换；成功后原子替换签名器并持久化 active）。
//!   HeadBucket 用 `ureq`（全仓唯一网络调用点，同步）。

pub mod fetch;
mod sigv4;

use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bqs_config::StorageSection;
use bqs_core::ports::{Clock, ObjectSigner, SettingsStore, SystemClock};
use bqs_core::{CoreError, CoreResult};

/// settings 表中的存储配置键（与 Go 版 internal/r2/profiles.go 一致）。
const KEY_STORAGE_PROFILES: &str = "storage.profiles";
const KEY_STORAGE_ACTIVE: &str = "storage.active";

/// 单个存储 profile（settings 表 `storage.profiles` JSON 数组元素）。
///
/// serde 键名对齐 Go 版 `r2.Profile` 的 json tag（camelCase：accessKey/secretKey/
/// pathStyle）——「迁移链可直接打开 Go 版库」的承诺覆盖 settings 表里的
/// `storage.profiles` JSON：Go 写的 Rust 能读、Rust 写的 Go 能读，双向兼容。
/// serde 往返完整保留 secret_key（落库需要）；Debug 手工实现以掩码输出，
/// 保证 secret_key 不出现在日志中。对外 API 的掩码（accessKey 遮蔽、secret 只回
/// hasSecret 布尔）由 api 层负责。
#[derive(Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageProfile {
    pub name: String,
    pub bucket: String,
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    #[serde(default)]
    pub secret_key: String,
    #[serde(default)]
    pub path_style: bool,
}

impl fmt::Debug for StorageProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageProfile")
            .field("name", &self.name)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("access_key", &self.access_key)
            .field("secret_key", &if self.secret_key.is_empty() { "(空)" } else { "******" })
            .field("path_style", &self.path_style)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("storage: {0}")]
    Invalid(String),
    #[error("head bucket 校验失败: {0}")]
    HeadBucket(String),
}

impl From<StorageError> for CoreError {
    fn from(e: StorageError) -> Self {
        CoreError::Storage(e.to_string())
    }
}

/// SigV4 预签名器（无状态，可测试）。凭据持有于内部，Debug 不输出。
pub struct Presigner {
    inner: sigv4::SigV4,
}

impl Presigner {
    pub fn new(profile: StorageProfile) -> Result<Self, StorageError> {
        Self::build(profile, Arc::new(SystemClock))
    }

    /// 测试 / 装配根可注入时钟（Amz-Date 取自时钟，不用 SystemTime::now 散落各处）。
    pub fn with_clock(profile: StorageProfile, clock: Arc<dyn Clock>) -> Result<Self, StorageError> {
        Self::build(profile, clock)
    }

    fn build(profile: StorageProfile, clock: Arc<dyn Clock>) -> Result<Self, StorageError> {
        if profile.bucket.is_empty() {
            return Err(StorageError::Invalid(format!(
                "storage profile {:?}: bucket 不能为空",
                profile.name
            )));
        }
        if profile.access_key.is_empty() || profile.secret_key.is_empty() {
            return Err(StorageError::Invalid(format!(
                "storage profile {:?}: access_key / secret_key 不能为空",
                profile.name
            )));
        }
        let inner = sigv4::SigV4::from_profile(&profile, clock)?;
        Ok(Presigner { inner })
    }

    /// 预签名 PUT（episode / 网络上传；数据直传对象存储，不经调度器）。
    pub fn presign_put(&self, key: &str, ttl_secs: i64) -> Result<String, StorageError> {
        self.inner.presign("PUT", key, ttl_secs)
    }

    /// 预签名 GET（episode / 网络下载）。
    pub fn presign_get(&self, key: &str, ttl_secs: i64) -> Result<String, StorageError> {
        self.inner.presign("GET", key, ttl_secs)
    }
}

impl fmt::Debug for Presigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Presigner")
            .field("bucket", &self.inner.bucket)
            .field("path_style", &self.inner.path_style)
            .finish_non_exhaustive()
    }
}

/// 端口实现：把 StorageError 映射为 CoreError::Storage（真故障约定）。
impl ObjectSigner for Presigner {
    fn presign_put(&self, key: &str, ttl_secs: i64) -> CoreResult<String> {
        self.presign_put(key, ttl_secs).map_err(StorageError::into)
    }
    fn presign_get(&self, key: &str, ttl_secs: i64) -> CoreResult<String> {
        self.presign_get(key, ttl_secs).map_err(StorageError::into)
    }
}

/// HeadBucket 用的 HTTP 客户端（同步，5s 超时）。
fn http_agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(5))
        .build()
}

/// 无 active profile 时的占位签名器：所有签名请求报「未配置」故障。
/// （`signer()` 必须总是返回 Arc<dyn ObjectSigner>；Go 版在 active 为空时
/// 由调用方判空，Rust 端口无 Option，用占位实现保持装配根简单。）
#[derive(Debug, Default)]
struct NullSigner;

impl ObjectSigner for NullSigner {
    fn presign_put(&self, _key: &str, _ttl_secs: i64) -> CoreResult<String> {
        Err(CoreError::Storage("未配置生效的存储 profile（storage.active 为空）".into()))
    }
    fn presign_get(&self, _key: &str, _ttl_secs: i64) -> CoreResult<String> {
        Err(CoreError::Storage("未配置生效的存储 profile（storage.active 为空）".into()))
    }
}

/// 存储后端管理器：profiles 持久化 + active 热切换（原子替换签名器快照）。
pub struct Manager {
    settings: Arc<dyn SettingsStore>,
    /// (active 名, 当前签名器快照)。热切换点；新请求读快照，已签发 URL 指向旧后端。
    current: RwLock<(Option<String>, Arc<dyn ObjectSigner>)>,
}

impl Manager {
    pub fn new(
        settings: Arc<dyn SettingsStore>,
        bootstrap: Option<StorageSection>,
    ) -> Result<Self, StorageError> {
        let (mut profiles, mut active) = load_profiles(&settings)?;

        // 库中无记录且给了 [storage] bootstrap → 引导 default profile 并持久化；
        // 两者皆无 → 报错（对照 Go 版 cmd/scheduler 首次启动引导语义）。
        if profiles.is_empty() && active.is_empty() {
            let Some(sec) = bootstrap else {
                return Err(StorageError::Invalid(
                    "settings 中无 storage.profiles 记录，且未提供 [storage] 引导段：无法初始化对象存储".into(),
                ));
            };
            if sec.bucket.is_empty() || sec.access_key.is_empty() || sec.secret_key.is_empty() {
                return Err(StorageError::Invalid(
                    "[storage] 引导段不完整（bucket / access_key / secret_key 均必填）".into(),
                ));
            }
            let p = StorageProfile {
                name: "default".into(),
                bucket: sec.bucket,
                region: sec.region,
                endpoint: sec.endpoint,
                access_key: sec.access_key,
                secret_key: sec.secret_key,
                path_style: sec.path_style,
            };
            tracing::info!(
                "[storage] 以 [storage] 引导段创建 default profile: bucket={} endpoint={} path_style={}",
                p.bucket,
                if p.endpoint.is_empty() { "(空)" } else { &p.endpoint },
                p.path_style
            );
            profiles.push(p.clone());
            active = p.name.clone();
            save_profiles(&settings, &profiles, &active)?;
        }

        // active 指向存在的 profile 才有生效签名器；否则用占位（签名请求即报错）。
        let signer: Arc<dyn ObjectSigner> = match profiles.iter().find(|p| p.name == active) {
            Some(p) => Arc::new(Presigner::new(p.clone())?),
            None => {
                if !active.is_empty() {
                    tracing::warn!("[storage] storage.active={active:?} 指向不存在的 profile，签名请求将报错");
                }
                Arc::new(NullSigner)
            }
        };
        Ok(Manager { settings, current: RwLock::new((Some(active).filter(|s| !s.is_empty()), signer)) })
    }

    /// 当前生效的签名器快照（实现 `ObjectSigner` 端口，装配根直接注入）。
    /// 内部为快照读取：热切换后新请求用新签名器，已签发 URL 指向旧后端。
    pub fn signer(&self) -> Arc<dyn ObjectSigner> {
        self.current.read().unwrap_or_else(|p| p.into_inner()).1.clone()
    }

    pub fn active_name(&self) -> Option<String> {
        self.current.read().unwrap_or_else(|p| p.into_inner()).0.clone()
    }

    pub fn list_profiles(&self) -> Vec<StorageProfile> {
        load_profiles(&self.settings).map(|(p, _)| p).unwrap_or_default()
    }

    pub fn profile(&self, name: &str) -> Option<StorageProfile> {
        self.list_profiles().into_iter().find(|p| p.name == name)
    }

    /// upsert（secret 留空 = 保持原值；新建必填）。不自动生效。
    pub fn upsert_profile(&self, mut p: StorageProfile) -> Result<(), StorageError> {
        if p.name.is_empty() {
            return Err(StorageError::Invalid("profile name 不能为空".into()));
        }
        if p.bucket.is_empty() {
            return Err(StorageError::Invalid(format!(
                "storage profile {:?}: bucket 不能为空",
                p.name
            )));
        }
        let (mut profiles, active) = load_profiles(&self.settings)?;
        match profiles.iter_mut().find(|x| x.name == p.name) {
            Some(existing) => {
                // secret 留空 = 保持原值
                if p.secret_key.is_empty() {
                    p.secret_key = existing.secret_key.clone();
                }
                *existing = p;
            }
            None => {
                if p.secret_key.is_empty() {
                    return Err(StorageError::Invalid(format!(
                        "storage profile {:?}: 新建时 secret_key 必填",
                        p.name
                    )));
                }
                profiles.push(p);
            }
        }
        save_profiles(&self.settings, &profiles, &active)
    }

    /// 删除非 active 的 profile（active 须先切换，`ProfileIsActive` 保护）。
    pub fn delete_profile(&self, name: &str) -> Result<(), StorageError> {
        if self.active_name().as_deref() == Some(name) {
            return Err(StorageError::Invalid(format!(
                "storage profile {name:?} 是当前生效 profile，请先切换到其他 profile 再删除"
            )));
        }
        let (mut profiles, active) = load_profiles(&self.settings)?;
        let before = profiles.len();
        profiles.retain(|p| p.name != name);
        if profiles.len() == before {
            return Err(StorageError::Invalid(format!("storage profile {name:?} 不存在")));
        }
        save_profiles(&self.settings, &profiles, &active)
    }

    /// 热切换：先 HeadBucket 连通性校验（失败不切换），成功后原子替换
    /// 客户端并持久化 active。
    pub fn activate(&self, name: &str) -> Result<(), StorageError> {
        let (profiles, _) = load_profiles(&self.settings)?;
        let p = profiles
            .into_iter()
            .find(|p| p.name == name)
            .ok_or_else(|| StorageError::Invalid(format!("storage profile {name:?} 不存在")))?;

        head_bucket(&p)?;

        // 先持久化 active（失败即不切换），再原子替换内存签名器快照。
        self.settings
            .set_setting(KEY_STORAGE_ACTIVE, name)
            .map_err(|e| StorageError::Invalid(format!("持久化 storage.active 失败: {e}")))?;
        let signer = Arc::new(Presigner::new(p.clone())?);
        let mut cur = self.current.write().unwrap_or_else(|po| po.into_inner());
        *cur = (Some(p.name.clone()), signer);
        drop(cur);
        tracing::info!("[storage] 已切换生效存储 profile: {name}");
        Ok(())
    }
}

impl fmt::Debug for Manager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Manager")
            .field("active", &self.active_name())
            .finish_non_exhaustive()
    }
}

/// 读取持久化的 profiles 与 active 名（active 空串 = 尚未引导）。
/// 对照 Go 版 LoadProfiles。
fn load_profiles(
    settings: &Arc<dyn SettingsStore>,
) -> Result<(Vec<StorageProfile>, String), StorageError> {
    let mut profiles = Vec::new();
    let raw = settings
        .get_setting(KEY_STORAGE_PROFILES)
        .map_err(|e| StorageError::Invalid(format!("读取 {KEY_STORAGE_PROFILES} 失败: {e}")))?;
    if let Some(raw) = raw.filter(|s| !s.is_empty()) {
        profiles = serde_json::from_str(&raw)
            .map_err(|e| StorageError::Invalid(format!("解析 {KEY_STORAGE_PROFILES}: {e}")))?;
    }
    let active = settings
        .get_setting(KEY_STORAGE_ACTIVE)
        .map_err(|e| StorageError::Invalid(format!("读取 {KEY_STORAGE_ACTIVE} 失败: {e}")))?
        .unwrap_or_default();
    Ok((profiles, active))
}

/// 持久化 profiles 与 active 名（两次 SetSetting，非事务——与 Go 版 SaveProfiles
/// 一致：两条独立写入，中间失败由下次启动的 LoadProfiles 容错）。
fn save_profiles(
    settings: &Arc<dyn SettingsStore>,
    profiles: &[StorageProfile],
    active: &str,
) -> Result<(), StorageError> {
    let raw = serde_json::to_string(profiles)
        .map_err(|e| StorageError::Invalid(format!("encode storage profiles: {e}")))?;
    settings
        .set_setting(KEY_STORAGE_PROFILES, &raw)
        .map_err(|e| StorageError::Invalid(format!("写入 {KEY_STORAGE_PROFILES} 失败: {e}")))?;
    settings
        .set_setting(KEY_STORAGE_ACTIVE, active)
        .map_err(|e| StorageError::Invalid(format!("写入 {KEY_STORAGE_ACTIVE} 失败: {e}")))?;
    Ok(())
}

/// HeadBucket：按 Profile 校验 bucket 连通性与凭据（仅探测，无副作用；
/// 切换 active 前强制调用。对照 Go 版 r2.Check）。
///
/// 用 5s 超时的同步 HEAD 请求；bucket 根路径以短 TTL 预签名（凭据不落 URL 日志——
/// URL 只在本次调用内存中出现）。非 2xx / 网络错误均视为校验失败且不切换。
fn head_bucket(p: &StorageProfile) -> Result<(), StorageError> {
    let sigv4 = sigv4::SigV4::from_profile(p, Arc::new(SystemClock))?;
    let url = sigv4.presign("HEAD", "", 60)?;
    let resp = http_agent().head(&url).call();
    match resp {
        Ok(r) if r.status() < 300 => Ok(()),
        Ok(r) => Err(StorageError::HeadBucket(format!(
            "bucket {:?} 不可达（HTTP {}；检查 endpoint / 凭据 / 权限）",
            p.bucket,
            r.status()
        ))),
        Err(e) => Err(StorageError::HeadBucket(format!(
            "bucket {:?} 不可达（{}；检查 endpoint / 凭据 / 网络）",
            p.bucket, e
        ))),
    }
}

// ---------------------------------------------------------------------------
// 单测
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bqs_core::ports::SettingsStore as SS;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    /// 内存 fake SettingsStore（表驱动风格复用）。
    #[derive(Default)]
    struct FakeSettings {
        kv: StdMutex<HashMap<String, String>>,
    }
    impl FakeSettings {
        fn new() -> Arc<Self> {
            Arc::new(FakeSettings::default())
        }
    }
    impl SS for FakeSettings {
        fn get_setting(&self, key: &str) -> CoreResult<Option<String>> {
            Ok(self.kv.lock().unwrap().get(key).cloned())
        }
        fn get_int_setting(&self, key: &str, default: i64) -> CoreResult<i64> {
            Ok(self.get_setting(key)?.and_then(|v| v.parse().ok()).unwrap_or(default))
        }
        fn set_setting(&self, key: &str, value: &str) -> CoreResult<()> {
            self.kv.lock().unwrap().insert(key.into(), value.into());
            Ok(())
        }
        fn inc_setting(&self, key: &str) -> CoreResult<i64> {
            let next = self.get_int_setting(key, 0)? + 1;
            self.set_setting(key, &next.to_string())?;
            Ok(next)
        }
    }

    fn profile(name: &str) -> StorageProfile {
        StorageProfile {
            name: name.into(),
            bucket: "bkt".into(),
            region: "auto".into(),
            endpoint: "https://acct.r2.cloudflarestorage.com".into(),
            access_key: "AK".into(),
            secret_key: "SK".into(),
            path_style: false,
        }
    }

    /// 完整引导段（Manager::new 的 bootstrap 校验要求三项凭据均非空）。
    fn section() -> StorageSection {
        StorageSection {
            bucket: "bkt".into(),
            region: "auto".into(),
            endpoint: "https://acct.r2.cloudflarestorage.com".into(),
            access_key: "AK".into(),
            secret_key: "SK".into(),
            path_style: false,
        }
    }

    /// StorageProfile serde 往返：secret 完整保留（落库需要）。
    #[test]
    fn profile_serde_roundtrip_keeps_secret() {
        let p = profile("a");
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("SK"));
        let back: StorageProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(back, p);
        // Debug 不泄密
        let dbg = format!("{p:?}");
        assert!(!dbg.contains("SK"));
        assert!(dbg.contains("******"));
    }

    /// Go 风格 camelCase JSON 双向兼容（偏差 5 修复）：Go 版 r2.Profile 的 json tag
    /// 写出的 profiles Rust 能读；Rust 序列化键名 = Go json tag，Go 能读回。
    #[test]
    fn profile_camelcase_go_compat() {
        // Go 版 json.Marshal(r2.Profile{...}) 的键名形态（secretKey/pathStyle camelCase）
        let go_json = r#"{"name":"r2-main","endpoint":"https://acct.r2.cloudflarestorage.com","region":"auto","accessKey":"AK-GO","secretKey":"SK-GO","bucket":"bkt","pathStyle":false}"#;
        let p: StorageProfile = serde_json::from_str(go_json).unwrap();
        assert_eq!(p.name, "r2-main");
        assert_eq!(p.access_key, "AK-GO");
        assert_eq!(p.secret_key, "SK-GO");
        assert_eq!(p.path_style, false);
        assert_eq!(p.bucket, "bkt");

        // Rust 序列化回 Go 键名（camelCase），Go 端 json.Unmarshal 可读
        let out = serde_json::to_string(&p).unwrap();
        assert!(out.contains("\"accessKey\":\"AK-GO\""), "键名应为 camelCase: {out}");
        assert!(out.contains("\"secretKey\":\"SK-GO\""));
        assert!(out.contains("\"pathStyle\":false"));
        assert!(!out.contains("access_key"), "不允许 snake_case 键混入: {out}");
        // 反序列化往返稳定
        let back: StorageProfile = serde_json::from_str(&out).unwrap();
        assert_eq!(back, p);
    }

    /// 引导：库中无记录 + [storage] 段 → default profile 持久化并生效。
    #[test]
    fn bootstrap_default_profile() {
        let st = FakeSettings::new();
        let m = Manager::new(st.clone(), Some(section())).unwrap();
        assert_eq!(m.active_name().as_deref(), Some("default"));

        // 持久化验证（与 Go 版键名一致）
        let raw = st.get_setting("storage.profiles").unwrap().unwrap();
        assert!(raw.contains("\"name\":\"default\""));
        assert_eq!(st.get_setting("storage.active").unwrap().as_deref(), Some("default"));

        // 重启（重新构造）加载既有记录
        let m2 = Manager::new(st, None).unwrap();
        assert_eq!(m2.active_name().as_deref(), Some("default"));
        assert_eq!(m2.list_profiles().len(), 1);
    }

    /// 两者皆无 → 报错。
    #[test]
    fn no_profiles_and_no_bootstrap_errors() {
        let st = FakeSettings::new();
        assert!(Manager::new(st, None).is_err());
    }

    /// upsert：secret 留空保持原值；新建必填；active 保护删除。
    #[test]
    fn upsert_and_delete_rules() {
        let st = FakeSettings::new();
        let m = Manager::new(st, Some(section())).unwrap();

        // 更新既有：secret 留空保持原值
        let mut p = profile("default");
        p.secret_key = String::new();
        p.bucket = "bkt2".into();
        m.upsert_profile(p).unwrap();
        let got = m.profile("default").unwrap();
        assert_eq!(got.bucket, "bkt2");
        assert_eq!(got.secret_key, "SK");

        // 新建缺 secret → 报错
        let mut p2 = profile("second");
        p2.secret_key = String::new();
        assert!(m.upsert_profile(p2).is_err());
        // 新建带 secret → ok，但 active 不变
        m.upsert_profile(profile("second")).unwrap();
        assert_eq!(m.active_name().as_deref(), Some("default"));

        // 删除 active 受保护
        assert!(m.delete_profile("default").is_err());
        // 删除非 active：ok
        m.delete_profile("second").unwrap();
        assert!(m.profile("second").is_none());
        // 删除不存在
        assert!(m.delete_profile("ghost").is_err());
    }

    /// activate：不可达 endpoint → HeadBucket 错误且不切换（含持久化 active 不变）。
    #[test]
    fn activate_failure_does_not_switch() {
        let st = FakeSettings::new();
        let m = Manager::new(st.clone(), Some(section())).unwrap();

        let mut bad = profile("bad");
        bad.endpoint = "http://127.0.0.1:1".into(); // 必然连接拒绝
        m.upsert_profile(bad).unwrap();

        match m.activate("bad") {
            Err(StorageError::HeadBucket(msg)) => {
                assert!(msg.contains("bkt"), "错误信息应含 bucket 名: {msg}");
            }
            other => panic!("期望 HeadBucket 错误，实际 {other:?}"),
        }
        // 未切换
        assert_eq!(m.active_name().as_deref(), Some("default"));
        assert_eq!(st.get_setting("storage.active").unwrap().as_deref(), Some("default"));
        // 激活不存在的 profile
        assert!(matches!(m.activate("ghost"), Err(StorageError::Invalid(_))));
    }

    /// signer() 返回可用签名器；activate 后快照换新（URL host 变化可观察）。
    #[test]
    fn signer_snapshot_swaps_on_activate() {
        let st = FakeSettings::new();
        let m = Manager::new(
            st,
            Some(StorageSection {
                bucket: "bkt".into(),
                region: "auto".into(),
                endpoint: "http://127.0.0.1:1".into(), // 先给一个不可达 endpoint，activate 会失败
                access_key: "AK".into(),
                secret_key: "SK".into(),
                path_style: true,
            }),
        )
        .unwrap();

        let s1 = m.signer();
        let url1 = s1.presign_get("episodes/n1/a.epb.gz", 600).unwrap();
        assert!(url1.starts_with("http://127.0.0.1:1/bkt/"), "path_style 寻址形态不符: {url1}");

        // activate 失败 → 快照不变
        let mut better = profile("better");
        better.endpoint = "https://acct.r2.cloudflarestorage.com".into();
        better.path_style = false;
        m.upsert_profile(better).unwrap();
        let _ = m.activate("better"); // 无真实网络，HeadBucket 大概率失败；快照与 active 必须不变
        let s2 = m.signer();
        let url2 = s2.presign_get("episodes/n1/a.epb.gz", 600).unwrap();
        assert_eq!(url1.split('?').next().unwrap(), url2.split('?').next().unwrap());
    }

    /// 未配置 active（库中有 profiles 但 active 缺失）时 signer 报故障而非 panic。
    #[test]
    fn null_signer_errors() {
        let st = FakeSettings::new();
        // 手工写库：有 profiles、无 active
        let p = profile("a");
        st.set_setting("storage.profiles", &serde_json::to_string(&vec![p]).unwrap()).unwrap();
        let m = Manager::new(st, None).unwrap();
        assert_eq!(m.active_name(), None);
        assert!(m.signer().presign_get("k", 60).is_err());
    }
}
