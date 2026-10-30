pub mod fetch;
mod sigv4;

use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bqs_config::StorageSection;
use bqs_core::ports::{Clock, ObjectSigner, SettingsStore, SystemClock};
use bqs_core::{CoreError, CoreResult};

const KEY_STORAGE_PROFILES: &str = "storage.profiles";
const KEY_STORAGE_ACTIVE: &str = "storage.active";

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
            .field(
                "secret_key",
                &if self.secret_key.is_empty() {
                    "(空)"
                } else {
                    "******"
                },
            )
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

pub struct Presigner {
    inner: sigv4::SigV4,
}

impl Presigner {
    pub fn new(profile: StorageProfile) -> Result<Self, StorageError> {
        Self::build(profile, Arc::new(SystemClock))
    }

    pub fn with_clock(
        profile: StorageProfile,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, StorageError> {
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

    pub fn presign_put(&self, key: &str, ttl_secs: i64) -> Result<String, StorageError> {
        self.inner.presign("PUT", key, ttl_secs)
    }

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

impl ObjectSigner for Presigner {
    fn presign_put(&self, key: &str, ttl_secs: i64) -> CoreResult<String> {
        self.presign_put(key, ttl_secs).map_err(StorageError::into)
    }
    fn presign_get(&self, key: &str, ttl_secs: i64) -> CoreResult<String> {
        self.presign_get(key, ttl_secs).map_err(StorageError::into)
    }
}

fn http_agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(5))
        .build()
}

#[derive(Debug, Default)]
struct NullSigner;

impl ObjectSigner for NullSigner {
    fn presign_put(&self, _key: &str, _ttl_secs: i64) -> CoreResult<String> {
        Err(CoreError::Storage(
            "未配置生效的存储 profile（storage.active 为空）".into(),
        ))
    }
    fn presign_get(&self, _key: &str, _ttl_secs: i64) -> CoreResult<String> {
        Err(CoreError::Storage(
            "未配置生效的存储 profile（storage.active 为空）".into(),
        ))
    }
}

pub struct Manager {
    settings: Arc<dyn SettingsStore>,

    current: RwLock<(Option<String>, Arc<dyn ObjectSigner>)>,
}

impl Manager {
    pub fn new(
        settings: Arc<dyn SettingsStore>,
        bootstrap: Option<StorageSection>,
    ) -> Result<Self, StorageError> {
        let (mut profiles, mut active) = load_profiles(&settings)?;

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

        let signer: Arc<dyn ObjectSigner> = match profiles.iter().find(|p| p.name == active) {
            Some(p) => Arc::new(Presigner::new(p.clone())?),
            None => {
                if !active.is_empty() {
                    tracing::warn!(
                        "[storage] storage.active={active:?} 指向不存在的 profile，签名请求将报错"
                    );
                }
                Arc::new(NullSigner)
            }
        };
        Ok(Manager {
            settings,
            current: RwLock::new((Some(active).filter(|s| !s.is_empty()), signer)),
        })
    }

    pub fn signer(&self) -> Arc<dyn ObjectSigner> {
        self.current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .1
            .clone()
    }

    pub fn active_name(&self) -> Option<String> {
        self.current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .0
            .clone()
    }

    pub fn list_profiles(&self) -> Vec<StorageProfile> {
        load_profiles(&self.settings)
            .map(|(p, _)| p)
            .unwrap_or_default()
    }

    pub fn profile(&self, name: &str) -> Option<StorageProfile> {
        self.list_profiles().into_iter().find(|p| p.name == name)
    }

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
            return Err(StorageError::Invalid(format!(
                "storage profile {name:?} 不存在"
            )));
        }
        save_profiles(&self.settings, &profiles, &active)
    }

    pub fn activate(&self, name: &str) -> Result<(), StorageError> {
        let (profiles, _) = load_profiles(&self.settings)?;
        let p = profiles
            .into_iter()
            .find(|p| p.name == name)
            .ok_or_else(|| StorageError::Invalid(format!("storage profile {name:?} 不存在")))?;

        head_bucket(&p)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use bqs_core::ports::SettingsStore as SS;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

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
            Ok(self
                .get_setting(key)?
                .and_then(|v| v.parse().ok())
                .unwrap_or(default))
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

    #[test]
    fn profile_serde_roundtrip_keeps_secret() {
        let p = profile("a");
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("SK"));
        let back: StorageProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(back, p);

        let dbg = format!("{p:?}");
        assert!(!dbg.contains("SK"));
        assert!(dbg.contains("******"));
    }

    #[test]
    fn profile_camelcase_go_compat() {
        let go_json = r#"{"name":"r2-main","endpoint":"https://acct.r2.cloudflarestorage.com","region":"auto","accessKey":"AK-GO","secretKey":"SK-GO","bucket":"bkt","pathStyle":false}"#;
        let p: StorageProfile = serde_json::from_str(go_json).unwrap();
        assert_eq!(p.name, "r2-main");
        assert_eq!(p.access_key, "AK-GO");
        assert_eq!(p.secret_key, "SK-GO");
        assert_eq!(p.path_style, false);
        assert_eq!(p.bucket, "bkt");

        let out = serde_json::to_string(&p).unwrap();
        assert!(
            out.contains("\"accessKey\":\"AK-GO\""),
            "键名应为 camelCase: {out}"
        );
        assert!(out.contains("\"secretKey\":\"SK-GO\""));
        assert!(out.contains("\"pathStyle\":false"));
        assert!(
            !out.contains("access_key"),
            "不允许 snake_case 键混入: {out}"
        );

        let back: StorageProfile = serde_json::from_str(&out).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn bootstrap_default_profile() {
        let st = FakeSettings::new();
        let m = Manager::new(st.clone(), Some(section())).unwrap();
        assert_eq!(m.active_name().as_deref(), Some("default"));

        let raw = st.get_setting("storage.profiles").unwrap().unwrap();
        assert!(raw.contains("\"name\":\"default\""));
        assert_eq!(
            st.get_setting("storage.active").unwrap().as_deref(),
            Some("default")
        );

        let m2 = Manager::new(st, None).unwrap();
        assert_eq!(m2.active_name().as_deref(), Some("default"));
        assert_eq!(m2.list_profiles().len(), 1);
    }

    #[test]
    fn no_profiles_and_no_bootstrap_errors() {
        let st = FakeSettings::new();
        assert!(Manager::new(st, None).is_err());
    }

    #[test]
    fn upsert_and_delete_rules() {
        let st = FakeSettings::new();
        let m = Manager::new(st, Some(section())).unwrap();

        let mut p = profile("default");
        p.secret_key = String::new();
        p.bucket = "bkt2".into();
        m.upsert_profile(p).unwrap();
        let got = m.profile("default").unwrap();
        assert_eq!(got.bucket, "bkt2");
        assert_eq!(got.secret_key, "SK");

        let mut p2 = profile("second");
        p2.secret_key = String::new();
        assert!(m.upsert_profile(p2).is_err());

        m.upsert_profile(profile("second")).unwrap();
        assert_eq!(m.active_name().as_deref(), Some("default"));

        assert!(m.delete_profile("default").is_err());

        m.delete_profile("second").unwrap();
        assert!(m.profile("second").is_none());

        assert!(m.delete_profile("ghost").is_err());
    }

    #[test]
    fn activate_failure_does_not_switch() {
        let st = FakeSettings::new();
        let m = Manager::new(st.clone(), Some(section())).unwrap();

        let mut bad = profile("bad");
        bad.endpoint = "http://127.0.0.1:1".into();
        m.upsert_profile(bad).unwrap();

        match m.activate("bad") {
            Err(StorageError::HeadBucket(msg)) => {
                assert!(msg.contains("bkt"), "错误信息应含 bucket 名: {msg}");
            }
            other => panic!("期望 HeadBucket 错误，实际 {other:?}"),
        }

        assert_eq!(m.active_name().as_deref(), Some("default"));
        assert_eq!(
            st.get_setting("storage.active").unwrap().as_deref(),
            Some("default")
        );

        assert!(matches!(m.activate("ghost"), Err(StorageError::Invalid(_))));
    }

    #[test]
    fn signer_snapshot_swaps_on_activate() {
        let st = FakeSettings::new();
        let m = Manager::new(
            st,
            Some(StorageSection {
                bucket: "bkt".into(),
                region: "auto".into(),
                endpoint: "http://127.0.0.1:1".into(),
                access_key: "AK".into(),
                secret_key: "SK".into(),
                path_style: true,
            }),
        )
        .unwrap();

        let s1 = m.signer();
        let url1 = s1.presign_get("episodes/n1/a.epb.gz", 600).unwrap();
        assert!(
            url1.starts_with("http://127.0.0.1:1/bkt/"),
            "path_style 寻址形态不符: {url1}"
        );

        let mut better = profile("better");
        better.endpoint = "https://acct.r2.cloudflarestorage.com".into();
        better.path_style = false;
        m.upsert_profile(better).unwrap();
        let _ = m.activate("better");
        let s2 = m.signer();
        let url2 = s2.presign_get("episodes/n1/a.epb.gz", 600).unwrap();
        assert_eq!(
            url1.split('?').next().unwrap(),
            url2.split('?').next().unwrap()
        );
    }

    #[test]
    fn null_signer_errors() {
        let st = FakeSettings::new();

        let p = profile("a");
        st.set_setting(
            "storage.profiles",
            &serde_json::to_string(&vec![p]).unwrap(),
        )
        .unwrap();
        let m = Manager::new(st, None).unwrap();
        assert_eq!(m.active_name(), None);
        assert!(m.signer().presign_get("k", 60).is_err());
    }
}
