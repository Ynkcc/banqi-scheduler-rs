//! 【所有权：infra 成员】SigV4 查询串预签名（手写实现，纯字符串运算）。
//!
//! 规范依据 AWS Signature Version 4（S3 preset）：
//! - payload 恒为 `UNSIGNED-PAYLOAD`（预签名 PUT/GET 不携带内容哈希；
//!   与 Go 版「不再把对象 sha256 作为 ChecksumSHA256 参与预签名」的取舍一致）；
//! - signed headers 恒为 `host`；
//! - 寻址：path_style=false → virtual-host `https://<bucket>.<host>/<key>`；
//!   path_style=true → path-style `https://<host>/<bucket>/<key>`；
//! - endpoint 可带 scheme（R2 https / MinIO http）；为空时退回 AWS 默认
//!   `s3.amazonaws.com`（https，virtual-host）。

use std::sync::Arc;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use bqs_core::ports::Clock;

use crate::{StorageError, StorageProfile};

type HmacSha256 = Hmac<Sha256>;

/// S3 服务签名预设参数。
const SERVICE: &str = "s3";
const TERMINATOR: &str = "aws4_request";
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

pub(crate) struct SigV4 {
    pub(crate) bucket: String,
    pub(crate) path_style: bool,
    scheme: String,
    /// authority（host[:port]，不含 scheme 与路径）。
    host: String,
    region: String,
    access_key: String,
    secret_key: String,
    clock: Arc<dyn Clock>,
}

impl SigV4 {
    pub(crate) fn from_profile(p: &StorageProfile, clock: Arc<dyn Clock>) -> Result<Self, StorageError> {
        if p.bucket.is_empty() {
            return Err(StorageError::Invalid(format!(
                "storage profile {:?}: bucket 不能为空",
                p.name
            )));
        }
        // endpoint 可带 scheme：https://host[:port] 或 http://host[:port]。
        let (scheme, host) = parse_endpoint(&p.endpoint);
        Ok(SigV4 {
            bucket: p.bucket.clone(),
            path_style: p.path_style,
            scheme,
            host,
            region: if p.region.trim().is_empty() { "auto".into() } else { p.region.trim().into() },
            access_key: p.access_key.clone(),
            secret_key: p.secret_key.clone(),
            clock,
        })
    }

    /// 生成预签名 URL（method：PUT / GET / HEAD；key 为空表示 bucket 根，
    /// 供 HeadBucket 连通性探测）。
    pub(crate) fn presign(&self, method: &str, key: &str, ttl_secs: i64) -> Result<String, StorageError> {
        if !(1..=604800).contains(&ttl_secs) {
            return Err(StorageError::Invalid(format!(
                "预签名 TTL 超出范围 [1, 604800]: {ttl_secs}"
            )));
        }
        let now = self.clock.now();
        let (amz_date, amz_stamp) = amz_formats(now);
        let scope = format!("{}/{}/{}/{}", amz_date, self.region, SERVICE, TERMINATOR);
        let credential = format!("{}/{}", self.access_key, scope);

        // 查询参数（按参数名 ASCII 升序：Algorithm < Credential < Date < Expires < SignedHeaders）。
        let canonical_query = format!(
            "X-Amz-Algorithm={ALGORITHM}\
             &X-Amz-Credential={}\
             &X-Amz-Date={amz_stamp}\
             &X-Amz-Expires={ttl_secs}\
             &X-Amz-SignedHeaders=host",
            percent_encode(&credential, EncodeSet::Query)
        );

        // 规范 URI：path-style 需带桶前缀；对象键按段编码（'/' 保留为分隔符）。
        let canonical_uri = match (self.path_style, key.is_empty()) {
            (_, true) => format!("/{}", if self.path_style { format!("{}/", self.bucket) } else { String::new() }),
            (false, false) => format!("/{}", encode_key(key)),
            (true, false) => format!("/{}/{}", self.bucket, encode_key(key)),
        };

        // 规范请求：METHOD\nURI\nQuery\nheaders\n\nsigned\npayload
        // host 头 = 实际请求的 authority（virtual-host 寻址下含桶前缀）。
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
            self.base_authority()
        );
        let scope_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        let string_to_sign = format!("{ALGORITHM}\n{amz_stamp}\n{scope}\n{scope_hash}");
        let signature = hex::encode(self.sign(&string_to_sign, &amz_date));

        Ok(format!(
            "{}://{}{}?{canonical_query}&X-Amz-Signature={signature}",
            self.scheme,
            self.base_authority(),
            canonical_uri
        ))
    }

    /// URL authority 部分：virtual-host 把桶放进主机名，path-style 保持主机名。
    fn base_authority(&self) -> String {
        if self.path_style {
            self.host.clone()
        } else {
            format!("{}.{}", self.bucket, self.host)
        }
    }

    /// SigV4 派生密钥链：kSecret → kDate → kRegion → kService → kSigning。
    fn sign(&self, string_to_sign: &str, amz_date: &str) -> [u8; 32] {
        let k_date = hmac_key(format!("AWS4{}", self.secret_key).as_bytes(), amz_date.as_bytes());
        let k_region = hmac_key(&k_date, self.region.as_bytes());
        let k_service = hmac_key(&k_region, SERVICE.as_bytes());
        let k_signing = hmac_key(&k_service, TERMINATOR.as_bytes());
        let mut mac = HmacSha256::new_from_slice(&k_signing).expect("hmac key");
        mac.update(string_to_sign.as_bytes());
        let out = mac.finalize().into_bytes();
        let mut sig = [0u8; 32];
        sig.copy_from_slice(&out);
        sig
    }
}

fn hmac_key(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// 解析 endpoint：返回 (scheme, authority)。空 endpoint 退回 AWS 默认。
fn parse_endpoint(endpoint: &str) -> (String, String) {
    let ep = endpoint.trim();
    if ep.is_empty() {
        return ("https".into(), "s3.amazonaws.com".into());
    }
    match ep.split_once("://") {
        Some((scheme, rest)) => (scheme.to_lowercase(), rest.trim_end_matches('/').into()),
        None => ("https".into(), ep.trim_end_matches('/').into()),
    }
}

// ---------------------------------------------------------------------------
// 编码与时间格式
// ---------------------------------------------------------------------------

/// percent-encode 字符集。
enum EncodeSet {
    /// 查询值 / 路径段：仅 AWS 保留字（unreserved）不编码。
    Query,
}

fn is_unreserved(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b'~')
}

fn percent_encode(s: &str, _set: EncodeSet) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if is_unreserved(b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// 对象键按段编码（每段 URI-encode 一次，'/' 保留为分隔符——S3 SigV4 的
/// 「do not double-encode」路径规则）。
fn encode_key(key: &str) -> String {
    key.trim_start_matches('/')
        .split('/')
        .map(|seg| percent_encode(seg, EncodeSet::Query))
        .collect::<Vec<_>>()
        .join("/")
}

/// Unix 秒 → (YYYYMMDD, YYYYMMDDTHHMMSSZ)（UTC，无外部时间库依赖）。
pub(crate) fn amz_formats(unix: i64) -> (String, String) {
    let days = unix.div_euclid(86400);
    let sod = unix.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let date = format!("{y:04}{m:02}{d:02}");
    let stamp = format!(
        "{date}T{:02}{:02}{:02}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    );
    (date, stamp)
}

/// 天数（自 1970-01-01）→ 公历年月日（Howard Hinnant civil_from_days 算法）。
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ---------------------------------------------------------------------------
// 单测：固定向量（时钟/凭据固定，锁定 URL 各组成部分与签名稳定性）
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageProfile;
    use bqs_core::ports::Clock;

    /// 固定时钟：unix 1700000000 = 2023-11-14T22:13:20Z。
    struct FixedClock(i64);
    impl Clock for FixedClock {
        fn now(&self) -> i64 {
            self.0
        }
    }

    fn profile() -> StorageProfile {
        StorageProfile {
            name: "test".into(),
            bucket: "testbucket".into(),
            region: "us-east-1".into(),
            endpoint: "https://s3.example.com".into(),
            access_key: "AKIDEXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            path_style: false,
        }
    }

    fn presigner() -> SigV4 {
        SigV4::from_profile(&profile(), Arc::new(FixedClock(1_700_000_000))).unwrap()
    }

    /// amz_formats：已知时间点换算。
    #[test]
    fn date_format_known_points() {
        assert_eq!(amz_formats(1_700_000_000), ("20231114".into(), "20231114T221320Z".into()));
        assert_eq!(amz_formats(0), ("19700101".into(), "19700101T000000Z".into()));
        // 闰日：2024-02-29T00:00:00Z = 1709164800
        assert_eq!(amz_formats(1_709_164_800).0, "20240229");
        // 年末跨年：2024-12-31T23:59:59Z = 1735689599
        assert_eq!(amz_formats(1_735_689_599).0, "20241231");
        assert_eq!(amz_formats(1_735_689_600).0, "20250101");
    }

    /// 固定向量 v1（virtual-host / R2 形态）：URL 各组成部分稳定。
    #[test]
    fn presign_get_virtual_host_components() {
        let s = presigner();
        let url = s.presign("GET", "episodes/abc123/def.epb.gz", 600).unwrap();
        assert_eq!(
            url.split('?').next().unwrap(),
            "https://testbucket.s3.example.com/episodes/abc123/def.epb.gz"
        );
        let q = url.split('?').nth(1).unwrap();
        let pairs: Vec<&str> = q.split('&').collect();
        // 参数按名升序且齐全
        assert_eq!(pairs.len(), 6);
        assert_eq!(pairs[0], "X-Amz-Algorithm=AWS4-HMAC-SHA256");
        assert_eq!(
            pairs[1],
            "X-Amz-Credential=AKIDEXAMPLE%2F20231114%2Fus-east-1%2Fs3%2Faws4_request"
        );
        assert_eq!(pairs[2], "X-Amz-Date=20231114T221320Z");
        assert_eq!(pairs[3], "X-Amz-Expires=600");
        assert_eq!(pairs[4], "X-Amz-SignedHeaders=host");
        let sig = pairs[5].strip_prefix("X-Amz-Signature=").unwrap();
        assert_eq!(sig.len(), 64);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase() || c.is_ascii_digit()));
    }

    /// 固定向量 v2（path-style / MinIO 形态）：桶在路径段、主机名不带桶、http scheme 保留。
    #[test]
    fn presign_put_path_style_components() {
        let mut p = profile();
        p.endpoint = "http://127.0.0.1:9000".into();
        p.path_style = true;
        let s = SigV4::from_profile(&p, Arc::new(FixedClock(1_700_000_000))).unwrap();
        let url = s.presign("PUT", "networks/sha1.onnx", 3600).unwrap();
        assert_eq!(
            url.split('?').next().unwrap(),
            "http://127.0.0.1:9000/testbucket/networks/sha1.onnx"
        );
        let q = url.split('?').nth(1).unwrap();
        assert!(q.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(q.contains("X-Amz-Credential=AKIDEXAMPLE%2F20231114%2Fus-east-1%2Fs3%2Faws4_request"));
        assert!(q.contains("X-Amz-Date=20231114T221320Z"));
        assert!(q.contains("X-Amz-Expires=3600"));
        assert!(q.contains("X-Amz-SignedHeaders=host"));
        assert!(q.starts_with("X-Amz-Algorithm="));
        assert!(q.contains("&X-Amz-Signature="));
        // 已知答案（Python hmac/hashlib 参考实现，同规范计算）
        let sig = q.rsplit("X-Amz-Signature=").next().unwrap();
        assert_eq!(
            sig, "ea1460a2fb60f51b0388b83f2ff0d75823150db3a4196735f0b835e6347c33f4",
            "known-answer mismatch：path-style PUT 实现偏离 SigV4 规范（签名 {sig}）"
        );
    }

    /// 签名稳定性与敏感性：同一 (时钟, 凭据, 请求) → 签名逐字节一致；
    /// secret / method / TTL 任一变化 → 签名变化（防把常量当实现）。
    #[test]
    fn signature_deterministic_and_sensitive() {
        let s = presigner();
        let u1 = s.presign("GET", "k/1", 600).unwrap();
        let u2 = s.presign("GET", "k/1", 600).unwrap();
        assert_eq!(u1, u2);

        let sig_of = |u: &str| {
            u.split('&').find(|p| p.starts_with("X-Amz-Signature=")).unwrap().to_string()
        };
        let base = sig_of(&u1);

        // method 变化 → 签名变化
        assert_ne!(base, sig_of(&s.presign("PUT", "k/1", 600).unwrap()));
        // TTL 变化 → 签名变化
        assert_ne!(base, sig_of(&s.presign("GET", "k/1", 900).unwrap()));
        // key 变化 → 签名变化
        assert_ne!(base, sig_of(&s.presign("GET", "k/2", 600).unwrap()));
        // secret 变化 → 签名变化
        let mut p2 = profile();
        p2.secret_key = "OTHER".into();
        let s2 = SigV4::from_profile(&p2, Arc::new(FixedClock(1_700_000_000))).unwrap();
        assert_ne!(base, sig_of(&s2.presign("GET", "k/1", 600).unwrap()));
    }

    /// 对象键特殊字符按段编码（'/' 保留；空格 → %20；保留字不双重编码）。
    #[test]
    fn key_encoding() {
        let s = presigner();
        let url = s.presign("GET", "episodes/a b/c~d.e-f_g+h.epb.gz", 60).unwrap();
        let path = url.split('?').next().unwrap();
        assert_eq!(
            path,
            "https://testbucket.s3.example.com/episodes/a%20b/c~d.e-f_g%2Bh.epb.gz"
        );
        // 前导 '/' 容忍
        let url2 = s.presign("GET", "/episodes/x", 60).unwrap();
        assert_eq!(url2.split('?').next().unwrap(), "https://testbucket.s3.example.com/episodes/x");
    }

    /// endpoint 无 scheme → 默认 https；空 endpoint → AWS 默认；TTL 越界报错。
    #[test]
    fn endpoint_and_ttl_validation() {
        let mut p = profile();
        p.endpoint = "minio.local:9000".into();
        let s = SigV4::from_profile(&p, Arc::new(FixedClock(0))).unwrap();
        assert_eq!(s.presign("GET", "k", 60).unwrap().split('?').next().unwrap(),
            "https://testbucket.minio.local:9000/k");

        p.endpoint = String::new();
        let s = SigV4::from_profile(&p, Arc::new(FixedClock(0))).unwrap();
        assert_eq!(s.presign("GET", "k", 60).unwrap().split('?').next().unwrap(),
            "https://testbucket.s3.amazonaws.com/k");

        assert!(s.presign("GET", "k", 0).is_err());
        assert!(s.presign("GET", "k", 604801).is_err());

        let mut p2 = profile();
        p2.bucket = String::new();
        assert!(SigV4::from_profile(&p2, Arc::new(FixedClock(0))).is_err());
    }

    /// 独立参考实现（Python hmac/hashlib 同规范计算）生成的已知答案向量，
    /// 锁死手写实现的正确性（固定凭据/时钟/请求，签名逐字符比对）。
    #[test]
    fn known_answer_vector() {
        // 参考实现：
        //   amz_date=20231114T221320Z, scope=20231114/us-east-1/s3/aws4_request
        //   GET https://testbucket.s3.example.com/episodes/abc123/def.epb.gz, ttl=600
        let s = presigner();
        let url = s.presign("GET", "episodes/abc123/def.epb.gz", 600).unwrap();
        let sig = url.rsplit("X-Amz-Signature=").next().unwrap();
        // 与规范参考实现（Python hmac/hashlib 同规范计算）比对，
        // 若实现偏离 SigV4 规范此断言即失败。
        assert_eq!(
            sig, "d6c940875c380f49aaca0b6c8d2b1a5420f769caee5f426e40030b4bd5d66903",
            "known-answer mismatch：实现偏离 SigV4 规范（签名 {sig}）"
        );
    }
}
