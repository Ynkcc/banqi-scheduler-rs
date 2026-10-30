use std::sync::Arc;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use bqs_core::ports::Clock;

use crate::{StorageError, StorageProfile};

type HmacSha256 = Hmac<Sha256>;

const SERVICE: &str = "s3";
const TERMINATOR: &str = "aws4_request";
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

pub(crate) struct SigV4 {
    pub(crate) bucket: String,
    pub(crate) path_style: bool,
    scheme: String,

    host: String,
    region: String,
    access_key: String,
    secret_key: String,
    clock: Arc<dyn Clock>,
}

impl SigV4 {
    pub(crate) fn from_profile(
        p: &StorageProfile,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, StorageError> {
        if p.bucket.is_empty() {
            return Err(StorageError::Invalid(format!(
                "storage profile {:?}: bucket 不能为空",
                p.name
            )));
        }

        let (scheme, host) = parse_endpoint(&p.endpoint);
        Ok(SigV4 {
            bucket: p.bucket.clone(),
            path_style: p.path_style,
            scheme,
            host,
            region: if p.region.trim().is_empty() {
                "auto".into()
            } else {
                p.region.trim().into()
            },
            access_key: p.access_key.clone(),
            secret_key: p.secret_key.clone(),
            clock,
        })
    }

    pub(crate) fn presign(
        &self,
        method: &str,
        key: &str,
        ttl_secs: i64,
    ) -> Result<String, StorageError> {
        if !(1..=604800).contains(&ttl_secs) {
            return Err(StorageError::Invalid(format!(
                "预签名 TTL 超出范围 [1, 604800]: {ttl_secs}"
            )));
        }
        let now = self.clock.now();
        let (amz_date, amz_stamp) = amz_formats(now);
        let scope = format!("{}/{}/{}/{}", amz_date, self.region, SERVICE, TERMINATOR);
        let credential = format!("{}/{}", self.access_key, scope);

        let canonical_query = format!(
            "X-Amz-Algorithm={ALGORITHM}\
             &X-Amz-Credential={}\
             &X-Amz-Date={amz_stamp}\
             &X-Amz-Expires={ttl_secs}\
             &X-Amz-SignedHeaders=host",
            percent_encode(&credential, EncodeSet::Query)
        );

        let canonical_uri = match (self.path_style, key.is_empty()) {
            (_, true) => format!(
                "/{}",
                if self.path_style {
                    format!("{}/", self.bucket)
                } else {
                    String::new()
                }
            ),
            (false, false) => format!("/{}", encode_key(key)),
            (true, false) => format!("/{}/{}", self.bucket, encode_key(key)),
        };

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

    fn base_authority(&self) -> String {
        if self.path_style {
            self.host.clone()
        } else {
            format!("{}.{}", self.bucket, self.host)
        }
    }

    fn sign(&self, string_to_sign: &str, amz_date: &str) -> [u8; 32] {
        let k_date = hmac_key(
            format!("AWS4{}", self.secret_key).as_bytes(),
            amz_date.as_bytes(),
        );
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

enum EncodeSet {
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

fn encode_key(key: &str) -> String {
    key.trim_start_matches('/')
        .split('/')
        .map(|seg| percent_encode(seg, EncodeSet::Query))
        .collect::<Vec<_>>()
        .join("/")
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageProfile;
    use bqs_core::ports::Clock;

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

    #[test]
    fn date_format_known_points() {
        assert_eq!(
            amz_formats(1_700_000_000),
            ("20231114".into(), "20231114T221320Z".into())
        );
        assert_eq!(
            amz_formats(0),
            ("19700101".into(), "19700101T000000Z".into())
        );

        assert_eq!(amz_formats(1_709_164_800).0, "20240229");

        assert_eq!(amz_formats(1_735_689_599).0, "20241231");
        assert_eq!(amz_formats(1_735_689_600).0, "20250101");
    }

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
        assert!(sig
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase() || c.is_ascii_digit()));
    }

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
        assert!(
            q.contains("X-Amz-Credential=AKIDEXAMPLE%2F20231114%2Fus-east-1%2Fs3%2Faws4_request")
        );
        assert!(q.contains("X-Amz-Date=20231114T221320Z"));
        assert!(q.contains("X-Amz-Expires=3600"));
        assert!(q.contains("X-Amz-SignedHeaders=host"));
        assert!(q.starts_with("X-Amz-Algorithm="));
        assert!(q.contains("&X-Amz-Signature="));

        let sig = q.rsplit("X-Amz-Signature=").next().unwrap();
        assert_eq!(
            sig, "ea1460a2fb60f51b0388b83f2ff0d75823150db3a4196735f0b835e6347c33f4",
            "known-answer mismatch：path-style PUT 实现偏离 SigV4 规范（签名 {sig}）"
        );
    }

    #[test]
    fn signature_deterministic_and_sensitive() {
        let s = presigner();
        let u1 = s.presign("GET", "k/1", 600).unwrap();
        let u2 = s.presign("GET", "k/1", 600).unwrap();
        assert_eq!(u1, u2);

        let sig_of = |u: &str| {
            u.split('&')
                .find(|p| p.starts_with("X-Amz-Signature="))
                .unwrap()
                .to_string()
        };
        let base = sig_of(&u1);

        assert_ne!(base, sig_of(&s.presign("PUT", "k/1", 600).unwrap()));

        assert_ne!(base, sig_of(&s.presign("GET", "k/1", 900).unwrap()));

        assert_ne!(base, sig_of(&s.presign("GET", "k/2", 600).unwrap()));

        let mut p2 = profile();
        p2.secret_key = "OTHER".into();
        let s2 = SigV4::from_profile(&p2, Arc::new(FixedClock(1_700_000_000))).unwrap();
        assert_ne!(base, sig_of(&s2.presign("GET", "k/1", 600).unwrap()));
    }

    #[test]
    fn key_encoding() {
        let s = presigner();
        let url = s
            .presign("GET", "episodes/a b/c~d.e-f_g+h.epb.gz", 60)
            .unwrap();
        let path = url.split('?').next().unwrap();
        assert_eq!(
            path,
            "https://testbucket.s3.example.com/episodes/a%20b/c~d.e-f_g%2Bh.epb.gz"
        );

        let url2 = s.presign("GET", "/episodes/x", 60).unwrap();
        assert_eq!(
            url2.split('?').next().unwrap(),
            "https://testbucket.s3.example.com/episodes/x"
        );
    }

    #[test]
    fn endpoint_and_ttl_validation() {
        let mut p = profile();
        p.endpoint = "minio.local:9000".into();
        let s = SigV4::from_profile(&p, Arc::new(FixedClock(0))).unwrap();
        assert_eq!(
            s.presign("GET", "k", 60)
                .unwrap()
                .split('?')
                .next()
                .unwrap(),
            "https://testbucket.minio.local:9000/k"
        );

        p.endpoint = String::new();
        let s = SigV4::from_profile(&p, Arc::new(FixedClock(0))).unwrap();
        assert_eq!(
            s.presign("GET", "k", 60)
                .unwrap()
                .split('?')
                .next()
                .unwrap(),
            "https://testbucket.s3.amazonaws.com/k"
        );

        assert!(s.presign("GET", "k", 0).is_err());
        assert!(s.presign("GET", "k", 604801).is_err());

        let mut p2 = profile();
        p2.bucket = String::new();
        assert!(SigV4::from_profile(&p2, Arc::new(FixedClock(0))).is_err());
    }

    #[test]
    fn known_answer_vector() {
        let s = presigner();
        let url = s.presign("GET", "episodes/abc123/def.epb.gz", 600).unwrap();
        let sig = url.rsplit("X-Amz-Signature=").next().unwrap();

        assert_eq!(
            sig, "d6c940875c380f49aaca0b6c8d2b1a5420f769caee5f426e40030b4bd5d66903",
            "known-answer mismatch：实现偏离 SigV4 规范（签名 {sig}）"
        );
    }
}
