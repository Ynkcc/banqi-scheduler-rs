use std::io::Read;

use bqs_core::ports::ObjectSigner;
use bqs_core::CoreError;

pub const MAX_OBJECT_SIZE: usize = 256 << 20;

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("presign get {key}: {source}")]
    Presign { key: String, source: CoreError },
    #[error("download {key}: {detail}")]
    Download { key: String, detail: String },
    #[error("gunzip {key}: {detail}")]
    Gunzip { key: String, detail: String },
}

pub fn fetch_object(
    signer: &dyn ObjectSigner,
    key: &str,
    timeout_secs: u64,
) -> Result<Vec<u8>, FetchError> {
    let url = signer
        .presign_get(key, 60)
        .map_err(|source| FetchError::Presign {
            key: key.to_string(),
            source,
        })?;

    let agent = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .build();
    let resp = agent.get(&url).call().map_err(|e| FetchError::Download {
        key: key.to_string(),
        detail: e.to_string(),
    })?;
    let status = resp.status();
    if status >= 300 {
        return Err(FetchError::Download {
            key: key.to_string(),
            detail: format!("HTTP {status}"),
        });
    }

    let mut raw = Vec::new();
    resp.into_reader()
        .take((MAX_OBJECT_SIZE + 1) as u64)
        .read_to_end(&mut raw)
        .map_err(|e| FetchError::Download {
            key: key.to_string(),
            detail: e.to_string(),
        })?;
    if raw.len() > MAX_OBJECT_SIZE {
        return Err(FetchError::Download {
            key: key.to_string(),
            detail: format!("对象超过 {} MB 上限", MAX_OBJECT_SIZE >> 20),
        });
    }
    Ok(raw)
}

pub fn fetch_gzip_object(signer: &dyn ObjectSigner, key: &str) -> Result<Vec<u8>, FetchError> {
    let url = signer
        .presign_get(key, 60)
        .map_err(|source| FetchError::Presign {
            key: key.to_string(),
            source,
        })?;

    let agent = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(120))
        .build();
    let resp = agent.get(&url).call().map_err(|e| FetchError::Download {
        key: key.to_string(),
        detail: e.to_string(),
    })?;
    let status = resp.status();
    if status >= 300 {
        return Err(FetchError::Download {
            key: key.to_string(),
            detail: format!("HTTP {status}"),
        });
    }

    let mut raw = Vec::new();
    resp.into_reader()
        .take((MAX_OBJECT_SIZE + 1) as u64)
        .read_to_end(&mut raw)
        .map_err(|e| FetchError::Download {
            key: key.to_string(),
            detail: e.to_string(),
        })?;
    if raw.len() > MAX_OBJECT_SIZE {
        return Err(FetchError::Download {
            key: key.to_string(),
            detail: format!("对象超过 {} MB 上限", MAX_OBJECT_SIZE >> 20),
        });
    }
    decompress_gzip(&raw, key)
}

pub fn decompress_gzip(raw: &[u8], key: &str) -> Result<Vec<u8>, FetchError> {
    const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
    if raw.len() < 2 || raw[..2] != GZIP_MAGIC {
        return Err(FetchError::Gunzip {
            key: key.to_string(),
            detail: format!("对象 {key} 不是 gzip 流（epb.gz）"),
        });
    }
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(raw)
        .read_to_end(&mut out)
        .map_err(|e| FetchError::Gunzip {
            key: key.to_string(),
            detail: e.to_string(),
        })?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn gzip_roundtrip_and_multimember() {
        let a = gzip(b"hello");
        let b = gzip(b" world");

        let mut joined = a.clone();
        joined.extend_from_slice(&b);
        let out = decompress_gzip(&joined, "k").unwrap();
        assert_eq!(out, b"hello world");
        let out = decompress_gzip(&a, "k").unwrap();
        assert_eq!(out, b"hello");
    }

    #[test]
    fn non_gzip_rejected() {
        let err = decompress_gzip(b"plain proto bytes", "episodes/x.epb.gz").unwrap_err();
        match err {
            FetchError::Gunzip { detail, .. } => assert!(detail.contains("不是 gzip 流")),
            other => panic!("期望 Gunzip 错误，实际 {other:?}"),
        }
    }

    #[test]
    fn truncated_gzip_rejected() {
        let full = gzip(&vec![7u8; 4096]);
        let err = decompress_gzip(&full[..full.len() - 8], "k").unwrap_err();
        assert!(matches!(err, FetchError::Gunzip { .. }));
    }
}
