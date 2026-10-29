//! 【所有权：infra 成员】对象拉取（回放路径专用）：预签名 GET → 限长下载 →
//! gzip 解压。（Go: internal/api/replay.go fetchEpisodeBatch 的传输段。）
//!
//! 全仓网络调用点约定：HeadBucket 之外唯一新增的 HTTP 客户端使用处
//! （复用 ureq）。预签名 URL 只在本次调用内存中出现，不打日志。

use std::io::Read;

use bqs_core::ports::ObjectSigner;
use bqs_core::CoreError;

/// epb 对象（压缩后）大小上限（Go: maxReplayObjectSize = 256MB；防误配后端
/// 拉取异常对象撑爆内存）。
pub const MAX_OBJECT_SIZE: usize = 256 << 20;

/// 拉取/解压失败（HTTP 语义由 api 层统一映射为 502，对齐 Go 版全部归 502）。
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("presign get {key}: {source}")]
    Presign { key: String, source: CoreError },
    #[error("download {key}: {detail}")]
    Download { key: String, detail: String },
    #[error("gunzip {key}: {detail}")]
    Gunzip { key: String, detail: String },
}

/// 下载（限长）+ gzip 解压，返回解压后的 proto 裸字节。
///
/// 与 Go 版一致：压缩体超过 [`MAX_OBJECT_SIZE`] 直接报错（Go 以 LimitReader
/// 截断导致后续解码失败，这里显式提前失败，语义等价且信息更明确）；
/// 无 gzip 魔数（1f 8b）→ 报「不是 gzip 流」。
pub fn fetch_gzip_object(signer: &dyn ObjectSigner, key: &str) -> Result<Vec<u8>, FetchError> {
    // 短 TTL 预签名（与 head_bucket 同口径：URL 不落日志）
    let url = signer
        .presign_get(key, 60)
        .map_err(|source| FetchError::Presign { key: key.to_string(), source })?;
    // 下载可能远慢于 HeadBucket：整体放宽到 120s（Go 版无显式超时）
    let agent = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(120))
        .build();
    let resp = agent
        .get(&url)
        .call()
        .map_err(|e| FetchError::Download { key: key.to_string(), detail: e.to_string() })?;
    let status = resp.status();
    if status >= 300 {
        return Err(FetchError::Download { key: key.to_string(), detail: format!("HTTP {status}") });
    }
    // 限长读压缩体（MAX+1 用于区分「恰好 256MB」与「超限」）
    let mut raw = Vec::new();
    resp.into_reader()
        .take((MAX_OBJECT_SIZE + 1) as u64)
        .read_to_end(&mut raw)
        .map_err(|e| FetchError::Download { key: key.to_string(), detail: e.to_string() })?;
    if raw.len() > MAX_OBJECT_SIZE {
        return Err(FetchError::Download {
            key: key.to_string(),
            detail: format!("对象超过 {} MB 上限", MAX_OBJECT_SIZE >> 20),
        });
    }
    decompress_gzip(&raw, key)
}

/// gzip 解压（多成员流，对齐 Go gzip.NewReader；供单测直接构造输入）。
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
        .map_err(|e| FetchError::Gunzip { key: key.to_string(), detail: e.to_string() })?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// 单测：gzip 往返与限长/魔数语义（不触网；拉取全链路由 bqs-api 契约测试覆盖）
// ---------------------------------------------------------------------------

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
        // 多成员 gzip 流（Go gzip.Reader 支持连接读取）
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
