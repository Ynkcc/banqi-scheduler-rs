use bqs_proto::*;

use crate::{consts, CoreResult};

use super::Server;

impl Server {
    pub fn register_network(&self, req: RegisterNetworkRequest) -> CoreResult<RegisterNetworkAck> {
        let ack = |accepted: bool, message: String| RegisterNetworkAck {
            accepted,
            message,
            match_task_hint: String::new(),
        };
        let format = match normalize_format(&req.format) {
            Ok(f) => f,
            Err(msg) => return Ok(ack(false, msg)),
        };
        let created = self.ports.networks.register_network(
            &req.sha,
            (!req.parent_sha.is_empty()).then_some(req.parent_sha.as_str()),
            (!req.notes.is_empty()).then_some(req.notes.as_str()),
            &format,
        )?;
        if !created {
            return Ok(ack(false, format!("duplicate_sha:{}", req.sha)));
        }
        tracing::info!("[network] registered candidate={} format={}", req.sha, format);
        Ok(ack(true, "network_registered".into()))
    }
}

fn normalize_format(format: &str) -> Result<String, String> {
    let f = format.trim().to_ascii_lowercase();
    if consts::WEIGHT_FORMATS.contains(&f.as_str()) {
        Ok(f)
    } else {
        Err(format!(
            "unsupported_format=\"{format}\" (want onnx/pt/nnue)"
        ))
    }
}
