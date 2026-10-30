use bqs_proto::*;

use crate::{CoreResult, assembler::network_key, consts};

use super::Server;

impl Server {
    pub fn get_network(&self, req: NetworkRequest) -> CoreResult<NetworkInfo> {
        let n = if req.sha.is_empty() {
            self.ports.networks.get_best()?
        } else {
            self.ports.networks.get_network(&req.sha)?
        };
        let Some(n) = n else {
            return Ok(NetworkInfo {
                sha: req.sha,
                ..Default::default()
            });
        };
        let key = network_key(&n.sha, &n.format);
        let url = self.signer.presign_get(&key, self.ttl_secs())?;
        Ok(NetworkInfo {
            sha: n.sha.clone(),
            download_url: url,
            created_at: n.created_at,
            is_best: n.is_best,
            game_data:
                serde_json::json!({ "parent_sha": n.parent_sha.clone().unwrap_or_default() })
                    .to_string(),
            key,
        })
    }

    pub fn register_network(&self, req: RegisterNetworkRequest) -> CoreResult<RegisterNetworkAck> {
        let ack = |accepted: bool, message: String| RegisterNetworkAck {
            accepted,
            message,
            match_task_hint: String::new(),
        };
        let best = self.ports.networks.get_best()?;
        if best.as_ref().is_some_and(|b| b.sha == req.sha) {
            return Ok(ack(false, "sha_equals_current_best".into()));
        }
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
        match best {
            None => {
                self.ports.networks.promote_best(&req.sha)?;
                tracing::info!("[network] first network promoted sha={}", req.sha);
                Ok(ack(true, "first_network_promoted".into()))
            }
            Some(best) => {
                let match_id = self.ports.matches.create_match(
                    &req.sha,
                    &best.sha,
                    self.cfg.selfplay.gatekeeper_games as i64,
                )?;
                tracing::info!(
                    "[gatekeeper] match={} created candidate={} vs best={} target_pairs={}",
                    match_id,
                    req.sha,
                    best.sha,
                    self.cfg.selfplay.gatekeeper_games
                );
                let mut a = ack(true, String::new());
                a.match_task_hint = format!("gatekeeper match {match_id} vs {}", best.sha);
                Ok(a)
            }
        }
    }

    pub fn sign_network_upload(
        &self,
        req: SignNetworkUploadRequest,
    ) -> CoreResult<SignNetworkUploadAck> {
        let ack = |accepted: bool, message: String| SignNetworkUploadAck {
            accepted,
            message,
            upload_url: String::new(),
            object_key: String::new(),
        };
        let sha = req.sha.trim();
        if sha.len() != 64 {
            return Ok(ack(
                false,
                format!("invalid_sha_len={} (want 64 hex chars)", sha.len()),
            ));
        }
        let format = match normalize_format(&req.format) {
            Ok(f) => f,
            Err(msg) => return Ok(ack(false, msg)),
        };
        let key = network_key(sha, &format);
        let url = self.signer.presign_put(&key, self.ttl_secs())?;
        tracing::info!(
            "[network] sign upload trainer={} key={} len={}",
            req.trainer_id,
            key,
            req.content_length
        );
        Ok(SignNetworkUploadAck {
            accepted: true,
            upload_url: url,
            object_key: key,
            ..Default::default()
        })
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
