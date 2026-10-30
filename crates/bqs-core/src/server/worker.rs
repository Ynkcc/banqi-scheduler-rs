use bqs_proto::*;

use crate::{CoreError, CoreResult, variants};

use super::Server;

impl Server {
    pub fn get_info(&self, _req: GetInfoRequest) -> CoreResult<GetInfoReply> {
        let variant = &self.cfg.selfplay.variant;
        let meta = variants::lookup(variant).ok_or_else(|| {
            CoreError::Internal(format!(
                "变体 {variant} 未在 bq-env 变体表登记（应启动即失败）"
            ))
        })?;
        let (should_stop, stop_reason) = self.ctl.should_stop();
        Ok(GetInfoReply {
            variant: variant.clone(),
            variant_meta: Some(VariantMeta {
                id: meta.id.clone(),
                label: meta.label.clone(),
                rows: meta.rows,
                cols: meta.cols,
                piece_counts: meta.piece_counts.clone(),
                piece_values: meta.piece_values.clone(),
                initial_health: meta.initial_health,
                max_consecutive_moves_for_draw: meta.max_consecutive_moves_for_draw,
                symmetries: meta.symmetries.clone(),
            }),
            should_stop,
            stop_reason,
        })
    }

    pub fn get_task(&self, req: TaskRequest) -> CoreResult<TaskResponse> {
        self.disp.prune();

        let none = |message: &str| TaskResponse {
            kind: TaskKind::TaskNone as i32,
            message: message.to_string(),
            ..Default::default()
        };

        let min = &self.cfg.server.min_client_version;
        if !min.is_empty()
            && !req.client_version.is_empty()
            && client_version_too_old(&req.client_version, min)
        {
            return Ok(none("client_version_too_old"));
        }
        if !req.client_version.is_empty() {
            tracing::info!(
                worker = %req.worker_id, version = %req.client_version,
                threads = req.threads, memory_mb = req.memory_mb, "[task] GetTask"
            );
        }

        self.balance.evaluate(self.clock.now());

        for src in &self.sources {
            if let Some(resp) = src.try_task(&req)? {
                return Ok(resp);
            }
        }

        if self.ctl.manual_paused() {
            return Ok(none("self_play_paused"));
        }

        Ok(none("no_task_available"))
    }

    pub fn heartbeat(&self, req: HeartbeatRequest) -> CoreResult<HeartbeatReply> {
        self.ports.workers.touch_worker(
            &req.worker_id,
            req.current_threads as i64,
            req.completed_games as i64,
            &req.client_version,
            req.memory_mb,
            &req.health_status,
            &req.last_error,
        )?;
        if !req.client_version.is_empty() {
            if let Ok(Some(v)) = self.ports.workers.worker_version(&req.worker_id) {
                if !v.is_empty() && v != req.client_version {
                    tracing::info!(
                        "[worker] {} version changed {} -> {}",
                        req.worker_id,
                        v,
                        req.client_version
                    );
                }
            }
        }
        let best = self.ports.networks.get_best()?;

        let mut reply = HeartbeatReply {
            pause_self_play: self.ctl.effective_paused(),
            ..Default::default()
        };
        if let Some(best) = best {
            reply.best_network = best.sha;
        }

        reply.cancel_task_ids = self.jobs.cancel_task_ids_for_worker(&req.worker_id);
        if !reply.cancel_task_ids.is_empty() {
            tracing::info!(
                "[job] 心跳下发在途取消 worker={} tasks={:?}",
                req.worker_id,
                reply.cancel_task_ids
            );
        }
        Ok(reply)
    }
}

fn client_version_too_old(client: &str, min: &str) -> bool {
    let parse = |s: &str| -> Option<Vec<Option<u64>>> {
        let mut parts = Vec::new();
        for seg in s.split('.') {
            match seg.parse::<u64>() {
                Ok(n) => parts.push(Some(n)),
                Err(_) => return None,
            }
        }
        Some(parts)
    };
    match (parse(client), parse(min)) {
        (Some(c), Some(m)) => {
            let len = c.len().max(m.len());
            for i in 0..len {
                let cv = c.get(i).copied().flatten().unwrap_or(0);
                let mv = m.get(i).copied().flatten().unwrap_or(0);
                if cv != mv {
                    return cv < mv;
                }
            }
            false
        }

        _ => client < min,
    }
}
