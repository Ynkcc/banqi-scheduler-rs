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
            message: message.to_string(),
            ..Default::default()
        };

        if !req.client_version.is_empty() {
            tracing::info!(
                worker = %req.worker_id, version = %req.client_version,
                threads = req.threads, memory_mb = req.memory_mb, "[task] GetTask"
            );
        }

        for src in &self.sources {
            if let Some(resp) = src.try_task(&req)? {
                return Ok(resp);
            }
        }

        Ok(none("no_task_available"))
    }

    pub fn heartbeat(&self, req: HeartbeatRequest, ip: &str) -> CoreResult<HeartbeatReply> {
        self.ports.workers.touch_worker(
            &req.worker_id,
            req.current_threads as i64,
            req.completed_games as i64,
            &req.client_version,
            req.memory_mb,
            &req.health_status,
            &req.last_error,
            ip,
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

        let mut reply = HeartbeatReply::default();
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
