use bqs_proto::*;

use crate::{
    CoreResult, consts,
    featurepack::{DEFAULT_FEATURE_PACK_VERSION, PendingDerive, feature_pack_key},
};

use super::Server;

impl Server {
    pub fn report_feature_pack(&self, req: FeaturePackMeta) -> CoreResult<FeaturePackAck> {
        let ack = |accepted: bool, message: String| FeaturePackAck {
            accepted,
            message,
            upload_url: String::new(),
            object_key: String::new(),
        };
        let mut version = req.pack_version as i64;
        if version <= 0 {
            version = DEFAULT_FEATURE_PACK_VERSION;
        }
        if req.episode_object_key.is_empty() {
            return Ok(ack(false, "missing_episode_object_key".into()));
        }
        let derived = feature_pack_key(&req.episode_object_key, version);

        let key = match self
            .ports
            .feature_packs
            .get_feature_pack(&req.episode_object_key, version)?
        {
            Some(existing) => existing.object_key,
            None => {
                self.ports
                    .feature_packs
                    .insert_feature_pack(crate::ports::FeaturePackDraft {
                        episode_object_key: req.episode_object_key.clone(),
                        pack_version: version,
                        object_key: derived.clone(),
                        created_at: self.clock.now(),
                    })?;
                derived
            }
        };
        let url = self.signer.presign_put(&key, self.ttl_secs())?;

        self.disp.mark_done(&req.task_id);
        tracing::info!(
            "[feature-pack] worker={} task={} episode={} pack_v{} -> {}",
            req.worker_id,
            req.task_id,
            req.episode_object_key,
            version,
            key
        );
        Ok(FeaturePackAck {
            accepted: true,
            upload_url: url,
            object_key: key,
            ..Default::default()
        })
    }

    pub fn request_feature_pack(&self, req: FeaturePackRequest) -> CoreResult<FeaturePackReply> {
        let mut version = req.pack_version as i64;
        if version <= 0 {
            version = DEFAULT_FEATURE_PACK_VERSION;
        }
        if req.episode_object_key.is_empty() {
            return Ok(FeaturePackReply {
                accepted: false,
                message: "missing_episode_object_key".into(),
                ..Default::default()
            });
        }
        if let Some(existing) = self
            .ports
            .feature_packs
            .get_feature_pack(&req.episode_object_key, version)?
        {
            let url = self
                .signer
                .presign_get(&existing.object_key, self.ttl_secs())?;
            return Ok(FeaturePackReply {
                accepted: true,
                ready: true,
                object_key: existing.object_key,
                download_url: url,
                ..Default::default()
            });
        }
        let ok = self.derive.enqueue(PendingDerive {
            episode_object_key: req.episode_object_key.clone(),
            pack_version: version,
            requester: req.requester.clone(),
            created_at: self.clock.now(),
        });
        if !ok {
            return Ok(FeaturePackReply {
                accepted: false,
                message: format!(
                    "特征包补派队列已满（上限 {}），稍后重试",
                    consts::DERIVE_QUEUE_MAX
                ),
                ..Default::default()
            });
        }
        tracing::info!(
            "[feature-pack] 补派入队 requester={} episode={} pack_v{} 队列={}",
            req.requester,
            req.episode_object_key,
            version,
            self.derive.pending()
        );
        Ok(FeaturePackReply {
            accepted: true,
            ready: false,
            message: "queued_for_derivation".into(),
            ..Default::default()
        })
    }
}
