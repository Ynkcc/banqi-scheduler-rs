use std::sync::Arc;

use bqs_core::server::Server;
use bqs_proto::pb::scheduler_service_server::SchedulerService;
use bqs_proto::*;

pub struct SchedulerSvc(pub Arc<Server>);

fn internal(err: bqs_core::CoreError) -> tonic::Status {
    tonic::Status::internal(err.to_string())
}

fn join_err(e: tokio::task::JoinError) -> tonic::Status {
    tonic::Status::internal(format!("task join: {e}"))
}

macro_rules! forward {
    ($server:expr, $req:expr, $call:ident) => {{
        let server = Arc::clone(&$server);
        let req = $req;
        tokio::task::spawn_blocking(move || server.$call(req))
            .await
            .map_err(join_err)?
            .map_err(internal)
    }};
}

#[tonic::async_trait]
impl SchedulerService for SchedulerSvc {
    async fn get_info(
        &self,
        request: tonic::Request<GetInfoRequest>,
    ) -> Result<tonic::Response<GetInfoReply>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), get_info)?;
        Ok(tonic::Response::new(reply))
    }

    async fn get_task(
        &self,
        request: tonic::Request<TaskRequest>,
    ) -> Result<tonic::Response<TaskResponse>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), get_task)?;
        Ok(tonic::Response::new(reply))
    }

    async fn report_episode(
        &self,
        request: tonic::Request<EpisodeMeta>,
    ) -> Result<tonic::Response<EpisodeAck>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), report_episode)?;
        Ok(tonic::Response::new(reply))
    }

    async fn get_network(
        &self,
        request: tonic::Request<NetworkRequest>,
    ) -> Result<tonic::Response<NetworkInfo>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), get_network)?;
        Ok(tonic::Response::new(reply))
    }

    async fn register_network(
        &self,
        request: tonic::Request<RegisterNetworkRequest>,
    ) -> Result<tonic::Response<RegisterNetworkAck>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), register_network)?;
        Ok(tonic::Response::new(reply))
    }

    async fn report_match_result(
        &self,
        request: tonic::Request<MatchResult>,
    ) -> Result<tonic::Response<MatchResultAck>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), report_match_result)?;
        Ok(tonic::Response::new(reply))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<HeartbeatRequest>,
    ) -> Result<tonic::Response<HeartbeatReply>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), heartbeat)?;
        Ok(tonic::Response::new(reply))
    }

    async fn sign_network_upload(
        &self,
        request: tonic::Request<SignNetworkUploadRequest>,
    ) -> Result<tonic::Response<SignNetworkUploadAck>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), sign_network_upload)?;
        Ok(tonic::Response::new(reply))
    }

    async fn list_episodes(
        &self,
        request: tonic::Request<ListEpisodesRequest>,
    ) -> Result<tonic::Response<ListEpisodesReply>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), list_episodes)?;
        Ok(tonic::Response::new(reply))
    }

    async fn submit_reanalysis(
        &self,
        request: tonic::Request<SubmitReanalysisRequest>,
    ) -> Result<tonic::Response<SubmitReanalysisReply>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), submit_reanalysis)?;
        Ok(tonic::Response::new(reply))
    }

    async fn get_train_config(
        &self,
        request: tonic::Request<GetTrainConfigRequest>,
    ) -> Result<tonic::Response<GetTrainConfigReply>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), get_train_config)?;
        Ok(tonic::Response::new(reply))
    }

    async fn trainer_heartbeat(
        &self,
        request: tonic::Request<TrainerHeartbeatRequest>,
    ) -> Result<tonic::Response<TrainerHeartbeatReply>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), trainer_heartbeat)?;
        Ok(tonic::Response::new(reply))
    }

    async fn report_feature_pack(
        &self,
        request: tonic::Request<FeaturePackMeta>,
    ) -> Result<tonic::Response<FeaturePackAck>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), report_feature_pack)?;
        Ok(tonic::Response::new(reply))
    }

    async fn request_feature_pack(
        &self,
        request: tonic::Request<FeaturePackRequest>,
    ) -> Result<tonic::Response<FeaturePackReply>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), request_feature_pack)?;
        Ok(tonic::Response::new(reply))
    }

    async fn submit_job(
        &self,
        request: tonic::Request<SubmitJobRequest>,
    ) -> Result<tonic::Response<SubmitJobReply>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), submit_job)?;
        Ok(tonic::Response::new(reply))
    }

    async fn list_jobs(
        &self,
        request: tonic::Request<ListJobsRequest>,
    ) -> Result<tonic::Response<ListJobsReply>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), list_jobs)?;
        Ok(tonic::Response::new(reply))
    }

    async fn control_job(
        &self,
        request: tonic::Request<JobControlRequest>,
    ) -> Result<tonic::Response<JobControlReply>, tonic::Status> {
        let reply = forward!(self.0, request.into_inner(), control_job)?;
        Ok(tonic::Response::new(reply))
    }
}
