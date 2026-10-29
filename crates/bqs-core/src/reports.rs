//! 【所有权：core-pipeline 成员】
//!
//! 上报路由链（reportSink）：ReportMatchResult 的「公共闸口 → 逐 sink 尝试 →
//! 链尾兜底」编排。与 taskSource 对称。
//!
//! 参考实现：`banqi-scheduler/internal/scheduler/reports.go`。
//! **保序铁律：先 DB 落库，后置终态**（rollback_report 可回滚）。

use std::sync::Arc;

use bqs_judgment::Hexanomial;
use bqs_proto::{MatchResult, MatchResultAck, TaskKind};

use crate::{
    dispatch::{RunningTask, TaskDispatcher, TaskState},
    evalcoord::EvalCoordinator,
    gatekeeper::Gatekeeper,
    CoreResult,
};

/// 公共闸口：任务查找 + worker 归属校验（ReportEpisode / ReportMatchResult 两条
/// 上报路径共用）。校验失败时返回 `Err(rejection ack)`（业务回包，不报 gRPC
/// 错误，worker 据 ack.message 处置）。
pub fn lookup_report_task(
    disp: &TaskDispatcher,
    task_id: &str,
    worker_id: &str,
) -> Result<RunningTask, MatchResultAck> {
    let Some(task) = disp.lookup(task_id) else {
        return Err(MatchResultAck {
            accepted: false,
            message: format!("unknown_task_id:{task_id}"),
            ..Default::default()
        });
    };
    if task.worker_id != worker_id {
        return Err(MatchResultAck {
            accepted: false,
            message: format!("worker_mismatch task_owner={} got={}", task.worker_id, worker_id),
            ..Default::default()
        });
    }
    Ok(task)
}

/// 上报聚合路由：evalSink（TASK_EVAL，只落 eval_results）→ gateSink（GSPRT 聚合）
/// → 链尾兜底 task_is_not_gating。
pub struct ReportRouter {
    pub evalc: Arc<EvalCoordinator>,
    pub gate: Arc<Gatekeeper>,
    pub disp: Arc<TaskDispatcher>,
}

impl ReportRouter {
    pub fn new(
        evalc: Arc<EvalCoordinator>,
        gate: Arc<Gatekeeper>,
        disp: Arc<TaskDispatcher>,
    ) -> Self {
        Self { evalc, gate, disp }
    }

    fn already_reported(task_id: &str) -> MatchResultAck {
        MatchResultAck {
            accepted: true,
            message: format!("already_reported:{task_id}"),
            ..Default::default()
        }
    }

    /// evalSink：TASK_EVAL 上报聚合，只落 eval_results 并更新停机判据，不参与
    /// GSPRT 与晋级。eval 在飞窗口过期靠重下发兜底（attempts 计数），迟到上报
    /// 不聚合（任务已非 Assigned 态）。
    fn eval_sink(&self, task: &RunningTask, req: &MatchResult) -> CoreResult<Option<MatchResultAck>> {
        if task.kind != TaskKind::TaskEval {
            return Ok(None);
        }
        if task.state != TaskState::Assigned {
            return Ok(Some(Self::already_reported(&req.task_id)));
        }
        Ok(Some(self.evalc.report_eval_result(task, req)?))
    }

    /// gateSink：参与 gatekeeper 聚合的两种任务——显式 TASK_RATING（滚动升级
    /// 期间在飞的旧任务仍可正常结算）与混入模式 TASK_SELFPLAY（MatchID != 0）。
    /// 普通纯自对弈 selfplay（MatchID == 0）没有五项计数，不消费（交链尾）。
    fn gate_sink(&self, task: &RunningTask, req: &MatchResult) -> CoreResult<Option<MatchResultAck>> {
        let is_gating = task.kind == TaskKind::TaskRating
            || (task.kind == TaskKind::TaskSelfplay && task.match_id != 0);
        if !is_gating {
            return Ok(None);
        }
        // CAS 抢占上报权（含 Expired 迟到上报）：失败 = 已有并发/前序上报完成
        if !self.disp.claim_report(&req.task_id) {
            return Ok(Some(Self::already_reported(&req.task_id)));
        }
        // 六桶成对计数（换色配对，互斥对型 [LL, LD, DD, WL, DW, WW]，与 proto
        // pair_* 字段一一对应）
        let delta = Hexanomial([
            req.pair_ll as i64,
            req.pair_ld as i64,
            req.pair_dd as i64,
            req.pair_wl as i64,
            req.pair_dw as i64,
            req.pair_ww as i64,
        ]);
        match self.gate.accept_report(task.match_id, delta, req.games as i64) {
            Ok(d) => Ok(Some(MatchResultAck {
                accepted: d.accepted,
                message: d.message,
                match_concluded: d.concluded,
                promoted: d.promoted,
                best_sha: d.best_sha,
            })),
            Err(e) => {
                // F4-P1-3：落库成功才保持终态；失败回滚，worker 重试可重新抢占
                self.disp.rollback_report(&req.task_id);
                Err(e)
            }
        }
    }

    /// 完整路由：公共闸口 → 幂等快速短路 → 逐 sink 尝试 → 链尾兜底。
    pub fn route(&self, req: &MatchResult) -> CoreResult<MatchResultAck> {
        let task = match lookup_report_task(&self.disp, &req.task_id, &req.worker_id) {
            Ok(t) => t,
            Err(rejected) => return Ok(rejected),
        };
        // 幂等快速短路：终态 Completed 的重复上报直接返回（权威闸口是各 sink 的 CAS）
        if task.state == TaskState::Completed {
            return Ok(Self::already_reported(&req.task_id));
        }
        if let Some(ack) = self.eval_sink(&task, req)? {
            return Ok(ack);
        }
        if let Some(ack) = self.gate_sink(&task, req)? {
            return Ok(ack);
        }
        Ok(MatchResultAck {
            accepted: false,
            message: "task_is_not_gating".into(),
            ..Default::default()
        })
    }
}
