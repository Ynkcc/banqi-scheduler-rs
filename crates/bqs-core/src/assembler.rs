use std::sync::Arc;

use bqs_config::Config;
use bqs_proto::{CollectionJobSpec, DataKind, SelfPlayParams, TaskKind, TaskRequest, TaskResponse};

use crate::{
    consts,
    control::Control,
    evalcoord::PendingEval,
    featurepack::PendingDerive,
    ports::{NetworkStore, ObjectSigner},
    reanalysis::PendingReanalysis,
    types::{Match, Network},
    CoreError, CoreResult,
};

pub fn network_key(sha: &str, format: &str) -> String {
    format!("networks/{sha}.{format}")
}

pub fn episode_key(network_sha: &str, data_id: &str) -> String {
    format!("episodes/{network_sha}/{data_id}.epb.gz")
}

pub fn stack_format_for_data_kind(kind: i32) -> Option<&'static str> {
    match kind {
        x if x == DataKind::DataResnet as i32 => Some("onnx"),
        x if x == DataKind::DataNnue as i32 => Some("nnue"),
        _ => None,
    }
}

pub fn stack_data_kind_for_weight_format(format: &str) -> Result<i32, String> {
    match format.trim().to_ascii_lowercase().as_str() {
        "onnx" => Ok(DataKind::DataResnet as i32),
        "nnue" => Ok(DataKind::DataNnue as i32),
        other => Err(format!("未知权重格式 \"{other}\"（可选 onnx / nnue）")),
    }
}

pub fn data_kind_for_format(format: &str) -> i32 {
    if format == "nnue" {
        DataKind::DataNnue as i32
    } else {
        DataKind::DataResnet as i32
    }
}

pub fn games_for_threads(baseline: i32, base: i64, threads: i32) -> i64 {
    if baseline <= 0 {
        return base;
    }
    let t = (threads as i64).max(1);
    let g = base * t / baseline as i64;
    g.max(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfplayMode {
    None,

    Pure,

    Mix,
}

#[derive(Debug, Clone)]
pub struct ProducerSelect {
    pub mode: SelfplayMode,

    pub producer: Option<Network>,

    pub m: Option<Match>,

    pub none_message: String,
}

pub fn select_producer(
    best: Option<&Network>,
    cand: Option<&Network>,
    alt: Option<&Network>,
    pm: Option<&Match>,
    data_kind: i32,
) -> Result<ProducerSelect, String> {
    let Some(best) = best else {
        return Ok(ProducerSelect {
            mode: SelfplayMode::None,
            producer: None,
            m: None,
            none_message: "no_best_network_registered".into(),
        });
    };
    if let (Some(pm), Some(cand)) = (pm, cand) {
        return Ok(ProducerSelect {
            mode: SelfplayMode::Mix,
            producer: Some(cand.clone()),
            m: Some(pm.clone()),
            none_message: String::new(),
        });
    }

    let Some(required_format) = stack_format_for_data_kind(data_kind) else {
        return Err(format!("resolve data kind stack: 未知数据类别 {data_kind}"));
    };
    if pm.is_none() && best.format != required_format {
        let Some(alt) = alt else {
            return Ok(ProducerSelect {
                mode: SelfplayMode::None,
                producer: None,
                m: None,
                none_message: format!("no_{required_format}_network_for_data_kind"),
            });
        };
        return Ok(ProducerSelect {
            mode: SelfplayMode::Pure,
            producer: Some(alt.clone()),
            m: None,
            none_message: String::new(),
        });
    }
    Ok(ProducerSelect {
        mode: SelfplayMode::Pure,
        producer: Some(best.clone()),
        m: None,
        none_message: String::new(),
    })
}

pub struct TaskAssembler {
    nets: Arc<dyn NetworkStore>,
    signer: Arc<dyn ObjectSigner>,
    cfg: Arc<Config>,
    ctl: Arc<Control>,
}

impl TaskAssembler {
    pub fn new(
        nets: Arc<dyn NetworkStore>,
        signer: Arc<dyn ObjectSigner>,
        cfg: Arc<Config>,
        ctl: Arc<Control>,
    ) -> Self {
        Self {
            nets,
            signer,
            cfg,
            ctl,
        }
    }

    fn ttl_secs(&self) -> i64 {
        consts::PRESIGN_TTL.as_secs() as i64
    }

    pub fn extra_config(&self) -> String {
        let mut revealed = self.ctl.initial_revealed();
        if revealed < 0 {
            revealed = 0;
        }
        let mut mix = self.cfg.selfplay.mix_ratio;
        if mix <= 0.0 || mix > 1.0 {
            mix = 0.0;
        }
        if revealed == 0 && mix == 0.0 {
            return String::new();
        }
        let mut obj = serde_json::Map::new();
        if revealed != 0 {
            obj.insert(
                "initial_revealed_pieces".into(),
                serde_json::json!(revealed),
            );
        }
        if mix != 0.0 {
            obj.insert("mix_ratio".into(), serde_json::json!(mix));
        }
        serde_json::Value::Object(obj).to_string()
    }

    pub fn games_for(&self, threads: i32) -> i64 {
        games_for_threads(
            self.cfg.selfplay.threads_baseline,
            self.cfg.selfplay.games_per_task as i64,
            threads,
        )
    }

    fn with_network_download(
        &self,
        req: &TaskRequest,
        resp: &mut TaskResponse,
        sha: &str,
        key: &str,
    ) -> CoreResult<()> {
        if req.current_network != sha {
            resp.network_url = self.signer.presign_get(key, self.ttl_secs())?;
        }
        Ok(())
    }

    pub fn resolve_selfplay(
        &self,
        best: Option<Network>,
        pm: Option<Match>,
        data_kind: i32,
    ) -> CoreResult<ProducerSelect> {
        let mut cand: Option<Network> = None;
        if let Some(ref m) = pm {
            cand = self.nets.get_network(&m.candidate)?;
            if cand.is_none() {
                tracing::warn!(
                    "[task] 混入模式候选记录缺失 sha={}，本批回退纯自对弈",
                    m.candidate
                );
            }
        }

        let mut alt: Option<Network> = None;
        if pm.is_none() {
            if let Some(ref best) = best {
                match stack_format_for_data_kind(data_kind) {
                    Some(required_format) => {
                        if best.format != required_format {
                            alt = self.nets.latest_network_by_format(required_format)?;
                        }
                    }
                    None => {
                        return Err(CoreError::Internal(format!(
                            "resolve data kind stack: 未知数据类别 {data_kind}"
                        )));
                    }
                }
            }
        }
        let sel = select_producer(
            best.as_ref(),
            cand.as_ref(),
            alt.as_ref(),
            pm.as_ref(),
            data_kind,
        )
        .map_err(CoreError::Internal)?;
        if sel.mode == SelfplayMode::Pure {
            if let (Some(best), Some(producer)) = (best.as_ref(), sel.producer.as_ref()) {
                if producer != best {
                    tracing::info!(
                        "[task] best({}) 与数据类别架构不符，纯自对弈改用最新 {} 网络 {}",
                        best.format,
                        producer.format,
                        producer.sha
                    );
                }
            }
        }
        Ok(sel)
    }

    pub fn selfplay_task(
        &self,
        task_id: &str,
        req: &TaskRequest,
        sel: &ProducerSelect,
    ) -> CoreResult<(TaskResponse, i64)> {
        let producer = sel.producer.as_ref().ok_or_else(|| {
            CoreError::Internal("selfplay_task: producer 缺失（应先检查 mode != None）".into())
        })?;
        let key = network_key(&producer.sha, &producer.format);

        let data_kind = data_kind_for_format(&producer.format);
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskSelfplay as i32,
            network_sha: producer.sha.clone(),
            network_sha_remote: producer.sha.clone(),
            games: self.games_for(req.threads) as i32,
            params: Some(SelfPlayParams {
                variant: self.cfg.selfplay.variant.clone(),
                extra_config: self.extra_config(),
                data_kind,
                ..Default::default()
            }),
            network_key: key.clone(),
            ..Default::default()
        };

        let mut match_id = 0i64;
        if sel.mode == SelfplayMode::Mix {
            if let Some(m) = &sel.m {
                if producer.sha != m.opponent {
                    let opp = self.nets.get_network(&m.opponent)?;
                    if let Some(opp) = opp {
                        match_id = m.id;
                        resp.opponent_sha = m.opponent.clone();
                        resp.opponent_key = network_key(&opp.sha, &opp.format);

                        match self.signer.presign_get(&resp.opponent_key, self.ttl_secs()) {
                            Ok(url) => resp.opponent_url = url,
                            Err(e) => tracing::warn!(
                                "[task] presign opponent {} 失败（本地有缓存的 worker 不受影响）: {e}",
                                resp.opponent_key
                            ),
                        }
                    }
                }
            }
        } else if !self.cfg.selfplay.train_match_opponent.is_empty() {
            resp.opponent_spec = self.cfg.selfplay.train_match_opponent.clone();
        }
        self.with_network_download(req, &mut resp, &producer.sha, &key)?;
        Ok((resp, match_id))
    }

    pub fn sentinel_task(&self, task_id: &str, req: &TaskRequest) -> TaskResponse {
        TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskSelfplay as i32,
            network_sha: consts::RULE_SENTINEL_SHA.to_string(),
            network_sha_remote: consts::RULE_SENTINEL_SHA.to_string(),
            games: self.games_for(req.threads) as i32,
            params: Some(SelfPlayParams {
                variant: self.cfg.selfplay.variant.clone(),
                extra_config: self.extra_config(),
                data_kind: DataKind::DataNnue as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn eval_task(
        &self,
        task_id: &str,
        job: &PendingEval,
        games: i64,
        req: &TaskRequest,
    ) -> CoreResult<TaskResponse> {
        let net = self.nets.get_network(&job.network_sha)?.ok_or_else(|| {
            CoreError::Internal(format!("eval network not found sha={}", job.network_sha))
        })?;
        let key = network_key(&net.sha, &net.format);
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskEval as i32,
            network_sha: job.network_sha.clone(),
            network_sha_remote: job.network_sha.clone(),
            opponent_spec: job.opponent_spec.clone(),
            games: games as i32,
            network_key: key.clone(),
            params: Some(SelfPlayParams {
                variant: self.cfg.selfplay.variant.clone(),

                mcts_sims: self.cfg.eval.mcts_sims,

                data_kind: DataKind::DataResnet as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        self.with_network_download(req, &mut resp, &job.network_sha, &key)?;

        if !job.opponent_sha.is_empty() {
            let opp = self.nets.get_network(&job.opponent_sha)?.ok_or_else(|| {
                CoreError::Internal(format!(
                    "eval opponent network not found sha={}",
                    job.opponent_sha
                ))
            })?;
            let opp_key = network_key(&opp.sha, &opp.format);
            resp.opponent_sha = opp.sha.clone();
            resp.opponent_key = opp_key.clone();
            match self.signer.presign_get(&opp_key, self.ttl_secs()) {
                Ok(url) => resp.opponent_url = url,
                Err(e) => tracing::warn!("预签名对手 URL 失败 key={opp_key}: {e}"),
            }
        }
        Ok(resp)
    }

    pub fn reanalysis_task(
        &self,
        task_id: &str,
        item: &PendingReanalysis,
        req: &TaskRequest,
    ) -> CoreResult<TaskResponse> {
        let best = self
            .nets
            .get_best()?
            .ok_or_else(|| CoreError::Internal("no best network registered".into()))?;
        let data_kind = stack_data_kind_for_weight_format(&best.format)
            .map_err(|e| CoreError::Internal(format!("best 网络 format 无法分派重搜栈: {e}")))?;
        let key = network_key(&best.sha, &best.format);
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskReanalysis as i32,
            network_sha: best.sha.clone(),
            network_sha_remote: best.sha.clone(),
            games: item.positions as i32,
            network_key: key.clone(),
            reanalysis_payload: item.payload.clone(),
            params: Some(SelfPlayParams {
                variant: self.cfg.selfplay.variant.clone(),
                mcts_sims: item.mcts_sims,

                data_kind,
                ..Default::default()
            }),
            ..Default::default()
        };
        self.with_network_download(req, &mut resp, &best.sha, &key)?;
        Ok(resp)
    }

    pub fn collect_task(
        &self,
        task_id: &str,
        spec: &CollectionJobSpec,
        games: u64,
        seed: u64,
    ) -> TaskResponse {
        TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskCollect as i32,
            games: games as i32,
            collect_spec: Some(spec.clone()),
            collect_games: games,
            collect_seed: seed,
            params: Some(SelfPlayParams {
                variant: spec.variant.clone(),
                extra_config: self.extra_config(),
                data_kind: spec.data_kind,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn rating_task(
        &self,
        task_id: &str,
        variant: &str,
        side_a: &bqs_proto::CollectPlayerSpec,
        side_b: &bqs_proto::CollectPlayerSpec,
        a_net: Option<&Network>,
        b_net: Option<&Network>,
        games: i64,
        req: &TaskRequest,
    ) -> CoreResult<TaskResponse> {
        let data_kind = a_net
            .or(b_net)
            .map(|n| data_kind_for_format(&n.format))
            .unwrap_or(DataKind::DataResnet as i32);
        let (a_sha, a_key) = match a_net {
            Some(n) => (n.sha.clone(), network_key(&n.sha, &n.format)),
            None => (String::new(), String::new()),
        };
        let mut resp = TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskRating as i32,
            network_sha: a_sha.clone(),
            network_sha_remote: a_sha.clone(),
            games: games as i32,
            network_key: a_key.clone(),
            rating_side_a: Some(side_a.clone()),
            rating_side_b: Some(side_b.clone()),
            params: Some(SelfPlayParams {
                variant: variant.to_string(),
                data_kind,
                ..Default::default()
            }),
            ..Default::default()
        };
        if let Some(a) = a_net {
            self.with_network_download(req, &mut resp, &a.sha, &a_key)?;
        }
        if let Some(opp) = b_net {
            let opp_key = network_key(&opp.sha, &opp.format);
            resp.opponent_sha = opp.sha.clone();
            resp.opponent_key = opp_key.clone();
            match self.signer.presign_get(&opp_key, self.ttl_secs()) {
                Ok(url) => resp.opponent_url = url,
                Err(e) => tracing::warn!(
                    "[task] presign opponent {} 失败（本地有缓存的 worker 不受影响）: {e}",
                    opp_key
                ),
            }
        }
        Ok(resp)
    }

    pub fn derive_task(&self, task_id: &str, item: &PendingDerive) -> CoreResult<TaskResponse> {
        let episode_url = self
            .signer
            .presign_get(&item.episode_object_key, self.ttl_secs())?;
        Ok(TaskResponse {
            task_id: task_id.to_string(),
            kind: TaskKind::TaskDeriveFeatures as i32,
            episode_object_key: item.episode_object_key.clone(),
            episode_url,
            params: Some(SelfPlayParams {
                variant: self.cfg.selfplay.variant.clone(),
                ..Default::default()
            }),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeSigner, Fakes};

    fn net(sha: &str, format: &str) -> Network {
        Network {
            sha: sha.into(),
            format: format.into(),
            created_at: 0,
            is_best: false,
            status: "candidate".into(),
            parent_sha: None,
            notes: None,
        }
    }

    fn matcher(id: i64, opponent: &str) -> Match {
        Match {
            id,
            candidate: "cand".into(),
            opponent: opponent.into(),
            status: "running".into(),
            pairs: bqs_judgment::Hexanomial::ZERO,
            num_games: 0,
            target_games: 400,
        }
    }

    fn assembler(f: &Fakes) -> TaskAssembler {
        let cfg = Arc::new(Config::parse("[eval]\nmcts_sims = 7\n", "test").unwrap());
        let ports = f.ports();
        let ctl = Arc::new(Control::new(&ports, cfg.clone()));
        TaskAssembler::new(f.networks.clone(), Arc::new(FakeSigner), cfg, ctl)
    }

    #[test]
    fn select_producer_table() {
        let best_onnx = net("best-onnx", "onnx");
        let best_nnue = net("best-nnue", "nnue");
        let cand_nnue = net("cand-nnue", "nnue");
        let alt_nnue = net("alt-nnue", "nnue");
        let resnet = bqs_proto::DataKind::DataResnet as i32;
        let nnue = bqs_proto::DataKind::DataNnue as i32;

        struct Case {
            name: &'static str,
            best: Option<Network>,
            cand: Option<Network>,
            alt: Option<Network>,
            pm: Option<Match>,
            data_kind: i32,
            want_mode: SelfplayMode,
            want_sha: &'static str,
            want_nil: bool,
            want_message: &'static str,
        }
        let cases = vec![
            Case {
                name: "无best网络",
                best: None,
                cand: None,
                alt: None,
                pm: None,
                data_kind: resnet,
                want_mode: SelfplayMode::None,
                want_sha: "",
                want_nil: true,
                want_message: "no_best_network_registered",
            },
            Case {
                name: "混入模式_候选产数据",
                best: Some(best_onnx.clone()),
                cand: Some(cand_nnue.clone()),
                alt: None,
                pm: Some(matcher(7, "opp")),
                data_kind: resnet,
                want_mode: SelfplayMode::Mix,
                want_sha: "cand-nnue",
                want_nil: false,
                want_message: "",
            },
            Case {
                name: "混入模式_候选记录缺失回退纯自对弈_架构相符",
                best: Some(best_nnue.clone()),
                cand: None,
                alt: None,
                pm: Some(matcher(7, "opp")),
                data_kind: nnue,
                want_mode: SelfplayMode::Pure,
                want_sha: "best-nnue",
                want_nil: false,
                want_message: "",
            },
            Case {
                name: "混入模式_候选缺失且回退后架构不符_仍用best回退",
                best: Some(best_onnx.clone()),
                cand: None,
                alt: Some(alt_nnue.clone()),
                pm: Some(matcher(7, "opp")),
                data_kind: nnue,
                want_mode: SelfplayMode::Pure,
                want_sha: "best-onnx",
                want_nil: false,
                want_message: "",
            },
            Case {
                name: "纯自对弈_best架构相符",
                best: Some(best_onnx.clone()),
                cand: None,
                alt: None,
                pm: None,
                data_kind: resnet,
                want_mode: SelfplayMode::Pure,
                want_sha: "best-onnx",
                want_nil: false,
                want_message: "",
            },
            Case {
                name: "纯自对弈_架构不符改用最新同格式网络",
                best: Some(best_onnx.clone()),
                cand: None,
                alt: Some(alt_nnue.clone()),
                pm: None,
                data_kind: nnue,
                want_mode: SelfplayMode::Pure,
                want_sha: "alt-nnue",
                want_nil: false,
                want_message: "",
            },
            Case {
                name: "纯自对弈_架构不符且无同格式网络",
                best: Some(best_onnx.clone()),
                cand: None,
                alt: None,
                pm: None,
                data_kind: nnue,
                want_mode: SelfplayMode::None,
                want_sha: "",
                want_nil: true,
                want_message: "no_nnue_network_for_data_kind",
            },
            Case {
                name: "纯自对弈_nnue_best架构相符",
                best: Some(best_nnue.clone()),
                cand: None,
                alt: None,
                pm: None,
                data_kind: nnue,
                want_mode: SelfplayMode::Pure,
                want_sha: "best-nnue",
                want_nil: false,
                want_message: "",
            },
            Case {
                name: "混入优先级高于架构换道",
                best: Some(best_onnx.clone()),
                cand: Some(cand_nnue.clone()),
                alt: Some(alt_nnue.clone()),
                pm: Some(matcher(7, "opp")),
                data_kind: resnet,
                want_mode: SelfplayMode::Mix,
                want_sha: "cand-nnue",
                want_nil: false,
                want_message: "",
            },
        ];
        for c in cases {
            let got = select_producer(
                c.best.as_ref(),
                c.cand.as_ref(),
                c.alt.as_ref(),
                c.pm.as_ref(),
                c.data_kind,
            )
            .unwrap_or_else(|e| panic!("case {}: {e}", c.name));
            assert_eq!(got.mode, c.want_mode, "case {}: mode", c.name);
            if c.want_nil {
                assert!(
                    got.producer.is_none(),
                    "case {}: producer 应为 None",
                    c.name
                );
                assert_eq!(got.none_message, c.want_message, "case {}: message", c.name);
            } else {
                assert_eq!(
                    got.producer.as_ref().unwrap().sha,
                    c.want_sha,
                    "case {}: sha",
                    c.name
                );
            }
            if c.want_mode == SelfplayMode::Mix {
                assert!(got.m.is_some(), "case {}: mix 必须携带 match", c.name);
            } else {
                assert!(got.m.is_none(), "case {}: 非 mix 不得携带 match", c.name);
            }
        }
    }

    #[test]
    fn select_producer_unknown_data_kind_errors() {
        let best = net("s", "onnx");
        assert!(select_producer(Some(&best), None, None, None, 99).is_err());

        assert!(select_producer(
            Some(&best),
            None,
            None,
            None,
            bqs_proto::DataKind::DataNone as i32
        )
        .is_err());
    }

    #[test]
    fn games_for_threads_scaling() {
        assert_eq!(games_for_threads(4, 16, 8), 32, "快设备多打");
        assert_eq!(games_for_threads(4, 16, 2), 8, "慢设备少打");
        assert_eq!(games_for_threads(4, 16, 0), 4, "threads<=0 视为 1");
        assert_eq!(games_for_threads(4, 16, 1), 4);
        assert_eq!(games_for_threads(0, 16, 100), 16, "baseline<=0 不缩放");
        assert_eq!(games_for_threads(-1, 16, 100), 16, "baseline<=0 不缩放");
        assert_eq!(games_for_threads(100, 16, 4), 1, "最小 1");
        assert_eq!(games_for_threads(3, 16, 2), 10);
    }

    #[test]
    fn data_kind_for_format_mapping() {
        assert_eq!(
            data_kind_for_format("onnx"),
            bqs_proto::DataKind::DataResnet as i32
        );
        assert_eq!(
            data_kind_for_format("nnue"),
            bqs_proto::DataKind::DataNnue as i32
        );
        assert_eq!(
            data_kind_for_format("whatever"),
            bqs_proto::DataKind::DataResnet as i32
        );
    }

    #[test]
    fn eval_task_assembly() {
        let f = Fakes::new();
        f.networks.seed_network("n1", None, "onnx");
        let asm = assembler(&f);
        let job = PendingEval {
            network_sha: "n1".into(),
            opponent_spec: "random".into(),
            ..Default::default()
        };

        let req = TaskRequest {
            current_network: "n1".into(),
            threads: 4,
            ..Default::default()
        };
        let resp = asm.eval_task("t1", &job, 10, &req).unwrap();
        assert_eq!(resp.kind, TaskKind::TaskEval as i32);
        assert_eq!(resp.task_id, "t1");
        assert_eq!(resp.network_sha, "n1");
        assert_eq!(resp.network_sha_remote, "n1");
        assert_eq!(resp.opponent_spec, "random");
        assert_eq!(resp.games, 10);
        assert_eq!(resp.network_key, "networks/n1.onnx");
        assert_eq!(resp.network_url, "", "本地已有缓存不应签发 URL");

        assert_eq!(resp.opponent_key, "");
        assert_eq!(resp.opponent_url, "");
        let p = resp.params.unwrap();
        assert_eq!(p.mcts_sims, 7);
        assert_eq!(p.data_kind, bqs_proto::DataKind::DataResnet as i32);
        assert_eq!(p.variant, "4x8");

        let req2 = TaskRequest {
            current_network: String::new(),
            ..Default::default()
        };
        let resp2 = asm.eval_task("t2", &job, 10, &req2).unwrap();
        assert_eq!(resp2.network_url, "get:networks/n1.onnx");

        let missing = PendingEval {
            network_sha: "ghost".into(),
            opponent_spec: "random".into(),
            ..Default::default()
        };
        assert!(asm.eval_task("t3", &missing, 10, &req2).is_err());
    }

    #[test]
    fn reanalysis_task_follows_best_stack() {
        let f = Fakes::new();
        let asm = assembler(&f);
        let item = PendingReanalysis {
            variant: "4x8".into(),
            mcts_sims: 64,
            payload: vec![9, 9],
            positions: 3,
            requester: "trainer".into(),
            created_at: 0,
        };
        let req = TaskRequest {
            current_network: String::new(),
            ..Default::default()
        };

        assert!(asm.reanalysis_task("t1", &item, &req).is_err());

        f.networks.seed_network("best", None, "onnx");
        f.networks.promote_best("best").unwrap();
        let resp = asm.reanalysis_task("t2", &item, &req).unwrap();
        assert_eq!(resp.kind, TaskKind::TaskReanalysis as i32);
        assert_eq!(resp.network_sha, "best");
        assert_eq!(resp.games, 3, "games = 位置条数");
        assert_eq!(resp.reanalysis_payload, vec![9, 9]);
        assert_eq!(resp.network_key, "networks/best.onnx");
        assert_eq!(resp.network_url, "get:networks/best.onnx");
        let p = resp.params.unwrap();
        assert_eq!(p.mcts_sims, 64, "取提交方指定值");
        assert_eq!(
            p.data_kind,
            bqs_proto::DataKind::DataResnet as i32,
            "onnx → MCTS 栈"
        );

        f.networks.seed_network("best2", None, "nnue");
        f.networks.promote_best("best2").unwrap();
        let resp2 = asm.reanalysis_task("t3", &item, &req).unwrap();
        assert_eq!(resp2.network_key, "networks/best2.nnue");
        assert_eq!(
            resp2.params.unwrap().data_kind,
            bqs_proto::DataKind::DataNnue as i32
        );

        f.networks.seed_network("best3", None, "pt");
        f.networks.promote_best("best3").unwrap();
        assert!(asm.reanalysis_task("t4", &item, &req).is_err());

        f.networks.promote_best("best").unwrap();
        let req_local = TaskRequest {
            current_network: "best".into(),
            ..Default::default()
        };
        let resp3 = asm.reanalysis_task("t5", &item, &req_local).unwrap();
        assert_eq!(resp3.network_url, "");
    }

    #[test]
    fn derive_task_assembly() {
        let f = Fakes::new();
        let asm = assembler(&f);
        let item = PendingDerive {
            episode_object_key: "episodes/sha/x.epb.gz".into(),
            pack_version: 1,
            requester: "trainer".into(),
            created_at: 0,
        };
        let resp = asm.derive_task("t1", &item).unwrap();
        assert_eq!(resp.kind, TaskKind::TaskDeriveFeatures as i32);
        assert_eq!(resp.episode_object_key, "episodes/sha/x.epb.gz");
        assert_eq!(
            resp.episode_url, "get:episodes/sha/x.epb.gz",
            "记录批次下载 URL 恒签发"
        );
        assert_eq!(resp.params.unwrap().variant, "4x8");
    }

    #[test]
    fn selfplay_task_mixed_and_pure() {
        let f = Fakes::new();
        f.networks.seed_network("best", None, "onnx");
        f.networks.seed_network("opp", None, "onnx");
        let asm = assembler(&f);

        let sel = ProducerSelect {
            mode: SelfplayMode::Mix,
            producer: Some(net("cand", "nnue")),
            m: Some(matcher(7, "opp")),
            none_message: String::new(),
        };
        let req = TaskRequest {
            threads: 4,
            current_network: "cand".into(),
            ..Default::default()
        };
        let (resp, match_id) = asm.selfplay_task("t1", &req, &sel).unwrap();
        assert_eq!(match_id, 7);
        assert_eq!(resp.network_sha, "cand");
        assert_eq!(resp.network_key, "networks/cand.nnue");
        assert_eq!(
            resp.params.unwrap().data_kind,
            bqs_proto::DataKind::DataNnue as i32,
            "数据类别跟随产数据方（候选）架构"
        );
        assert_eq!(resp.opponent_sha, "opp");
        assert_eq!(resp.opponent_key, "networks/opp.onnx");
        assert_eq!(
            resp.opponent_url, "get:networks/opp.onnx",
            "对手 URL 恒签发"
        );
        assert_eq!(resp.network_url, "", "worker 已有主网络不签 URL");

        let sel_missing = ProducerSelect {
            mode: SelfplayMode::Mix,
            producer: Some(net("cand", "nnue")),
            m: Some(matcher(8, "ghost")),
            none_message: String::new(),
        };
        let (resp2, mid2) = asm.selfplay_task("t2", &req, &sel_missing).unwrap();
        assert_eq!(mid2, 0);
        assert_eq!(resp2.opponent_sha, "");

        let sel_pure = ProducerSelect {
            mode: SelfplayMode::Pure,
            producer: Some(net("best", "onnx")),
            m: None,
            none_message: String::new(),
        };
        let req_fresh = TaskRequest {
            threads: 4,
            current_network: String::new(),
            ..Default::default()
        };
        let (resp3, mid3) = asm.selfplay_task("t3", &req_fresh, &sel_pure).unwrap();
        assert_eq!(mid3, 0);
        assert_eq!(resp3.network_url, "get:networks/best.onnx");
    }

    #[test]
    fn rating_task_assembly() {
        let f = Fakes::new();
        f.networks.seed_network("cand", None, "onnx");
        f.networks.seed_network("opp", None, "nnue");
        let asm = assembler(&f);
        let cand = f.networks.get_network("cand").unwrap().unwrap();
        let opp = f.networks.get_network("opp").unwrap().unwrap();

        let req = TaskRequest {
            current_network: "cand".into(),
            threads: 4,
            ..Default::default()
        };
        let side_a = bqs_proto::CollectPlayerSpec {
            kind: "expectimax_nnue".into(),
            network_sha: "cand".into(),
            search: None,
        };
        let side_b = bqs_proto::CollectPlayerSpec {
            kind: "mcts_resnet".into(),
            network_sha: "opp".into(),
            search: None,
        };
        let resp = asm
            .rating_task(
                "t1",
                "4x8",
                &side_a,
                &side_b,
                Some(&cand),
                Some(&opp),
                8,
                &req,
            )
            .unwrap();
        assert_eq!(resp.kind, TaskKind::TaskRating as i32);
        assert_eq!(resp.network_sha, "cand");
        assert_eq!(resp.network_key, "networks/cand.onnx");
        assert_eq!(resp.network_url, "", "本地已有主网络不签 URL");
        assert_eq!(resp.opponent_sha, "opp");
        assert_eq!(resp.opponent_key, "networks/opp.nnue");
        assert_eq!(
            resp.opponent_url, "get:networks/opp.nnue",
            "对手 URL 恒签发"
        );
        assert_eq!(resp.games, 8);
        let p = resp.params.unwrap();
        assert_eq!(p.variant, "4x8");
        assert_eq!(
            p.data_kind,
            bqs_proto::DataKind::DataResnet as i32,
            "data_kind 跟随候选架构"
        );

        let req2 = TaskRequest {
            current_network: String::new(),
            ..Default::default()
        };
        let resp2 = asm
            .rating_task("t2", "4x8", &side_a, &side_b, Some(&cand), None, 4, &req2)
            .unwrap();
        assert_eq!(resp2.network_url, "get:networks/cand.onnx");
        assert_eq!(resp2.opponent_key, "", "无对手时不签对手键");
    }

    #[test]
    fn sentinel_task_is_nnue_without_network() {
        let f = Fakes::new();
        let asm = assembler(&f);
        let req = TaskRequest {
            threads: 4,
            ..Default::default()
        };
        let resp = asm.sentinel_task("t1", &req);
        assert_eq!(resp.kind, TaskKind::TaskSelfplay as i32);
        assert_eq!(resp.network_sha, consts::RULE_SENTINEL_SHA);
        assert_eq!(resp.network_key, "", "哨兵任务不签发任何权重对象键");
        assert_eq!(
            resp.params.unwrap().data_kind,
            bqs_proto::DataKind::DataNnue as i32
        );
    }

    #[test]
    fn extra_config_json_semantics() {
        let f = Fakes::new();
        let cfg = Arc::new(Config::parse("[selfplay]\nmix_ratio = 0.25\n", "test").unwrap());
        let ports = f.ports();
        let ctl = Arc::new(Control::new(&ports, cfg.clone()));
        let asm = TaskAssembler::new(f.networks.clone(), Arc::new(FakeSigner), cfg, ctl);
        assert_eq!(asm.extra_config(), r#"{"mix_ratio":0.25}"#);

        let cfg2 = Arc::new(
            Config::parse(
                "[selfplay]\nmix_ratio = 1.5\ninitial_revealed = -3\n",
                "test",
            )
            .unwrap(),
        );
        let ports2 = f.ports();
        let ctl2 = Arc::new(Control::new(&ports2, cfg2.clone()));
        let asm2 = TaskAssembler::new(f.networks.clone(), Arc::new(FakeSigner), cfg2, ctl2);
        assert_eq!(asm2.extra_config(), "", "无有效字段时为空串");
    }
}
