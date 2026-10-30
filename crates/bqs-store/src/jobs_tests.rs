//! JobStore 适配器（jobs / job_tasks 表）集成单测：真实 SQLite 走一遍
//! 登记 → 派发记账（seq 推进）→ 映射 → 聚合（含排序与过滤）。

#[cfg(test)]
mod tests {
    use bqs_core::ports::{JobRow, JobStore, JobTaskLink};

    fn row(job_id: &str, kind: i64, priority: i64, state: &str, created_at: i64) -> JobRow {
        JobRow {
            job_id: job_id.into(),
            kind,
            priority,
            state: state.into(),
            total_games: 100,
            completed_games: 0,
            dispatched_games: 0,
            spec_json: "{}".into(),
            created_at,
            updated_at: created_at,
            next_seq: 0,
        }
    }

    #[test]
    fn job_lifecycle_and_task_links() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(dir.path().join("t.db").to_str().unwrap()).unwrap();

        // 登记 + 排序：priority DESC、created_at ASC
        store.insert_job(row("j-low-1", 0, 50, "running", 2)).unwrap();
        store.insert_job(row("j-low-2", 0, 50, "running", 1)).unwrap();
        store.insert_job(row("j-high", 2, 100, "running", 3)).unwrap();
        let all = store.list_jobs(None, None).unwrap();
        let ids: Vec<&str> = all.iter().map(|j| j.job_id.as_str()).collect();
        assert_eq!(ids, vec!["j-high", "j-low-2", "j-low-1"], "优先级 DESC、created_at ASC");
        let collect = store.list_jobs(None, Some(0)).unwrap();
        assert_eq!(collect.len(), 2);
        let running = store.list_jobs(Some("running"), None).unwrap();
        assert_eq!(running.len(), 3);

        // 派发记账：dispatched += games 且 seq +1（单事务）
        let r = store.incr_job_dispatched("j-low-1", 32, 10).unwrap();
        assert_eq!(r.dispatched_games, 32);
        assert_eq!(r.next_seq, 1);
        assert_eq!(r.updated_at, 10);

        // 映射：插入 / 反查 / 删除
        store.insert_job_task("t1", "j-low-1", 32).unwrap();
        assert_eq!(
            store.get_job_task("t1").unwrap(),
            Some(JobTaskLink { task_id: "t1".into(), job_id: "j-low-1".into(), games: 32 })
        );
        store.delete_job_task("t1").unwrap();
        assert_eq!(store.get_job_task("t1").unwrap(), None);

        // 聚合：completed += games（单事务）；达标后由调用方置终态
        let r = store.incr_job_completed("j-low-1", 64, 20).unwrap();
        assert_eq!(r.completed_games, 64);
        assert_eq!(r.dispatched_games, 32, "聚合不影响派发记账");
        assert!(store.update_job_state("j-low-1", "completed", 30).unwrap());
        assert_eq!(store.get_job("j-low-1").unwrap().unwrap().state, "completed");

        // 控制面过滤：state=completed 只剩收敛的 job
        let done = store.list_jobs(Some("completed"), None).unwrap();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].job_id, "j-low-1");

        // 幂等防重：主键冲突（insert_job 重复）报错而非静默覆盖
        assert!(store.insert_job(row("j-low-1", 0, 50, "running", 2)).is_err());
    }
}
