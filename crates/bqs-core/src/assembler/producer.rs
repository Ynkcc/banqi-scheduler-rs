pub fn network_key(sha: &str, format: &str) -> String {
    format!("networks/{sha}.{format}")
}

pub fn episode_key(network_sha: &str, data_id: &str) -> String {
    format!("episodes/{network_sha}/{data_id}.epb.gz")
}

pub fn games_for_threads(baseline: i32, base: i64, threads: i32) -> i64 {
    if baseline <= 0 {
        return base;
    }
    let t = (threads as i64).max(1);
    let g = base * t / baseline as i64;
    g.max(1)
}
