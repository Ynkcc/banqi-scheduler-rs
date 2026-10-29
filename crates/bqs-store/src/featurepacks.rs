//! 【所有权：infra 成员】feature_packs 表端口：特征包登记。
//! SQL 逐条对照 Go 版 internal/store/featurepacks.go。

use bqs_core::ports::{FeaturePackDraft, FeaturePackStore};
use bqs_core::types::FeaturePack;
use bqs_core::CoreResult;

use crate::{Store, StoreError};

use rusqlite::OptionalExtension;

impl FeaturePackStore for Store {
    /// 登记特征包（同 (episode_key, version) 幂等：冲突时保留首行——R2 对象内容
    /// 由 sha256 校验，重复上传无害。对照 Go 版 InsertFeaturePack 的 ON CONFLICT DO NOTHING）。
    fn insert_feature_pack(&self, fp: FeaturePackDraft) -> CoreResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO feature_packs
					(episode_object_key, pack_version, object_key, created_at)
				 VALUES (?,?,?,?)
				 ON CONFLICT(episode_object_key, pack_version) DO NOTHING",
                rusqlite::params![
                    fp.episode_object_key, fp.pack_version, fp.object_key, fp.created_at
                ],
            )
            .map_err(StoreError::from)?;
            Ok(())
        })
    }

    /// 查询某记录批次的特征包；未登记返回 None。
    fn get_feature_pack(
        &self,
        episode_object_key: &str,
        pack_version: i64,
    ) -> CoreResult<Option<FeaturePack>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT object_key, created_at FROM feature_packs
				 WHERE episode_object_key = ? AND pack_version = ?",
                rusqlite::params![episode_object_key, pack_version],
                |row| {
                    Ok(FeaturePack {
                        id: 0,
                        episode_object_key: episode_object_key.to_string(),
                        pack_version,
                        object_key: row.get(0)?,
                        created_at: row.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
        })
    }
}

#[cfg(test)]
mod tests {
    use bqs_core::ports::FeaturePackStore as FPS;
    use bqs_core::ports::FeaturePackDraft;

    #[test]
    fn insert_idempotent_and_get() {
        let (_dir, s) = crate::testutil::open_temp_store();

        FPS::insert_feature_pack(
            &*s,
            FeaturePackDraft {
                episode_object_key: "episodes/n1/a.epb.gz".into(),
                pack_version: 2,
                object_key: "episodes/n1/a.pack.v2.fpb.gz".into(),
                created_at: 123,
            },
        )
        .unwrap();

        // 幂等：同 (episode_key, version) 冲突保留首行
        FPS::insert_feature_pack(
            &*s,
            FeaturePackDraft {
                episode_object_key: "episodes/n1/a.epb.gz".into(),
                pack_version: 2,
                object_key: "episodes/n1/OTHER.fpb.gz".into(),
                created_at: 999,
            },
        )
        .unwrap();

        let fp = FPS::get_feature_pack(&*s, "episodes/n1/a.epb.gz", 2).unwrap().unwrap();
        assert_eq!(fp.object_key, "episodes/n1/a.pack.v2.fpb.gz");
        assert_eq!(fp.created_at, 123);

        // 不同版本互不影响
        FPS::insert_feature_pack(
            &*s,
            FeaturePackDraft {
                episode_object_key: "episodes/n1/a.epb.gz".into(),
                pack_version: 1,
                object_key: "episodes/n1/a.pack.v1.fpb.gz".into(),
                created_at: 124,
            },
        )
        .unwrap();
        assert_eq!(
            FPS::get_feature_pack(&*s, "episodes/n1/a.epb.gz", 1).unwrap().unwrap().object_key,
            "episodes/n1/a.pack.v1.fpb.gz"
        );
        assert!(FPS::get_feature_pack(&*s, "nope", 2).unwrap().is_none());
    }
}
