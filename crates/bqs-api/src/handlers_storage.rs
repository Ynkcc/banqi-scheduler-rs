use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;

use bqs_storage::{StorageError, StorageProfile};

use crate::views::{OkView, StorageProfileView, StorageView};
use crate::{
    decode_body, to_profile_view, valid_profile_name, ApiError, ApiResult, AppState,
    MAX_CONTROL_BODY,
};

pub async fn storage_get(State(st): State<AppState>) -> ApiResult<Json<StorageView>> {
    let profiles = st.storage.list_profiles();
    let active = st.storage.active_name().unwrap_or_default();
    Ok(Json(StorageView {
        active,
        profiles: profiles.iter().map(to_profile_view).collect(),
    }))
}

pub async fn storage_active_get(State(st): State<AppState>) -> ApiResult<Json<StorageProfileView>> {
    let active = st.storage.active_name().unwrap_or_default();
    profile_view_of(&st, &active).map(Json).ok_or_else(|| {
        ApiError::not_found("尚无生效的存储 profile（首次启动引导未完成或 settings 缺失）")
    })
}

pub async fn profile_get(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<StorageProfileView>> {
    profile_view_of(&st, &name)
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("profile not found: {name}")))
}

fn profile_view_of(st: &AppState, name: &str) -> Option<StorageProfileView> {
    st.storage.profile(name).as_ref().map(to_profile_view)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StorageProfileUpdate {
    endpoint: Option<String>,
    region: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    bucket: Option<String>,
    path_style: Option<bool>,
}

pub async fn profile_put(
    State(st): State<AppState>,
    Path(name): Path<String>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<OkView>> {
    if !valid_profile_name(&name) {
        return Err(ApiError::bad_request(
            "profile 名只允许字母数字与 - _（1~32 字符）",
        ));
    }
    let req: StorageProfileUpdate = decode_body(&bytes, MAX_CONTROL_BODY)?;
    let storage = st.storage.clone();
    tokio::task::spawn_blocking(move || -> Result<(), ApiError> {
        let existing = storage.profile(&name);
        let p = match existing {
            None => {
                if req.secret_key.as_deref().unwrap_or("").is_empty() {
                    return Err(ApiError::bad_request("新建 profile 必须提供 secretKey"));
                }
                StorageProfile {
                    name: name.clone(),
                    ..Default::default()
                }
            }
            Some(p) => p,
        };
        let mut p = p;
        apply_profile_update(&mut p, &req);
        storage
            .upsert_profile(p)
            .map_err(|e| ApiError::internal(e.to_string()))?;
        tracing::info!("[storage] profile={name} updated/created");
        Ok(())
    })
    .await
    .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(OkView { ok: true }))
}

fn apply_profile_update(p: &mut StorageProfile, req: &StorageProfileUpdate) {
    if let Some(v) = &req.endpoint {
        p.endpoint = v.clone();
    }
    if let Some(v) = &req.region {
        p.region = v.clone();
    }
    if let Some(v) = &req.access_key {
        p.access_key = v.clone();
    }
    if let Some(v) = &req.secret_key {
        if !v.is_empty() {
            p.secret_key = v.clone();
        }
    }
    if let Some(v) = &req.bucket {
        p.bucket = v.clone();
    }
    if let Some(v) = req.path_style {
        p.path_style = v;
    }
}

pub async fn profile_delete(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<OkView>> {
    let storage = st.storage.clone();
    tokio::task::spawn_blocking(move || -> Result<(), ApiError> {
        let active = storage.active_name().unwrap_or_default();
        if name == active {
            return Err(ApiError::conflict("不能删除当前生效的 profile，请先切换"));
        }
        if storage.profile(&name).is_none() {
            return Err(ApiError::not_found(format!("profile not found: {name}")));
        }
        storage
            .delete_profile(&name)
            .map_err(|e| ApiError::internal(e.to_string()))?;
        tracing::info!("[storage] profile={name} deleted");
        Ok(())
    })
    .await
    .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(OkView { ok: true }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StorageActivateRequest {
    name: String,
}

pub async fn activate(
    State(st): State<AppState>,
    bytes: axum::body::Bytes,
) -> ApiResult<Json<OkView>> {
    let req: StorageActivateRequest = decode_body(&bytes, MAX_CONTROL_BODY)?;
    let storage = st.storage.clone();
    tokio::task::spawn_blocking(move || -> Result<(), ApiError> {
        if storage.profile(&req.name).is_none() {
            return Err(ApiError::not_found(format!(
                "profile not found: {}",
                req.name
            )));
        }
        if let Err(e) = storage.activate(&req.name) {
            tracing::warn!("[storage] activate profile={} 失败: {e}", req.name);
            return Err(match e {
                StorageError::HeadBucket(_) => {
                    ApiError::bad_gateway(format!("连通性校验失败: {e}"))
                }
                other => ApiError::internal(other.to_string()),
            });
        }
        tracing::info!("[storage] 已切换到 profile={}", req.name);
        Ok(())
    })
    .await
    .map_err(|e| ApiError::internal(format!("join: {e}")))??;
    Ok(Json(OkView { ok: true }))
}
