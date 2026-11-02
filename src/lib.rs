pub mod adapters;
pub mod application;
pub mod config;
pub mod domain;
pub mod port;

pub use application::EntityService;
pub use config::AppConfig;
pub use domain::{ConfigSchemaMigrator, EntityId, NewEntity, Cursor};
pub use port::{Cache, EventBus, Storage};

use std::sync::Arc;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone)]
pub struct Pagination {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Deserialize, Clone)]
pub struct CursorPagination {
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Serialize)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}

#[derive(Serialize)]
pub struct CursorResponse<T> {
    pub data: Vec<T>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Clone)]
pub struct AppState {
    pub service: Arc<EntityService>,
}

pub fn create_app_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/api/v1/entities/cursor", get(get_all_cursor))
        .route("/api/v1/entities", get(get_all).post(add))
        .route("/api/v1/entities/:id", get(get_by_id).put(update).delete(del))
        .with_state(state)
}

async fn get_all(
    State(s): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<impl IntoResponse, AppError> {
    let offset = p.offset.unwrap_or(0);
    let limit = p.limit.unwrap_or(100);
    let (data, total) = s.service.get_all_paginated(offset, limit).await?;
    Ok(Json(PaginatedResponse { data, total, offset, limit }))
}

async fn get_all_cursor(
    State(s): State<AppState>,
    Query(p): Query<CursorPagination>,
) -> Result<impl IntoResponse, AppError> {
    let limit = p.limit.unwrap_or(100);
    let (data, next_cursor, has_more) = s.service.get_all_cursor(p.cursor, limit).await?;
    Ok(Json(CursorResponse { data, next_cursor, has_more }))
}

async fn get_by_id(
    State(s): State<AppState>,
    Path(id): Path<EntityId>,
) -> Result<impl IntoResponse, AppError> {
    Ok(Json(s.service.get(&id).await?))
}

async fn add(
    State(s): State<AppState>,
    Json(data): Json<NewEntity>,
) -> Result<impl IntoResponse, AppError> {
    Ok((StatusCode::CREATED, Json(s.service.add(data).await?)))
}

async fn update(
    State(s): State<AppState>,
    Path(id): Path<EntityId>,
    Json(data): Json<NewEntity>,
) -> Result<impl IntoResponse, AppError> {
    Ok(Json(s.service.update(id, data).await?))
}

async fn del(
    State(s): State<AppState>,
    Path(id): Path<EntityId>,
) -> Result<impl IntoResponse, AppError> {
    if s.service.delete(id).await? {
        Ok(StatusCode::NO_CONTENT.into_response())
    } else {
        Err(AppError(domain::DomainError::NotFound))
    }
}

pub struct AppError(pub domain::DomainError);

impl From<domain::DomainError> for AppError {
    fn from(err: domain::DomainError) -> Self {
        AppError(err)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match self.0 {
            domain::DomainError::NotFound => (StatusCode::NOT_FOUND, "Not Found"),
            domain::DomainError::ValidationError(_) => (StatusCode::BAD_REQUEST, "Bad Request"),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "Internal"),
        };
        (status, msg).into_response()
    }
}
