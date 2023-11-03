use axum::{
    extract::{Extension, Path},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use validator::Validate;

use crate::repositories::tag::TagRepository;

use super::ValidatedJson;

pub async fn create_tag<T: TagRepository>(
    ValidatedJson(payload): ValidatedJson<CreateTag>,
    Extension(repository): Extension<Arc<T>>
) -> Result<impl IntoResponse, StatusCode> {
    let tag = repository
        .create(payload.name)
        .await
        .or(Err(StatusCode::INTERNAL_SERVER_ERROR))?;

    Ok((StatusCode::CREATED, Json(tag)))
}

pub async fn all_tag<T: TagRepository>(
    Extension(repository): Extension<Arc<T>>
) -> Result<impl IntoResponse, StatusCode> {
    let tags = repository
        .all()
        .await
        .unwrap();

    Ok((StatusCode::OK, Json(tags)))
}

pub async fn delete_tag<T: TagRepository>(
    Path(id): Path<i32>,
    Extension(repository): Extension<Arc<T>>
) -> StatusCode {
    repository
        .delete(id)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Validate)]
pub struct CreateTag {
    #[validate(length(min = 1, message = "Can not be empty"))]
    #[validate(length(max = 100, message = "Over text length"))]
    name: String,
}