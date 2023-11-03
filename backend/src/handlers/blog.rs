use axum::{
    extract::{Extension, Path},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::sync::Arc;

use crate::repositories::blog::{CreateBlog, BlogRepository, UpdateBlog};

use super::ValidatedJson;

pub async fn create_blog<T: BlogRepository>(
    ValidatedJson(payload): ValidatedJson<CreateBlog>,
    Extension(repository): Extension<Arc<T>>
) -> Result<impl IntoResponse, StatusCode> {
    let blog = repository
        .create(payload)
        .await
        .or(Err(StatusCode::NOT_FOUND))?;

    Ok((StatusCode::CREATED, Json(blog)))
}

pub async fn find_blog<T: BlogRepository>(
    Path(id): Path<i32>,
    Extension(repository): Extension<Arc<T>>,
) -> Result<impl IntoResponse, StatusCode> {
    let blog = repository.find(id).await.or(Err(StatusCode::NOT_FOUND))?;
    Ok((StatusCode::OK, Json(blog)))
}

pub async fn all_blog<T: BlogRepository>(
    Extension(repository): Extension<Arc<T>>,
) -> Result<impl IntoResponse, StatusCode> {
    let blog = repository.all().await.unwrap();
    Ok((StatusCode::OK, Json(blog)))
}

pub async fn update_blog<T: BlogRepository>(
    Path(id): Path<i32>,
    ValidatedJson(payload): ValidatedJson<UpdateBlog>,
    Extension(repository): Extension<Arc<T>>,
) -> Result<impl IntoResponse, StatusCode> {
    let blog = repository
        .update(id, payload)
        .await
        .or(Err(StatusCode::NOT_FOUND))?;
    Ok((StatusCode::CREATED, Json(blog)))
}

pub async fn delete_blog<T: BlogRepository>(
    Path(id): Path<i32>,
    Extension(repository): Extension<Arc<T>>,
) -> StatusCode {
    repository
        .delete(id)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .unwrap_or(StatusCode::NOT_FOUND)
}
