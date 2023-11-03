mod handlers;
mod repositories;

use crate::repositories::{
    blog::{BlogRepository, BlogRepositoryForDb},
    tag::TagRepository,
};
use axum::{
    extract::Extension,
    routing::{get, post, delete},
    Router,
};
use handlers::{
    blog::{all_blog, create_blog, delete_blog, find_blog, update_blog},
    tag::{all_tag, create_tag, delete_tag}
};
use std::net::SocketAddr;
use std::{env, sync::Arc};
use hyper::header::CONTENT_TYPE;
use sqlx::PgPool;
use dotenv::dotenv;
use tower_http::cors::{
    Any,
    CorsLayer,
    Origin
};
use repositories::tag::TagRepositoryForDb;

#[tokio::main]
async fn main() {
    let log_level = env::var("RUST_LOG").unwrap_or("info".to_string());
    env::set_var("RUST_LOG", log_level);
    tracing_subscriber::fmt::init();
    dotenv().ok();

    let database_url = &env::var("DATABASE_URL").expect("undefined [DATABASE_URL]");
    tracing::debug!("start connect database...");
    let pool = PgPool::connect(database_url)
        .await
        .expect(&format!("fail connect database, url is [{}]", database_url));
    let app = create_app(
        BlogRepositoryForDb::new(pool.clone()),
        TagRepositoryForDb::new(pool.clone())
    );
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

fn create_app<Blog: BlogRepository, Tag: TagRepository>(
    blog_repository: Blog,
    tag_repository: Tag,
) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/blogs", post(create_blog::<Blog>).get(all_blog::<Blog>))
        .route(
            "/blogs/:id",
            get(find_blog::<Blog>)
                .delete(delete_blog::<Blog>)
                .patch(update_blog::<Blog>),
        )
        .route("/tags", post(create_tag::<Tag>).get(all_tag::<Tag>))
        .route("/tag/:id", delete(delete_tag::<Tag>))
        .layer(Extension(Arc::new(blog_repository)))
        .layer(Extension(Arc::new(tag_repository)))
        .layer(
            CorsLayer::new()
                .allow_origin(Origin::exact("http://localhost:3001".parse().unwrap()))
                .allow_methods(Any)
                .allow_headers(vec![CONTENT_TYPE])
        )
}

async fn root() -> &'static str {
    "Hello World"
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::repositories::tag::test_utils::TagRepositoryForMemory;
    use crate::repositories::tag::Tag;
    use crate::repositories::blog::test_utils::BlogRepositoryForMemory;
    use crate::repositories::blog::BlogEntity;
    use axum::response::Response;
    use axum::{
        body::Body,
        http::{header, Method, Request},
    };
    use tower::ServiceExt;

    fn build_blog_req_with_json(path: &str, method: Method, json_body: String) -> Request<Body> {
        Request::builder()
            .uri(path)
            .method(method)
            .header(header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(json_body))
            .unwrap()
    }

    // fn build_blog_req_with_empty(method: Method, path: &str) -> Request<Body> {
    //     Request::builder()
    //         .uri(path)
    //         .method(method)
    //         .body(Body::empty())
    //         .unwrap()
    // }

    async fn res_to_blog(res: Response) -> BlogEntity {
        let bytes = hyper::body::to_bytes(res.into_body()).await.unwrap();
        let body: String = String::from_utf8(bytes.to_vec()).unwrap();
        let blog: BlogEntity = serde_json::from_str(&body)
            .expect(&format!("cannot convert Blog instance. body: {}", body));
        blog
    }

    fn tag_fixture() -> (Vec<Tag>, Vec<i32>) {
        let id = 999;
        (
            vec![Tag {
                id,
                name: String::from("test tag"),
            }],
            vec![id],
        )
    }

    #[tokio::test]
    async fn should_created_blog() {
        let (tags, _tag_ids) = tag_fixture();
        let expected = BlogEntity::new(1, "blog title".to_string(), "blog body".to_string(), tags.clone());

        let req = build_blog_req_with_json(
            "/blogs",
             Method::POST, 
            r#"{
            "title": "blog title",
            "body": "blog body",
            "tags": [999]
            }"#.to_string(),
        );

        let res = create_app(
            BlogRepositoryForMemory::new(tags),
            TagRepositoryForMemory::new(),
        )
        .oneshot(req)
        .await
        .unwrap();
        let blog = res_to_blog(res).await;
        assert_eq!(expected, blog);
    }
}




