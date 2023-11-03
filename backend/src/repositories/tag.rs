use anyhow::Ok;
use axum::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use super::RepositoryError;

#[async_trait]
pub trait TagRepository: Clone + std::marker::Send + std::marker::Sync + 'static {
    async fn create(&self, name: String) -> anyhow::Result<Tag>;
    async fn all(&self) -> anyhow::Result<Vec<Tag>>;
    async fn delete(&self, id: i32) -> anyhow::Result<()>;
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct Tag {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct UpdateTag {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct TagRepositoryForDb {
    pool: PgPool,
}

impl TagRepositoryForDb {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TagRepository for TagRepositoryForDb {
    async fn create(&self, name: String) -> anyhow::Result<Tag> {
        let optional_tag = sqlx::query_as::<_, Tag> (
            r#"
            select * from tags where name = $1
            "#
        )
        .bind(name.clone())
        .fetch_optional(&self.pool)
        .await?;

        if let Some(tag) = optional_tag {
            return  Err(RepositoryError::Duplicate(tag.id).into());
        }

        let tag = sqlx::query_as::<_, Tag>(
            r#"
            insert into tags ( name )
            values ( $1 )
            returning *
            "#
        )
        .bind(name.clone())
        .fetch_one(&self.pool)
        .await?;

    Ok(tag)
    }

    async fn all(&self) -> anyhow::Result<Vec<Tag>>{
        let tags = sqlx::query_as::<_, Tag>(
            r#"
            select * from tags
            order by tags.id asc;
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(tags)
    }

    async fn delete(&self, id: i32) -> anyhow::Result<()>{
        sqlx::query(
            r#"
            delete from tags where id=$1
            "#
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => RepositoryError::NotFound(id),
            _ => RepositoryError::Unexpected(e.to_string()),
        })?;

        Ok(())
    }
}

#[cfg(test)]
#[cfg(feature = "database-test")]
mod test {
    use super::*;
    use dotenv::dotenv;
    use sqlx::PgPool;
    use std::env;

    #[tokio::test]
    async fn crud_scenario() {
        dotenv().ok();

        let database_url = &env::var("DATABASE_URL").expect("undefined [DATABASE_URL]");
        let pool = PgPool::connect(database_url)
            .await
            .expect(&format!("fail connect database, url is [{}]", database_url));

        let repository = TagRepositoryForDb::new(pool);
        let tag_text = "test_tag";

        //create
        let tag = repository
            .create(tag_text.to_string())
            .await
            .expect("[create] returned Err");
        assert_eq!(tag.name, tag_text);

        //delete
        repository
            .delete(tag.id)
            .await
            .expect("[delete] returned Err")
    }
}

#[cfg(test)]
pub mod test_utils {
    use crate::repositories::tag::{TagRepository, RepositoryError};
    use anyhow::Ok;
    use axum::async_trait;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

    use super::Tag;

    impl Tag {
        pub fn new(id: i32, name: String) -> Self {
            Tag { id, name }
        }
    }

    type TagData = HashMap<i32, Tag>;

    #[derive(Debug, Clone)]
    pub struct TagRepositoryForMemory {
        store: Arc<RwLock<TagData>>,
    }

    impl TagRepositoryForMemory {
        pub fn new() -> Self {
            TagRepositoryForMemory { store: Arc::default() }
        }

        fn write_store_ref(&self) -> RwLockWriteGuard<TagData> {
            self.store.write().unwrap()
        }

        fn read_store_ref(&self) -> RwLockReadGuard<TagData> {
            self.store.read().unwrap()
        }
    }

    #[async_trait]
    impl TagRepository for TagRepositoryForMemory {
        async fn create(&self, name: String) -> anyhow::Result<Tag> {
            let mut store = self.write_store_ref();
            if let Some((_key, tag)) = store.iter().find(|(_key, tag)| tag.name == name){
                return  Ok(tag.clone());
            };

            let id = (store.len() + 1) as i32;
            let tag = Tag::new(id, name.clone());
            store.insert(id, tag.clone());
            Ok(tag)
        }

        async fn all(&self) -> anyhow::Result<Vec<Tag>> {
            let mut store = self.read_store_ref();
            let tags = Vec::from_iter(store.values().map(|tag| tag.clone()));
            Ok(tags)
        }

        async fn delete(&self, id: i32) -> anyhow::Result<()> {
            let mut store = self.write_store_ref();
            store.remove(&id).ok_or(RepositoryError::NotFound(id))?;
            Ok(())
        }
    }

    mod test {
        use std::vec;

        use super::{TagRepository, TagRepositoryForMemory};
        use crate::repositories::tag::Tag;

        #[tokio::test]
        async fn tag_crud_scenario() {
            let text = "test_tag".to_string();
            let id = 1;
            let expected = Tag::new(id, text.clone());

            // create
            let repository = TagRepositoryForMemory::new();
            let tag = repository
                .create(text.clone())
                .await
                .expect("failed tag create");
            assert_eq!(expected, tag);

            // all
            let tag = repository.all().await.unwrap();
            assert_eq!(vec![expected], tag);

            // delete
            let res = repository.delete(id).await;
            assert!(res.is_ok())


        }
    }
}