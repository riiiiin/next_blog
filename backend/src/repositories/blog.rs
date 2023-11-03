use std::vec;

use anyhow::Ok;
use axum::async_trait;
use serde::{Deserialize, Serialize};
use validator::Validate;
use sqlx::{
    FromRow,
    PgPool
};

use super::{
    RepositoryError,
    tag::Tag
};


//共通の振る舞いを定義する
#[async_trait]
pub trait BlogRepository: Clone + std::marker::Send + std::marker::Sync + 'static {
    async fn create(&self, payload: CreateBlog) -> anyhow::Result<BlogEntity>;
    async fn find(&self, id: i32) -> anyhow::Result<BlogEntity>;
    async fn all(&self) -> anyhow::Result<Vec<BlogEntity>>;
    async fn update(&self, id: i32, payload: UpdateBlog) -> anyhow::Result<BlogEntity>;
    async fn delete(&self, id: i32) -> anyhow::Result<()>;
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, FromRow)]
pub struct BlogWithTagFromRow {
    pub id: i32,
    pub title: String,
    pub body: String,
    pub label_id: Option<i32>,
    pub tag_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, FromRow)]
pub struct BlogEntity {
    pub id: i32,
    pub title: String,
    pub body: String,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Validate)]
pub struct CreateBlog {
    #[validate(length(min=1, message="can not be empty"))]
    #[validate(length(max=100, message="Over text length"))]
    pub title: String,
    pub body: String,
    pub tags: Vec<i32>,
}


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Validate)]
pub struct UpdateBlog {
    #[validate(length(min=1, message="can not be empty"))]
    #[validate(length(max=100, message="Over text length"))]
    pub title: Option<String>,
    pub body: Option<String>,
    pub tags: Option<Vec<i32>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Validate, FromRow)]
pub struct BlogFromRow {
    id: i32,
    title: String,
    body: String,
}

#[derive(Debug, Clone)]
pub struct BlogRepositoryForDb {
    pool: PgPool,
}

impl BlogRepositoryForDb {
    pub fn new(pool: PgPool) -> Self {
        BlogRepositoryForDb { pool }
    }
}

fn fold_entities(rows: Vec<BlogWithTagFromRow>) -> Vec<BlogEntity> {
    let mut rows = rows.iter();
    let mut accum: Vec<BlogEntity> = vec![];
    'outer: while let Some(row) = rows.next() {
        let mut blogs = accum.iter_mut();
        while let Some(blog) = blogs.next() {
            if blog.id == row.id {
                blog.tags.push(Tag {
                    id: row.label_id.unwrap(),
                    name: row.tag_name.clone().unwrap(),
                });
                continue 'outer;
            }
        }

        let tags = if row.label_id.is_some() {
            vec![Tag {
                id: row.label_id.unwrap(),
                name:row.tag_name.clone().unwrap(),
            }]
        } else {
            vec![]
        };

        accum.push(BlogEntity { id: row.id, title: row.title.clone(), body: row.body.clone(), tags })
    }
    accum
}

#[async_trait]
impl BlogRepository for BlogRepositoryForDb {
    async fn create(&self, payload: CreateBlog) -> anyhow::Result<BlogEntity> {
        let tx = self.pool.begin().await?;
        let row = sqlx::query_as::<_, BlogFromRow>(
            r#"
            insert into blogs (title, body)
            values ($1, $2)
            returning *
            "#
        )
        .bind(payload.title.clone())
        .bind(payload.body.clone())
        .fetch_one(&self.pool)
        .await?;

        sqlx::query(
            r#"
            insert into blog_tags (blog_id, label_id)
            select $1, id
            from unnest($2) as t(id);
            "#
        )
        .bind(row.id)
        .bind(payload.tags)
        .execute(&self.pool)
        .await?;

        tx.commit().await?;
        
        let blog = self.find(row.id).await?;
        Ok(blog)
    }

    async fn find(&self, id: i32) -> anyhow::Result<BlogEntity> {
        let items = sqlx::query_as::<_, BlogWithTagFromRow>(
            r#"
            select blogs.*, tags.id as label_id, tags.name as tag_name
            from blogs
                    left outer join blog_tags tl on blogs.id = tl.
            blog_id
                    left outer join tags on tags.id = tl.label_id
            where blogs.id=$1
            "#
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => RepositoryError::NotFound(id),
            _ => RepositoryError::Unexpected(e.to_string()),
        })?;

        let blogs = fold_entities(items);
        let blog = blogs.first().ok_or(RepositoryError::NotFound(id))?;
        Ok(blog.clone())
    }

    async fn all(&self) -> anyhow::Result<Vec<BlogEntity>> {
        let blogs = sqlx::query_as::<_, BlogWithTagFromRow>(
            r#"
            select blogs.*, tags.id as label_id, tags.name as tag_name
            from blogs
                    left outer join blog_tags tl on blogs.id = tl.blog_id
                    left outer join tags on tags.id = tl.label_id
            order by blogs.id desc;
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(fold_entities(blogs))
    }

    async fn update(&self, id: i32, payload: UpdateBlog) -> anyhow::Result<BlogEntity> {
        let tx = self.pool.begin().await?;

        let old_blog = self.find(id).await?;
        sqlx::query(
            r#"
            update blogs set title=$1, body=$2
            where id=$3
            returning *
            "#
        )
        .bind(payload.title.unwrap_or(old_blog.title))
        .bind(payload.body.unwrap_or(old_blog.body))
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        if let Some(tags) = payload.tags{
            sqlx::query(
                r#"
                delete from blog_tags where blog_id=$1
                "#
            )
            .bind(id)
            .execute(&self.pool)
            .await?;

            sqlx::query(
                r#"
                insert into blog_tags (blog_id, label_id)
                select $1, id
                from unnest($2) as t(id);
                "#
            )
            .bind(id)
            .bind(tags)
            .execute(&self.pool)
            .await?;
        };

        tx.commit().await?;
        let blog = self.find(id).await?;
        Ok(blog)
    }

    async fn delete(&self, id: i32) -> anyhow::Result<()> {
        let tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            delete from blog_tags where blog_id=$1
            "#
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => RepositoryError::NotFound(id),
            _ => RepositoryError::Unexpected(e.to_string())
        })?;

        sqlx::query(
            r#"
            delete from blogs where id=$1
            "#
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => RepositoryError::NotFound(id),
            _ => RepositoryError::Unexpected(e.to_string())
        })?;

        tx.commit().await?;

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

    #[test]
    fn fold_entities_test() {
        let tag_1 = Tag {
            id: 1,
            name: String::from("tag 1"),
        };
        let tag_2 = Tag {
            id: 2,
            name: String::from("tag 2"),
        };
        let rows = vec![
            BlogWithTagFromRow {
                id: 1,
                title: String::from("Blog 1"),
                body: String::from("Blog 1"),
                label_id: Some(tag_1.id),
                tag_name: Some(tag_1.name.clone()),
            },
            BlogWithTagFromRow {
                id: 1,
                title: String::from("Blog 1"),
                body: String::from("Blog 1"),
                label_id: Some(tag_2.id),
                tag_name: Some(tag_2.name.clone()),
            },
            BlogWithTagFromRow {
                id: 2,
                title: String::from("Blog 2"),
                body: String::from("Blog 2"),
                label_id: Some(tag_1.id),
                tag_name: Some(tag_1.name.clone()),
            },
        ];

        let res = fold_entities(rows);
        assert_eq!(
            res,
            vec![
                BlogEntity {
                    id: 1,
                    title: String::from("Blog 1"),
                    body: String::from("Blog 1"),
                    tags: vec![tag_1.clone(), tag_2.clone()],
                },
                BlogEntity {
                    id: 2,
                    title: String::from("Blog 2"),
                    body: String::from("Blog 2"),
                    tags: vec![tag_1.clone()],
                },
            ]
        )
    }

    #[tokio::test]
    async fn crud_scenario() {
        let database_url = &env::var("DATABASE_URL").expect("undefined [DATABASE_URL]");
        let pool = PgPool::connect(database_url)
            .await
            .expect(&format!("fail connect database, url is [{}]", database_url));

        // tag data prepare
        let tag_name = String::from("test tag");
        let optional_tag = sqlx::query_as::<_, Tag>(
            r#"
            select * from tags where name=$1
            "#
        )
        .bind(tag_name.clone())
        .fetch_optional(&pool)
        .await
        .expect("Failed to prepare label data.");

        let tag_1 = if let Some(tag) = optional_tag {
            tag
        } else {
            let tag = sqlx::query_as::<_, Tag>(
                r#"
                insert into tags ( name )
                values ( $1 )
                returning *
                "#
            )
            .bind(tag_name)
            .fetch_one(&pool)
            .await
            .expect("Failed to insert label data.");
            
            tag
        };

        let repository = BlogRepositoryForDb::new(pool.clone());
        let blog_title = "[crud_scenario] title";
        let blog_body = "[crud_scenario] body";

        //create
        let created = repository
            .create(CreateBlog::new(blog_title.to_string(), blog_body.to_string(), vec![tag_1.id]))
            .await
            .expect("[create] returned Err");
        assert_eq!(created.title, blog_title);
        assert_eq!(created.body, blog_body);
        assert_eq!(*created.tags.first().unwrap(), tag_1);

        //find
        let blog = repository
            .find(created.id)
            .await
            .expect("[find] returned Err");
        assert_eq!(created, blog);

        //all
        let blogs = repository
            .all()
            .await
            .expect("[all] returned Err");
        let blog = blogs.first().unwrap();
        assert_eq!(created, *blog);

        //update
        let update_title = "[crud_scenario] updated title";
        let update_body = "[crud_scenario] updated body";
        let blog = repository
            .update(
                blog.id,
                UpdateBlog {
                    title: Some(update_title.to_string()),
                    body: Some(update_body.to_string()),
                    tags: Some(vec![])
                }
            )
            .await
            .expect("[update] returned Err");
        assert_eq!(created.id, blog.id);
        assert_eq!(blog.title, update_title.clone());
        assert_eq!(blog.body, update_body.clone());
        assert!(blog.tags.len() == 0);

        //delete
        let _ = repository
            .delete(blog.id)
            .await
            .expect("[delete] returned Err");
        let res = repository.find(created.id).await;
        assert!(res.is_err());

        let blog_rows = sqlx::query(
            r#"
            select * from blogs where id=$1
            "#
        )
        .bind(blog.id)
        .fetch_all(&pool)
        .await
        .expect("[delete] todo_labels fetch error");
        assert!(blog_rows.len() == 0);

        let rows = sqlx::query(
            r#"
            select * from blog_tags where blog_id=$1
            "#
        )
        .bind(blog.id)
        .fetch_all(&pool)
        .await
        .expect("[delete] todo_labels fetch error");
        assert!(rows.len() == 0);
    }

}

#[cfg(test)]
pub mod test_utils {
    use anyhow::Context;
    use axum::async_trait;
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    };

    use super::*;

    impl BlogEntity {
        pub fn new(id: i32, title: String, body: String, tags: Vec<Tag>) -> Self {
            Self { id, title, body, tags }
        }
    }

    // impl CreateBlog {
    //     pub fn new(title: String, body: String, tags: Vec<i32>) -> Self {
    //         Self { title, body, tags }
    //     }
    // }

    type BlogDatas = HashMap<i32, BlogEntity>;

    #[derive(Debug, Clone)]
    pub struct BlogRepositoryForMemory {
        store: Arc<RwLock<BlogDatas>>,
        tags: Vec<Tag>,
    }

    //メソッド定義
    impl BlogRepositoryForMemory {
        pub fn new(tags: Vec<Tag>) -> Self {
            BlogRepositoryForMemory { store: Arc::default(), tags }
        }

        fn write_store_ref(&self) -> RwLockWriteGuard<BlogDatas> {
            self.store.write().unwrap()
        }
        fn read_store_ref(&self) -> RwLockReadGuard<BlogDatas> {
            self.store.read().unwrap()
        }

        fn resolve_tags(&self, tags: Vec<i32>) -> Vec<Tag> {
            let mut tag_list = self.tags.iter().cloned();
            let tags = tags
                .iter()
                .map(|id| tag_list.find(|tag| tag.id == *id).unwrap())
                .collect();
            tags
        }
    }


    #[async_trait]
    impl BlogRepository for BlogRepositoryForMemory {
        async fn create(&self, payload: CreateBlog) -> anyhow::Result<BlogEntity> {
            let mut store = self.write_store_ref();
            let id = (store.len() + 1) as i32;
            let tags = self.resolve_tags(payload.tags);
            let blog = BlogEntity::new(id, payload.title.clone(), payload.body.clone(), tags);
            store.insert(id, blog.clone());
            Ok(blog)
        }

        async fn find(&self, id: i32) -> anyhow::Result<BlogEntity> {
            let store = self.read_store_ref();
            let blog = store
                .get(&id)
                .map(|blog| blog.clone())
                .ok_or(RepositoryError::NotFound(id))?;
            Ok(blog)
        }

        async fn all(&self) -> anyhow::Result<Vec<BlogEntity>>{
            let store = self.read_store_ref();
            Ok(Vec::from_iter(store.values().map(|blog| blog.clone())))
        }

        async fn update(&self, id: i32, payload: UpdateBlog) -> anyhow::Result<BlogEntity> {
            let mut store = self.write_store_ref();
            let blog = store
                .get(&id)
                .context(RepositoryError::NotFound(id))?;
            let title = payload.title.unwrap_or(blog.title.clone());
            let body = payload.body.unwrap_or(blog.body.clone());
            let tags = match payload.tags {
                Some(tag_ids) => self.resolve_tags(tag_ids),
                None => blog.tags.clone(),
            };
            let blog = BlogEntity {
                id,
                title,
                body,
                tags
            };
            store.insert(id, blog.clone());
            Ok(blog)
        }

        async fn delete(&self, id: i32) -> anyhow::Result<()> {
            let mut store = self.write_store_ref();
            store.remove(&id).ok_or(RepositoryError::NotFound(id))?;
            Ok(())
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;
    
        #[tokio::test]
        async fn blog_crud_scenario() {
            let title = "blog title".to_string();
            let body = "blog body".to_string();
            let id = 1;
            let tag_data = Tag {
                id: 1,
                name: String::from("test tag"),
            };
            let tags = vec![tag_data.clone()];
            let expected = BlogEntity{
                id,
                title: title.clone(),
                body: body.clone(),
                tags: tags.clone()
            };
    
            //create
            let tag_data = Tag {
                id: 1,
                name: String::from("test tag"),
            };
            let tags = vec![tag_data.clone()];
            let repository = BlogRepositoryForMemory::new(tags.clone());
            let blog = repository.create(CreateBlog { title, body, tags: vec![tag_data.id] }).await.expect("failed create blog");
            assert_eq!(expected, blog);
    
            //find
            let blog = repository.find(blog.id).await.unwrap();
            assert_eq!(expected, blog);
    
            //all
            let blog = repository.all().await.expect("failed get all blog");
            assert_eq!(vec![expected], blog);
    
            //update
            let title = "update blog title".to_string();
            let body = "update blog body".to_string();
            let blog = repository
                .update(
                    1,
                    UpdateBlog { title: Some(title.clone()), body: Some(body.clone()), tags: Some(vec![]) }
                )
                .await
                .expect("failed update blog.");
            assert_eq!(
                BlogEntity {
                    id,
                    title,
                    body,
                    tags: vec![]
                },
                blog
            );
    
            //delete
            let res = repository.delete(id).await;
            assert!(res.is_ok())
    
        }
    }
}
