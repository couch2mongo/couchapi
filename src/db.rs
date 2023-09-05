use async_trait::async_trait;
use bson::{doc, Document};
use futures_util::StreamExt;
use mongodb::error::Error;
use mongodb::options::{DeleteOptions, ReplaceOptions};
use mongodb::results::UpdateResult;

#[cfg(test)]
use mockall::*;
use tracing::debug;

#[async_trait]
#[cfg_attr(test, automock)]
pub trait Database {
    async fn get_version(&self) -> Result<Document, Error>;
    async fn find_one(&self, coll: String, id: String) -> Result<Option<Document>, Error>;
    async fn replace_one(
        &self,
        coll: String,
        filter: Document,
        replacement: Document,
        options: ReplaceOptions,
    ) -> Result<UpdateResult, Error>;
    async fn delete_one(
        &self,
        coll: String,
        filter: Document,
        options: DeleteOptions,
    ) -> Result<u64, Error>;
    async fn aggregate(
        &self,
        coll: String,
        pipeline: Vec<Document>,
    ) -> Result<Vec<Document>, Error>;
    async fn count(&self, coll: String) -> Result<u64, Error>;
}

#[derive(Debug)]
pub struct MongoDB {
    pub db: mongodb::Database,
}

#[async_trait]
impl Database for MongoDB {
    #[tracing::instrument]
    async fn get_version(&self) -> Result<Document, Error> {
        self.db.run_command(doc! { "buildInfo": 1 }, None).await
    }

    #[tracing::instrument]
    async fn find_one(&self, coll: String, id: String) -> Result<Option<Document>, Error> {
        let c = self.db.collection::<Document>(&coll);
        c.find_one(doc! { "_id": id }, None).await
    }

    #[tracing::instrument]
    async fn replace_one(
        &self,
        coll: String,
        filter: Document,
        replacement: Document,
        options: ReplaceOptions,
    ) -> Result<UpdateResult, Error> {
        let c = self.db.collection::<Document>(&coll);
        c.replace_one(filter, replacement, options).await
    }

    #[tracing::instrument]
    async fn delete_one(
        &self,
        coll: String,
        filter: Document,
        options: DeleteOptions,
    ) -> Result<u64, Error> {
        let c = self.db.collection::<Document>(&coll);
        c.delete_one(filter, options).await.map(|r| r.deleted_count)
    }

    #[tracing::instrument]
    async fn aggregate(
        &self,
        coll: String,
        pipeline: Vec<Document>,
    ) -> Result<Vec<Document>, Error> {
        debug!(
            "aggregate: coll: {}, pipeline: {:?}",
            coll,
            serde_json::to_string(&pipeline).unwrap()
        );

        let c = self.db.collection::<Document>(&coll);
        let mut cursor = c.aggregate(pipeline, None).await?;
        let mut results = Vec::new();

        while let Some(doc) = cursor.next().await {
            results.push(doc?);
        }
        Ok(results)
    }

    #[tracing::instrument]
    async fn count(&self, coll: String) -> Result<u64, Error> {
        let c = self.db.collection::<Document>(&coll);
        c.estimated_document_count(None).await
    }
}
