use async_trait::async_trait;
use bson::{doc, Document};
use mongodb::error::Error;
use mongodb::options::{DeleteOptions, ReplaceOptions};
use mongodb::results::UpdateResult;

#[cfg(test)]
use mockall::*;

#[async_trait]
#[cfg_attr(test, automock)]
pub trait Database {
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
}

pub struct MongoDB {
    pub db: mongodb::Database,
}

#[async_trait]
impl Database for MongoDB {
    async fn find_one(&self, coll: String, id: String) -> Result<Option<Document>, Error> {
        let c = self.db.collection::<Document>(&coll);
        c.find_one(doc! { "_id": id }, None).await
    }

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

    async fn delete_one(
        &self,
        coll: String,
        filter: Document,
        options: DeleteOptions,
    ) -> Result<u64, Error> {
        let c = self.db.collection::<Document>(&coll);
        c.delete_one(filter, options).await.map(|r| r.deleted_count)
    }
}
