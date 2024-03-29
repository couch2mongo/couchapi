// Copyright (c) 2024, Green Man Gaming Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    async fn find_one(&self, coll: &str, id: &str) -> Result<Option<Document>, Error>;
    async fn replace_one(
        &self,
        coll: &str,
        filter: Document,
        replacement: Document,
        options: ReplaceOptions,
    ) -> Result<UpdateResult, Error>;
    async fn delete_one(
        &self,
        coll: &str,
        filter: Document,
        options: DeleteOptions,
    ) -> Result<u64, Error>;
    async fn aggregate(&self, coll: &str, pipeline: Vec<Document>) -> Result<Vec<Document>, Error>;
    async fn count(&self, coll: &str) -> Result<u64, Error>;
}

#[derive(Debug)]
pub struct MongoDB {
    pub db: mongodb::Database,
}

#[async_trait]
impl Database for MongoDB {
    #[tracing::instrument(skip(self))]
    async fn get_version(&self) -> Result<Document, Error> {
        self.db.run_command(doc! { "buildInfo": 1 }, None).await
    }

    #[tracing::instrument(skip(self))]
    async fn find_one(&self, coll: &str, id: &str) -> Result<Option<Document>, Error> {
        let c = self.db.collection::<Document>(coll);
        c.find_one(doc! { "_id": id }, None).await
    }

    #[tracing::instrument(skip(self))]
    async fn replace_one(
        &self,
        coll: &str,
        filter: Document,
        replacement: Document,
        options: ReplaceOptions,
    ) -> Result<UpdateResult, Error> {
        let c = self.db.collection::<Document>(coll);
        c.replace_one(filter, replacement, options).await
    }

    #[tracing::instrument(skip(self))]
    async fn delete_one(
        &self,
        coll: &str,
        filter: Document,
        options: DeleteOptions,
    ) -> Result<u64, Error> {
        let c = self.db.collection::<Document>(coll);
        c.delete_one(filter, options).await.map(|r| r.deleted_count)
    }

    #[tracing::instrument(skip(self))]
    async fn aggregate(&self, coll: &str, pipeline: Vec<Document>) -> Result<Vec<Document>, Error> {
        debug!(
            "aggregate: coll: {}, pipeline: {:?}",
            coll,
            serde_json::to_string(&pipeline).unwrap()
        );

        let c = self.db.collection::<Document>(coll);
        let options = mongodb::options::AggregateOptions::builder()
            .allow_disk_use(Some(true))
            .build();
        let mut cursor = c.aggregate(pipeline, options).await?;
        let mut results = Vec::new();

        while let Some(doc) = cursor.next().await {
            results.push(doc?);
        }
        Ok(results)
    }

    #[tracing::instrument(skip(self))]
    async fn count(&self, coll: &str) -> Result<u64, Error> {
        let c = self.db.collection::<Document>(coll);
        c.estimated_document_count(None).await
    }
}
