use crossbeam_channel::Sender;
use tantivy::{schema::Schema, Result, TantivyError};

use crate::{input::JsonObjects, Input, InputConfig};

pub struct IndexUpdater {
    sender: Sender<Input>,
    t2s: bool,
    schema: Schema,
}

impl IndexUpdater {
    pub(crate) fn new(sender: Sender<Input>, schema: Schema, t2s: bool) -> Self {
        Self {
            sender,
            schema,
            t2s,
        }
    }

    pub fn add(&mut self, text: &str, config: &InputConfig) -> Result<()> {
        let objs = JsonObjects::new(text, config, self.t2s)?;
        let docs = objs.to_docs(&self.schema)?;
        let msg = Input::new_create(docs);
        self.sender
            .send(msg)
            .map_err(|e| TantivyError::SystemError(e.to_string()))
    }

    pub fn update(&mut self, text: &str, config: &InputConfig) -> Result<()> {
        let objs = JsonObjects::new(text, config, self.t2s)?;
        let docs = objs.to_docs(&self.schema)?;
        let msg = Input::new_update(docs);
        self.sender
            .send(msg)
            .map_err(|e| TantivyError::SystemError(e.to_string()))
    }

    pub fn commit(&self) -> Result<()> {
        let msg = Input::new_commit();
        self.sender
            .send(msg)
            .map_err(|e| TantivyError::SystemError(e.to_string()))
    }

    pub fn clear(&self) -> Result<()> {
        let msg = Input::new_clear();
        self.sender
            .send(msg)
            .map_err(|e| TantivyError::SystemError(e.to_string()))
    }
}
