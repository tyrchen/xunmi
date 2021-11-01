use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{borrow::Cow, collections::HashMap};
use tantivy::{
    schema::{DocParsingError, FieldValue, Schema, Value},
    Document, IndexWriter, Result, Term,
};

pub struct IndexUpdater {
    schema: Schema,
    writer: IndexWriter,
    #[allow(dead_code)]
    t2s: bool,
}

pub type JsonObject = serde_json::Map<String, JsonValue>;

pub struct JsonObjects(Vec<JsonObject>);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum InputType {
    Json,
    Yaml,
    Xml,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValueType {
    String,
    Number,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InputConfig {
    input_type: InputType,
    mapping: HashMap<String, String>,
    conversion: HashMap<String, (ValueType, ValueType)>,
}

impl InputConfig {
    pub fn new(
        input_type: InputType,
        mapping: Vec<(String, String)>,
        conversion: Vec<(String, (ValueType, ValueType))>,
    ) -> Self {
        let mapping: HashMap<_, _> = mapping.into_iter().collect();
        let conversion: HashMap<_, _> = conversion.into_iter().collect();
        Self {
            input_type,
            mapping,
            conversion,
        }
    }
}

impl JsonObjects {
    pub fn new(input: &str, config: &InputConfig, t2s: bool) -> Result<Self> {
        let input = match t2s {
            true => Cow::Owned(fast2s::convert(input)),
            false => Cow::Borrowed(input),
        };
        let err_fn =
            || DocParsingError::NotJson(format!("Failed to parse: {:?}...", &input[0..20]));
        let result: std::result::Result<Vec<JsonObject>, _> = match config.input_type {
            InputType::Json => serde_json::from_str(&input).map_err(|_| err_fn()),
            InputType::Yaml => serde_yaml::from_str(&input).map_err(|_| err_fn()),
            InputType::Xml => serde_xml_rs::from_str(&input).map_err(|_| err_fn()),
        };

        let mut data = match result {
            Ok(v) => v,
            Err(_) => {
                let obj: JsonObject = match config.input_type {
                    InputType::Json => serde_json::from_str(&input).map_err(|_| err_fn())?,
                    InputType::Yaml => serde_yaml::from_str(&input).map_err(|_| err_fn())?,
                    InputType::Xml => serde_xml_rs::from_str(&input).map_err(|_| err_fn())?,
                };
                vec![obj]
            }
        };

        let convert = |obj: &mut JsonObject| {
            for (k, k1) in &config.mapping {
                match obj.remove_entry(k) {
                    Some((_, v)) => obj.insert(k1.into(), v),
                    None => None,
                };
            }
            for (k, (t1, t2)) in &config.conversion {
                if let Some((k, v)) = obj.remove_entry(k) {
                    match (v, t1, t2) {
                        (JsonValue::Number(n), ValueType::Number, ValueType::String) => {
                            obj.insert(k, JsonValue::String(n.to_string()));
                        }
                        (JsonValue::String(s), ValueType::String, ValueType::Number) => {
                            obj.insert(k, JsonValue::Number(s.parse().unwrap()));
                        }
                        _ => {}
                    }
                }
            }
        };

        for item in data.iter_mut() {
            convert(item);
        }

        Ok(Self(data))
    }

    pub fn to_docs(&self, schema: &Schema) -> Result<Vec<Document>> {
        let obj2doc = |obj: &JsonObject| -> Result<Document> {
            let mut doc = Document::default();
            for (field_name, json_value) in obj.iter() {
                let field = schema
                    .get_field(field_name)
                    .ok_or_else(|| DocParsingError::NoSuchFieldInSchema(field_name.clone()))?;
                let field_entry = schema.get_field_entry(field);
                let field_type = field_entry.field_type();
                match *json_value {
                    JsonValue::Array(ref json_items) => {
                        for json_item in json_items {
                            let value = field_type
                                .value_from_json(json_item)
                                .map_err(|e| DocParsingError::ValueError(field_name.clone(), e))?;
                            doc.add(FieldValue::new(field, value));
                        }
                    }
                    _ => {
                        let value = field_type
                            .value_from_json(json_value)
                            .map_err(|e| DocParsingError::ValueError(field_name.clone(), e))?;
                        doc.add(FieldValue::new(field, value));
                    }
                }
            }
            Ok(doc)
        };
        let docs: Result<Vec<_>> = self.0.par_iter().map(obj2doc).collect();
        docs
    }
}

impl IndexUpdater {
    pub fn new(writer: IndexWriter, schema: Schema, t2s: bool) -> Self {
        Self {
            writer,
            schema,
            t2s,
        }
    }

    pub fn clear(&self) -> Result<u64> {
        self.writer.delete_all_documents()
    }

    pub fn add(&mut self, input: &str, config: &InputConfig) -> Result<()> {
        let objs = JsonObjects::new(input, config, self.t2s)?;
        let docs = objs.to_docs(&self.schema)?;
        for doc in docs {
            self.writer.add_document(doc);
        }

        self.writer.commit()?;
        Ok(())
    }

    pub fn update(&mut self, input: &str, config: &InputConfig) -> Result<()> {
        let objs = JsonObjects::new(input, config, self.t2s)?;
        let docs = objs.to_docs(&self.schema)?;
        for doc in docs {
            if let Some(id_field) = self.schema.get_field("id") {
                let named_doc = self.schema.to_named_doc(&doc);
                if let Some(v) = named_doc.0.get("id") {
                    if let [Value::U64(id)] = v.as_slice() {
                        let term = Term::from_field_u64(id_field, *id);
                        self.writer.delete_term(term);
                    }
                }
            }
            self.writer.add_document(doc);
        }

        self.writer.commit()?;
        Ok(())
    }
}
