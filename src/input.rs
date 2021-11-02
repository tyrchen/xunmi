use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{borrow::Cow, collections::HashMap, fmt};
use tantivy::{
    schema::{DocParsingError, FieldValue, Schema, Value},
    Document, IndexWriter, Result, Term,
};

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

#[derive(Clone, PartialEq)]
pub(crate) enum Input {
    Create(Vec<Document>),
    Update(Vec<Document>),
    Commit,
    Clear,
}

impl fmt::Debug for Input {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Create(docs) => write!(f, "Create: {} docs", docs.len()),
            Self::Update(docs) => write!(f, "Update: {} docs", docs.len()),
            Self::Commit => write!(f, "Commit"),
            Self::Clear => write!(f, "Clear"),
        }
    }
}

impl Input {
    pub fn new_create(docs: Vec<Document>) -> Self {
        Self::Create(docs)
    }

    pub fn new_update(docs: Vec<Document>) -> Self {
        Self::Update(docs)
    }

    pub fn new_commit() -> Self {
        Self::Commit
    }
    pub fn new_clear() -> Self {
        Self::Clear
    }

    pub fn process(self, writer: &mut IndexWriter, schema: &Schema) -> tantivy::Result<()> {
        match self {
            Input::Create(docs) => {
                for doc in docs {
                    writer.add_document(doc);
                }
            }
            Input::Update(docs) => {
                for doc in docs {
                    if let Some(id_field) = schema.get_field("id") {
                        let named_doc = schema.to_named_doc(&doc);
                        if let Some(v) = named_doc.0.get("id") {
                            if let [Value::U64(id)] = v.as_slice() {
                                let term = Term::from_field_u64(id_field, *id);
                                writer.delete_term(term);
                            }
                        }
                    }
                    writer.add_document(doc);
                }
            }
            Input::Commit => {
                writer.commit()?;
            }
            Input::Clear => {
                writer.delete_all_documents()?;
            }
        }

        Ok(())
    }
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
