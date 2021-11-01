use std::{path::PathBuf, str::FromStr};

use serde::{Deserialize, Serialize};
use tantivy::schema::Schema;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexConfig {
    pub path: Option<PathBuf>,
    pub schema: Schema,
    pub text_lang: TextLanguage,
    pub writer_memory: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TextLanguage {
    Western,
    Chinese(bool),
}

impl FromStr for IndexConfig {
    type Err = serde_yaml::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_yaml::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_should_work() {
        let dir = tempfile::tempdir().unwrap();
        let lang = TextLanguage::Chinese(true);
        let schema: Schema = serde_yaml::from_str(include_str!("../fixtures/schema.yml")).unwrap();
        let config = IndexConfig {
            path: Some(dir.into_path()),
            schema,
            text_lang: lang,
            writer_memory: 100_000_000,
        };

        let config1: IndexConfig =
            serde_yaml::from_str(include_str!("../fixtures/config.yml")).unwrap();

        assert_eq!(config1.schema, config.schema);
        assert_eq!(config1.text_lang, config.text_lang);
    }
}
