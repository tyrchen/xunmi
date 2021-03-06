mod config;
mod indexer;
mod input;
mod updater;

pub use config::{IndexConfig, TextLanguage};
pub use indexer::Indexer;
pub(crate) use input::Input;
pub use input::{InputConfig, InputType, ValueType};
pub use updater::IndexUpdater;

// re-exports
pub use tantivy::schema::Schema;

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use std::{str::FromStr, thread, time::Duration};

    #[test]
    fn index_data_and_search() {
        let mut config = IndexConfig::from_str(include_str!("../fixtures/config.yml")).unwrap();
        config.path = None;
        let indexer = Indexer::open_or_create(config).unwrap();
        let mut updater = indexer.get_updater();

        let data = include_str!("../fixtures/test.yml");
        let config = InputConfig::new(InputType::Yaml, vec![], vec![]);
        updater.add(data, &config).unwrap();
        let config = InputConfig::new(
            InputType::Json,
            vec![],
            vec![("id".into(), (ValueType::String, ValueType::Number))],
        );
        let json_data = serde_json::to_string(&json!({
            "id": "1024",
            "title": "你好，唐宋元明清",
            "url": "http://example.com",
            "content": "hell world!"
        }))
        .unwrap();
        updater.add(&json_data, &config).unwrap();
        updater.commit().unwrap();

        // need to wait enough time for commit to be ready
        while indexer.num_docs() == 0 {
            thread::sleep(Duration::from_millis(100));
        }

        let result = indexer.search("宋元", &["title", "content"], 5, 0).unwrap();

        assert_eq!(result.len(), 2);
        let ids: Vec<_> = result
            .iter()
            .map(|(_, doc)| serde_json::to_string(doc.0.get("id").unwrap()).unwrap())
            .collect();

        assert_eq!(ids, ["[1024]", "[13]"]);
    }
}
