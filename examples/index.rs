use searcher::*;
use std::{path::PathBuf, str::FromStr};

fn main() {
    let mut config = IndexConfig::from_str(include_str!("../fixtures/config.yml")).unwrap();
    config.path = Some(PathBuf::from_str("/tmp/searcher_index").unwrap());
    let indexer = Indexer::open_or_create(config).unwrap();
    let mut updater = indexer.get_updater().unwrap();

    let content = include_str!("../fixtures/wiki_00");
    let config = InputConfig::new(
        InputType::Xml,
        vec![("$value".into(), "content".into())],
        vec![("id".into(), (ValueType::String, ValueType::Number))],
    );
    updater.update(content, &config).unwrap();

    indexer.reload().unwrap();

    println!("total: {}", indexer.num_docs());
    let result = indexer.search("历史", &["title", "content"], 5, 0).unwrap();
    for (score, doc) in result.iter() {
        println!("score: {}, doc: {:?}", score, doc);
    }
}
