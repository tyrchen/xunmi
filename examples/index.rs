use std::{str::FromStr, thread, time::Duration};
use xunmi::*;

fn main() {
    // you can load a human-readable configuration (e.g. yaml)
    let config = IndexConfig::from_str(include_str!("../fixtures/config.yml")).unwrap();

    // then open or create the index based on the configuration
    let indexer = Indexer::open_or_create(config).unwrap();

    // data to index could comes from json / yaml / xml, as long as they're compatible with schema
    let content = include_str!("../fixtures/wiki_00.xml");

    // you could even provide mapping for renaming fields and converting data types
    // e.g. index schema has id as u64, content as text, but xml has id as string
    // and it doesn't have a content field, instead the $value of the doc is the content.
    // so we can use mapping / conversion to normalize these.
    let config = InputConfig::new(
        InputType::Xml,
        vec![("$value".into(), "content".into())],
        vec![("id".into(), (ValueType::String, ValueType::Number))],
    );

    // then you can get the updater for adding / updating index
    let mut updater = indexer.get_updater();
    // you can have multiple updaters to run it in different threads
    let mut updater1 = indexer.get_updater();
    // you could use add() or update() to add data into the search index
    // if you add, it will insert new docs; if you update, and if the doc
    // contains an "id" field, updater will first delete the term matching
    // id (so id shall be unique), then insert new docs.
    updater.update(content, &config).unwrap();
    // commit all data added/deleted.
    updater.commit().unwrap();

    // update in other thread
    thread::spawn(move || {
        let config = InputConfig::new(InputType::Yaml, vec![], vec![]);
        let text = include_str!("../fixtures/test.yml");

        updater1.update(text, &config).unwrap();
        updater1.commit().unwrap();
    });

    // by default the indexer will be auto reloaded upon every commit,
    // but that has delays in hundreds of milliseconds, so for this demo,
    // we need to wait enough time for commit to be ready
    while indexer.num_docs() == 0 {
        thread::sleep(Duration::from_millis(100));
    }

    println!("total: {}", indexer.num_docs());

    // you could provide a query and fields you want to search
    let result = indexer.search("历史", &["title", "content"], 5, 0).unwrap();
    for (score, doc) in result.iter() {
        println!("score: {}, doc: {:?}", score, doc);
    }
}
