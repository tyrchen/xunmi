use std::{path::Path, sync::Arc};

use cang_jie::{CangJieTokenizer, TokenizerOption, CANG_JIE};
use jieba_rs::Jieba;
use serde::{Deserialize, Serialize};
use tantivy::{
    collector::TopDocs,
    query::{QueryParser, TermQuery},
    schema::{
        Field, FieldType, IndexRecordOption, NamedFieldDocument, Schema, TextFieldIndexing,
        TextOptions, FAST, INDEXED, STORED,
    },
    Document, Index, IndexReader, ReloadPolicy, Term, TERMINATED,
};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Post {
    id: u64,
    url: String,
    title: String,
    #[serde(rename(deserialize = "$value"))]
    content: String,
}

fn main() -> tantivy::Result<()> {
    let index = create_or_load_index("/tmp/test_index");
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;

    let content = include_str!("../fixtures/wiki_00");
    add_or_update_index(&index, content);

    reader.reload().unwrap();

    for (score, item) in search(&index, &reader, "图灵")? {
        println!(
            "score: {}, id: {:?}, title: {:?}, url: {:?}",
            score,
            item.0.get("id"),
            item.0.get("title"),
            item.0.get("url")
        );
    }

    let schema = index.schema();
    println!("{}", serde_yaml::to_string(&schema).unwrap());

    Ok(())
}

#[allow(dead_code)]
fn add_or_update_index(index: &Index, content: &str) {
    let posts: Vec<Post> = serde_xml_rs::from_str(content).unwrap();

    let schema = index.schema();
    let mut index_writer = index.writer(100_000_000).unwrap();

    let id_field = schema.get_field("id").unwrap();
    for post in posts.iter() {
        let term = Term::from_field_u64(id_field, post.id);
        index_writer.delete_term(term.clone());
        let s = serde_json::to_string(post).unwrap();
        let doc = schema.parse_document(&s).unwrap();
        index_writer.add_document(doc);
    }

    index_writer.commit().unwrap();
}

#[allow(dead_code)]
fn get_doc_by_id(
    reader: &IndexReader,
    id: u64,
    id_field: Field,
) -> tantivy::Result<Option<Document>> {
    let searcher = reader.searcher();
    let term = Term::from_field_u64(id_field, id);

    let term_query = TermQuery::new(term, IndexRecordOption::Basic);
    let top_docs = searcher.search(&term_query, &TopDocs::with_limit(1))?;

    if let Some((_score, doc_address)) = top_docs.first() {
        let doc = searcher.doc(*doc_address)?;
        Ok(Some(doc))
    } else {
        // no doc matching this ID.
        Ok(None)
    }
}

fn search(
    index: &Index,
    reader: &IndexReader,
    query: &str,
) -> tantivy::Result<Vec<(f32, NamedFieldDocument)>> {
    let schema = index.schema();
    let title = schema.get_field("title").unwrap();
    let content = schema.get_field("content").unwrap();
    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(index, vec![title, content]);
    let query = query_parser.parse_query(query)?;
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
    let mut result = Vec::with_capacity(10);
    for (score, addr) in top_docs {
        let doc = searcher.doc(addr)?;
        let named_doc = schema.to_named_doc(&doc);
        result.push((score, named_doc));
    }

    Ok(result)
}

#[allow(dead_code)]
fn search_old(index: &Index, query: &str) -> Vec<NamedFieldDocument> {
    let schema = index.schema();
    let default_fields: Vec<_> = schema
        .fields()
        .filter(|&(_, field_entry)| match field_entry.field_type() {
            FieldType::Str(options) => options.get_indexing_options().is_some(),
            _ => false,
        })
        .map(|(field, _)| field)
        .collect();

    let query_parser = QueryParser::new(schema.clone(), default_fields, index.tokenizers().clone());
    let query = query_parser.parse_query(query).unwrap();
    let searcher = index.reader().unwrap().searcher();
    let weight = query.weight(&searcher, true).unwrap();
    let mut result = Vec::with_capacity(64);
    for segment_reader in searcher.segment_readers() {
        let mut scorer = weight.scorer(segment_reader, 1.0).unwrap();
        let store_reader = segment_reader.get_store_reader().unwrap();
        while scorer.doc() != TERMINATED {
            let doc_id = scorer.doc();
            let doc = store_reader.get(doc_id).unwrap();

            let named_doc = schema.to_named_doc(&doc);

            result.push(named_doc);
            scorer.advance();
        }
    }

    result
}

fn create_or_load_index(dir: impl AsRef<Path>) -> Index {
    let tokenizer = CangJieTokenizer {
        worker: Arc::new(Jieba::empty()), // empty dictionary
        option: TokenizerOption::Unicode,
    };

    let index = if let Ok(index) = Index::open_in_dir(dir.as_ref()) {
        index
    } else {
        let text_indexing = TextFieldIndexing::default()
            .set_tokenizer(CANG_JIE) // Set custom tokenizer
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_options = TextOptions::default().set_indexing_options(text_indexing);
        let mut builder = Schema::builder();

        let _id = builder.add_u64_field("id", INDEXED | FAST | STORED);
        let _url = builder.add_text_field("url", STORED);
        let _title = builder.add_text_field("title", text_options.clone() | STORED);
        let _content = builder.add_text_field("content", text_options);
        let schema = builder.build();
        Index::create_in_dir(dir, schema).unwrap()
    };

    index.tokenizers().register(CANG_JIE, tokenizer);
    index
}
