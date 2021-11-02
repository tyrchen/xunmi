use cang_jie::{CangJieTokenizer, TokenizerOption, CANG_JIE};
use crossbeam_channel::{unbounded, Sender};
use jieba_rs::Jieba;
use std::{fs, ops::Deref, sync::Arc, thread};
use tantivy::{
    collector::TopDocs, directory::MmapDirectory, query::QueryParser, schema::NamedFieldDocument,
    Index, IndexReader, ReloadPolicy, Result,
};
use tracing::{info, warn};

use crate::{IndexConfig, IndexUpdater, Input, TextLanguage};

#[derive(Clone)]
pub struct Indexer {
    inner: Arc<IndexInner>,
}

impl Deref for Indexer {
    type Target = IndexInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct IndexInner {
    index: Index,
    reader: IndexReader,
    config: IndexConfig,
    updater: Sender<Input>,
}

impl Indexer {
    pub fn open_or_create(config: IndexConfig) -> Result<Self> {
        let schema = config.schema.clone();
        let index = if let Some(dir) = &config.path {
            fs::create_dir_all(dir)?;
            let dir = MmapDirectory::open(dir)?;
            Index::open_or_create(dir, schema.clone())?
        } else {
            Index::create_in_ram(schema.clone())
        };

        Self::set_tokenizer(&index, &config);

        let mut writer = index.writer(config.writer_memory)?;
        let (s, r) = unbounded::<Input>();

        // spawn a thread to process the writer request
        thread::spawn(move || {
            for input in r {
                if let Err(e) = input.process(&mut writer, &schema) {
                    warn!("Failed to process input. Error: {:?}", e);
                }
            }
        });

        Self::new(index, config, s)
    }

    pub fn get_updater(&self) -> IndexUpdater {
        let t2s = TextLanguage::Chinese(true) == self.config.text_lang;
        IndexUpdater::new(self.updater.clone(), self.index.schema(), t2s)
    }

    pub fn reload(&self) -> Result<()> {
        self.reader.reload()
    }

    pub fn search(
        &self,
        query: &str,
        fields: &[&str],
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(f32, NamedFieldDocument)>> {
        let schema = &self.config.schema;
        let query_fields: Vec<_> = fields.iter().filter_map(|s| schema.get_field(s)).collect();

        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, query_fields);
        let query = query_parser.parse_query(query)?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit).and_offset(offset))?;
        let mut result = Vec::with_capacity(limit);
        for (score, addr) in top_docs {
            let doc = searcher.doc(addr)?;
            let named_doc = schema.to_named_doc(&doc);
            result.push((score, named_doc));
        }

        Ok(result)
    }

    pub fn num_docs(&self) -> u64 {
        let searcher = self.reader.searcher();
        searcher.num_docs()
    }

    // private functions
    fn new(index: Index, config: IndexConfig, updater: Sender<Input>) -> Result<Self> {
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;
        let inner = IndexInner {
            index,
            reader,
            config,
            updater,
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    fn set_tokenizer(index: &Index, config: &IndexConfig) {
        let tokenizer = CangJieTokenizer {
            worker: Arc::new(Jieba::empty()), // empty dictionary
            option: TokenizerOption::Unicode,
        };
        if let TextLanguage::Chinese(_) = config.text_lang {
            info!("Set tokenizer to CANG_JIE");
            index.tokenizers().register(CANG_JIE, tokenizer);
        }
    }
}
