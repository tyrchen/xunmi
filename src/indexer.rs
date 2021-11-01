use std::{fs, ops::Deref, sync::Arc};

use cang_jie::{CangJieTokenizer, TokenizerOption, CANG_JIE};
use jieba_rs::Jieba;
use tantivy::{
    collector::TopDocs, directory::MmapDirectory, query::QueryParser, schema::NamedFieldDocument,
    Index, IndexReader, ReloadPolicy, Result,
};
use tracing::info;

use crate::{IndexConfig, IndexUpdater, TextLanguage};

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
}

impl Indexer {
    fn new(index: Index, config: IndexConfig) -> Result<Self> {
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;
        let inner = IndexInner {
            index,
            reader,
            config,
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub fn open_or_create(config: IndexConfig) -> Result<Self> {
        let schema = config.schema.clone();
        let index = if let Some(dir) = &config.path {
            fs::create_dir_all(dir)?;
            let dir = MmapDirectory::open(dir)?;
            Index::open_or_create(dir, schema)?
        } else {
            Index::create_in_ram(schema)
        };

        Self::set_tokenizer(&index, &config);
        Self::new(index, config)
    }

    pub fn get_updater(&self) -> Result<IndexUpdater> {
        let writer = self.index.writer(self.config.writer_memory)?;
        let t2s = match self.config.text_lang {
            TextLanguage::Chinese(t2s) => t2s,
            _ => false,
        };
        Ok(IndexUpdater::new(writer, self.index.schema(), t2s))
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
