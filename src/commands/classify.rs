use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, ListStream, PipelineData, Signature, Signals, SyntaxShape,
    Type, Value,
};

use crate::algo::{clustering, discover, taxonomy};
#[cfg(feature = "cache")]
use crate::algo::{cache, storage};
use crate::TopologyPlugin;

use super::util;

pub struct Classify;

impl PluginCommand for Classify {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology classify"
    }

    fn description(&self) -> &str {
        "Classify items into categories discovered automatically from content, or from a user-provided taxonomy file"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_types(vec![
                (Type::table(), Type::table()),
                (Type::list(Type::Any), Type::list(Type::Any)),
                (Type::record(), Type::list(Type::Any)),
                (Type::String, Type::list(Type::Any)),
                (Type::Any, Type::Any),
            ])
            .named(
                "field",
                SyntaxShape::String,
                "Field containing text to classify (default: content)",
                Some('f'),
            )
            .named(
                "taxonomy",
                SyntaxShape::String,
                "Path to taxonomy JSON file. If omitted, taxonomy is discovered from the data",
                Some('t'),
            )
            .named(
                "clusters",
                SyntaxShape::Int,
                "Number of categories to discover (default: 15)",
                Some('k'),
            )
            .named(
                "sample",
                SyntaxShape::Int,
                "Max items to sample for discovery (HAC is O(n^2), default: 500)",
                None,
            )
            .named(
                "threshold",
                SyntaxShape::Float,
                "Minimum BM25 score to assign a category (default: 0.5)",
                None,
            )
            .named(
                "linkage",
                SyntaxShape::String,
                "HAC linkage: ward, complete, average, single (default: ward)",
                None,
            )
            .named(
                "seed",
                SyntaxShape::Int,
                "Random seed for sampling (default: 42)",
                None,
            )
            .named(
                "cache",
                SyntaxShape::String,
                "Path to SQLite cache database for persistent artifact caching",
                None,
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["classify", "categorize", "label", "discover", "cluster", "taxonomy"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: r#"[[content]; ["rust memory safety"] ["python data science"]] | topology classify --clusters 2"#,
                description: "Auto-discover 2 categories from a table",
                result: None,
            },
            Example {
                example: r#"["rust programming" "web development" "data science"] | topology classify --clusters 2"#,
                description: "Classify a list of strings",
                result: None,
            },
        ]
    }

    fn run(
        &self,
        _plugin: &TopologyPlugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let field: String = call
            .get_flag::<String>("field")?
            .unwrap_or_else(|| "content".into());
        let taxonomy_path: Option<String> = call.get_flag("taxonomy")?;
        let k: usize = call.get_flag::<i64>("clusters")?.unwrap_or(15) as usize;
        let sample_size: usize = call.get_flag::<i64>("sample")?.unwrap_or(500) as usize;
        let threshold: f64 = call.get_flag::<f64>("threshold")?.unwrap_or(0.5);
        let linkage_str: String = call
            .get_flag::<String>("linkage")?
            .unwrap_or_else(|| "ward".into());
        let seed: u64 = call.get_flag::<i64>("seed")?.unwrap_or(42) as u64;
        let cache_path: Option<String> = call.get_flag("cache")?;
        let head = call.head;

        let linkage = clustering::Linkage::from_str(&linkage_str).ok_or_else(|| {
            LabeledError::new(format!(
                "Unknown linkage '{linkage_str}'. Use: ward, complete, average, single"
            ))
        })?;

        let rows = util::normalize_input(input, head);
        if rows.is_empty() {
            return Ok(PipelineData::Value(Value::list(vec![], head), None));
        }

        let texts: Vec<String> = rows
            .iter()
            .map(|row| {
                row.get_data_by_key(&field)
                    .and_then(|v| v.coerce_string().ok())
                    .unwrap_or_default()
            })
            .collect();

        let tax = match taxonomy_path {
            Some(path) => taxonomy::load_taxonomy(&path)
                .map_err(|e| LabeledError::new(e))?,
            None => {
                let config = discover::DiscoverConfig {
                    k,
                    sample_size,
                    label_terms: 3,
                    keywords_per_cluster: 20,
                    linkage,
                    seed,
                };
                resolve_taxonomy_cached(&texts, &config, cache_path.as_deref())
            }
        };

        let classifications = discover::classify_against_taxonomy(&texts, &tax, threshold);

        let results: Vec<Value> = rows
            .into_iter()
            .zip(classifications)
            .map(|(row, (category, hierarchy, confidence))| {
                util::append_columns(
                    row,
                    &[
                        ("_category", Value::string(&category, head)),
                        ("_hierarchy", Value::string(&hierarchy, head)),
                        ("_confidence", Value::float(confidence, head)),
                    ],
                    head,
                )
            })
            .collect();

        Ok(ListStream::new(results.into_iter(), head, Signals::empty()).into())
    }
}

fn resolve_taxonomy_cached(
    texts: &[String],
    config: &discover::DiscoverConfig,
    #[allow(unused)] cache_path: Option<&str>,
) -> taxonomy::Taxonomy {
    #[cfg(feature = "cache")]
    if let Some(cp) = cache_path {
        if let Ok(db) = storage::CacheDb::open_or_create(cp) {
            let c_hash = cache::content_hash(texts);
            let args = serde_json::json!({
                "k": config.k,
                "sample_size": config.sample_size,
                "seed": config.seed,
            });
            let a_hash = cache::args_hash(&args);

            if let Ok(Some((meta, payload))) =
                db.get(cache::ArtifactKind::Taxonomy, c_hash, a_hash)
            {
                if cache::is_valid(&meta, c_hash, a_hash) {
                    if let Ok(tax) =
                        serde_json::from_slice::<taxonomy::Taxonomy>(&payload)
                    {
                        return tax;
                    }
                }
            }

            let tax = discover::discover_taxonomy(texts, config);
            if let Ok(payload) = serde_json::to_vec(&tax) {
                let meta = cache::CacheMeta::new(c_hash, texts.len(), a_hash);
                let _ = db.put(cache::ArtifactKind::Taxonomy, &meta, &payload);
            }
            return tax;
        }
    }

    discover::discover_taxonomy(texts, config)
}
