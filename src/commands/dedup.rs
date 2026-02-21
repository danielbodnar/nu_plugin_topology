use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, ListStream, PipelineData, Record, Signature, Signals,
    SyntaxShape, Type, Value,
};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};

use crate::algo::{lsh, simhash, tokenizer, url_normalize};
use crate::TopologyPlugin;

pub struct Dedup;

impl PluginCommand for Dedup {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology dedup"
    }

    fn description(&self) -> &str {
        "Find duplicate and near-duplicate items using LSH + SimHash + URL normalization"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::table(), Type::table())
            .named(
                "field",
                SyntaxShape::String,
                "Field containing text for content dedup (default: content)",
                Some('f'),
            )
            .named(
                "url-field",
                SyntaxShape::String,
                "Field containing URL for URL-based dedup (default: url)",
                None,
            )
            .named(
                "strategy",
                SyntaxShape::String,
                "Strategy: url, fuzzy, combined (default: combined)",
                Some('s'),
            )
            .named(
                "threshold",
                SyntaxShape::Int,
                "SimHash hamming distance threshold (default: 3)",
                None,
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec![
            "dedup", "duplicate", "deduplicate", "near-duplicate", "lsh", "simhash",
        ]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[content url]; ["rust programming" "https://example.com"] ["rust programming language" "https://www.example.com"]] | topology dedup"#,
            description: "Find duplicates using combined URL + content strategy",
            result: None,
        }]
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
        let url_field: String = call
            .get_flag::<String>("url-field")?
            .unwrap_or_else(|| "url".into());
        let strategy_str: String = call
            .get_flag::<String>("strategy")?
            .unwrap_or_else(|| "combined".into());
        let threshold: u32 = call.get_flag::<i64>("threshold")?.unwrap_or(3) as u32;
        let head = call.head;

        let rows: Vec<Value> = input.into_iter().collect();
        if rows.is_empty() {
            return Ok(PipelineData::Value(Value::list(vec![], head), None));
        }

        let n = rows.len();

        // Phase 1: URL-based exact dedup
        let mut url_groups: HashMap<String, Vec<usize>> = HashMap::new();
        if strategy_str == "url" || strategy_str == "combined" {
            for (i, row) in rows.iter().enumerate() {
                if let Some(url_val) = row.get_data_by_key(&url_field) {
                    if let Ok(url_str) = url_val.coerce_string() {
                        if let Some(key) = url_normalize::canonical_key(&url_str) {
                            url_groups.entry(key).or_default().push(i);
                        }
                    }
                }
            }
        }

        // Phase 2: Content-based fuzzy dedup with SimHash + LSH
        let mut content_pairs: HashSet<(usize, usize)> = HashSet::new();
        if strategy_str == "fuzzy" || strategy_str == "combined" {
            let texts: Vec<String> = rows
                .iter()
                .map(|row| {
                    row.get_data_by_key(&field)
                        .and_then(|v| v.coerce_string().ok())
                        .unwrap_or_default()
                })
                .collect();

            let token_lists: Vec<Vec<String>> =
                texts.par_iter().map(|t| tokenizer::tokenize(t)).collect();

            let fingerprints: Vec<u64> = token_lists
                .par_iter()
                .map(|tokens| simhash::simhash_uniform(tokens))
                .collect();

            // Build LSH index
            let mut lsh_index = lsh::SimHashLshIndex::default_64();
            for (i, &fp) in fingerprints.iter().enumerate() {
                lsh_index.insert(i, fp);
            }

            // Get candidate pairs and verify
            for (i, j) in lsh_index.candidate_pairs() {
                if simhash::hamming_distance(fingerprints[i], fingerprints[j]) <= threshold {
                    content_pairs.insert((i, j));
                }
            }
        }

        // Merge groups using union-find
        let mut parent: Vec<usize> = (0..n).collect();

        let find = |parent: &mut Vec<usize>, mut x: usize| -> usize {
            while parent[x] != x {
                parent[x] = parent[parent[x]]; // path compression
                x = parent[x];
            }
            x
        };

        let union = |parent: &mut Vec<usize>, a: usize, b: usize| {
            let ra = {
                let mut x = a;
                while parent[x] != x {
                    parent[x] = parent[parent[x]];
                    x = parent[x];
                }
                x
            };
            let rb = {
                let mut x = b;
                while parent[x] != x {
                    parent[x] = parent[parent[x]];
                    x = parent[x];
                }
                x
            };
            if ra != rb {
                parent[rb] = ra;
            }
        };

        // Union from URL groups
        for members in url_groups.values() {
            for i in 1..members.len() {
                union(&mut parent, members[0], members[i]);
            }
        }

        // Union from content pairs
        for &(i, j) in &content_pairs {
            union(&mut parent, i, j);
        }

        // Build final groups
        let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
        for i in 0..n {
            let root = find(&mut parent, i);
            groups.entry(root).or_default().push(i);
        }

        // Assign group IDs and primary flag
        let mut group_ids = vec![0usize; n];
        let mut is_primary = vec![true; n];
        let mut group_counter = 0;

        for members in groups.values() {
            let gid = group_counter;
            group_counter += 1;

            // First item in group is primary
            for (idx, &member) in members.iter().enumerate() {
                group_ids[member] = gid;
                is_primary[member] = idx == 0;
            }
        }

        // Append columns
        let results: Vec<Value> = rows
            .into_iter()
            .enumerate()
            .map(|(i, row)| {
                let mut record = match row {
                    Value::Record { val, .. } => val.into_owned(),
                    other => {
                        let mut r = Record::new();
                        r.push("value", other);
                        r
                    }
                };
                record.push("_dup_group", Value::int(group_ids[i] as i64, head));
                record.push("_is_primary", Value::bool(is_primary[i], head));
                Value::record(record, head)
            })
            .collect();

        Ok(ListStream::new(results.into_iter(), head, Signals::empty()).into())
    }
}
