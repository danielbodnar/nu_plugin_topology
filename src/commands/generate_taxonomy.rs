use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, PipelineData, Record, Signature, SyntaxShape, Type, Value,
};

use crate::algo::{clustering, tfidf, tokenizer};
use crate::TopologyPlugin;

use super::util;

pub struct GenerateTaxonomy;

impl PluginCommand for GenerateTaxonomy {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology generate"
    }

    fn description(&self) -> &str {
        "Auto-generate a taxonomy from content using hierarchical clustering"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_types(vec![
                (Type::table(), Type::record()),
                (Type::list(Type::Any), Type::record()),
                (Type::Any, Type::record()),
            ])
            .named(
                "field",
                SyntaxShape::String,
                "Field containing text (default: content)",
                Some('f'),
            )
            .named(
                "depth",
                SyntaxShape::Int,
                "Number of clusters / taxonomy depth (default: 10)",
                Some('k'),
            )
            .named(
                "linkage",
                SyntaxShape::String,
                "Linkage method: ward, complete, average, single (default: ward)",
                Some('l'),
            )
            .named(
                "top-terms",
                SyntaxShape::Int,
                "Number of top terms per cluster label (default: 5)",
                None,
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["taxonomy", "generate", "cluster", "hierarchical", "hac"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: r#"["rust systems fast" "go concurrent simple" "python data science"] | topology generate --depth 2"#,
                description: "Generate 2-cluster taxonomy from a list of strings",
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
        let k: usize = call.get_flag::<i64>("depth")?.unwrap_or(10) as usize;
        let linkage_str: String = call
            .get_flag::<String>("linkage")?
            .unwrap_or_else(|| "ward".into());
        let top_n: usize = call.get_flag::<i64>("top-terms")?.unwrap_or(5) as usize;
        let head = call.head;

        let linkage = clustering::Linkage::from_str(&linkage_str).ok_or_else(|| {
            LabeledError::new(format!(
                "Unknown linkage '{linkage_str}'. Use: ward, complete, average, single"
            ))
        })?;

        let rows = util::normalize_input(input, head);
        let n = rows.len();

        if n < 2 {
            return Err(LabeledError::new(
                "Need at least 2 items to generate a taxonomy",
            ));
        }

        let texts: Vec<String> = rows
            .iter()
            .map(|row| {
                row.get_data_by_key(&field)
                    .and_then(|v| v.coerce_string().ok())
                    .unwrap_or_default()
            })
            .collect();

        let token_lists: Vec<Vec<String>> = texts.iter().map(|t| tokenizer::tokenize(t)).collect();

        let mut corpus = tfidf::Corpus::new();
        for tokens in &token_lists {
            corpus.add_document(tokens);
        }

        let vectors: Vec<std::collections::HashMap<String, f64>> = (0..n)
            .map(|i| corpus.tfidf_vector(i))
            .collect();

        let distances = clustering::cosine_distance_matrix(&vectors);
        let k = k.min(n);
        let dendrogram = clustering::hac(&distances, n, linkage);
        let labels = clustering::cut_tree(&dendrogram, k);

        let actual_k = labels.iter().max().map(|m| m + 1).unwrap_or(0);
        let mut categories: Vec<Value> = Vec::with_capacity(actual_k);

        for cluster_idx in 0..actual_k {
            let member_indices: Vec<usize> = labels
                .iter()
                .enumerate()
                .filter(|(_, &l)| l == cluster_idx)
                .map(|(i, _)| i)
                .collect();

            if member_indices.is_empty() {
                continue;
            }

            let mut merged: std::collections::HashMap<String, f64> =
                std::collections::HashMap::new();
            for &i in &member_indices {
                for (term, weight) in &vectors[i] {
                    *merged.entry(term.clone()).or_insert(0.0) += weight;
                }
            }

            let mut sorted_terms: Vec<(String, f64)> = merged.into_iter().collect();
            sorted_terms
                .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            sorted_terms.truncate(top_n);

            let label = sorted_terms
                .iter()
                .take(3)
                .map(|(t, _)| t.as_str())
                .collect::<Vec<&str>>()
                .join(", ");

            let keywords: Vec<Value> = sorted_terms
                .iter()
                .map(|(t, w)| {
                    let mut r = Record::new();
                    r.push("term", Value::string(t, head));
                    r.push("weight", Value::float(*w, head));
                    Value::record(r, head)
                })
                .collect();

            let members: Vec<Value> = member_indices
                .iter()
                .map(|&i| Value::int(i as i64, head))
                .collect();

            let mut cat = Record::new();
            cat.push("id", Value::int(cluster_idx as i64, head));
            cat.push("label", Value::string(&label, head));
            cat.push("size", Value::int(member_indices.len() as i64, head));
            cat.push("keywords", Value::list(keywords, head));
            cat.push("members", Value::list(members, head));

            categories.push(Value::record(cat, head));
        }

        let mut result = Record::new();
        result.push("name", Value::string("generated", head));
        result.push("num_clusters", Value::int(actual_k as i64, head));
        result.push("num_items", Value::int(n as i64, head));
        result.push("linkage", Value::string(&linkage_str, head));
        result.push("categories", Value::list(categories, head));

        Ok(PipelineData::Value(Value::record(result, head), None))
    }
}
