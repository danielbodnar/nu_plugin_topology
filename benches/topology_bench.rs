use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use nu_plugin_topology::algo::{clustering, discover, lsh, simhash, tfidf, tokenizer};
use std::collections::HashMap;

/// Generate synthetic text data for benchmarking
fn generate_texts(n: usize) -> Vec<String> {
    let domains = [
        "rust programming memory safety borrow checker ownership lifetime",
        "python data science machine learning pandas numpy tensorflow",
        "javascript web development react angular vue node express",
        "go concurrency goroutines channels microservices kubernetes docker",
        "java enterprise spring boot hibernate jpa gradle maven",
        "database sql postgresql mysql redis mongodb cassandra",
        "devops ci cd pipeline github actions jenkins terraform ansible",
        "security authentication authorization oauth jwt encryption",
        "cloud aws azure gcp serverless lambda functions compute",
        "mobile ios android flutter react native swift kotlin",
    ];
    (0..n)
        .map(|i| {
            let base = domains[i % domains.len()];
            format!("{base} item-{i} extra-context-{}", i % 100)
        })
        .collect()
}

fn bench_tokenize(c: &mut Criterion) {
    let text = "rust programming language memory safety borrow checker ownership system";
    c.bench_function("tokenize/single", |b| {
        b.iter(|| tokenizer::tokenize(black_box(text)))
    });
}

fn bench_simhash(c: &mut Criterion) {
    let texts = generate_texts(1000);
    let token_lists: Vec<Vec<String>> = texts.iter().map(|t| tokenizer::tokenize(t)).collect();

    c.bench_function("simhash/1000_items", |b| {
        b.iter(|| {
            for tokens in &token_lists {
                black_box(simhash::simhash_uniform(tokens));
            }
        })
    });
}

fn bench_corpus_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("corpus_build");
    for size in [100, 1000, 5000] {
        let texts = generate_texts(size);
        let token_lists: Vec<Vec<String>> = texts.iter().map(|t| tokenizer::tokenize(t)).collect();

        group.bench_with_input(BenchmarkId::from_parameter(size), &token_lists, |b, tl| {
            b.iter(|| {
                let mut corpus = tfidf::Corpus::new();
                for tokens in tl {
                    corpus.add_document(tokens);
                }
                black_box(corpus)
            })
        });
    }
    group.finish();
}

fn bench_lsh_index(c: &mut Criterion) {
    let texts = generate_texts(5000);
    let token_lists: Vec<Vec<String>> = texts.iter().map(|t| tokenizer::tokenize(t)).collect();
    let fingerprints: Vec<u64> = token_lists
        .iter()
        .map(|t| simhash::simhash_uniform(t))
        .collect();

    c.bench_function("lsh_index/5000_insert_query", |b| {
        b.iter(|| {
            let mut idx = lsh::SimHashLshIndex::default_64();
            for (i, &fp) in fingerprints.iter().enumerate() {
                idx.insert(i, fp);
            }
            black_box(idx.candidate_pairs().len())
        })
    });
}

fn bench_distance_matrix(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance_matrix");
    for size in [50, 100, 200] {
        let texts = generate_texts(size);
        let token_lists: Vec<Vec<String>> = texts.iter().map(|t| tokenizer::tokenize(t)).collect();
        let mut corpus = tfidf::Corpus::new();
        for tokens in &token_lists {
            corpus.add_document(tokens);
        }
        let vectors: Vec<HashMap<String, f64>> = (0..size).map(|i| corpus.tfidf_vector(i)).collect();

        group.bench_with_input(BenchmarkId::from_parameter(size), &vectors, |b, vecs| {
            b.iter(|| black_box(clustering::cosine_distance_matrix(vecs)))
        });
    }
    group.finish();
}

fn bench_hac(c: &mut Criterion) {
    let mut group = c.benchmark_group("hac");
    for size in [50, 100, 200] {
        let texts = generate_texts(size);
        let token_lists: Vec<Vec<String>> = texts.iter().map(|t| tokenizer::tokenize(t)).collect();
        let mut corpus = tfidf::Corpus::new();
        for tokens in &token_lists {
            corpus.add_document(tokens);
        }
        let vectors: Vec<HashMap<String, f64>> = (0..size).map(|i| corpus.tfidf_vector(i)).collect();
        let distances = clustering::cosine_distance_matrix(&vectors);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            b.iter(|| {
                let dend = clustering::hac(&distances, n, clustering::Linkage::Ward);
                black_box(clustering::cut_tree(&dend, 10))
            })
        });
    }
    group.finish();
}

fn bench_discover_taxonomy(c: &mut Criterion) {
    let mut group = c.benchmark_group("discover_taxonomy");
    group.sample_size(10); // Expensive — fewer samples
    for size in [100, 500] {
        let texts = generate_texts(size);
        let config = discover::DiscoverConfig {
            k: 10,
            sample_size: size.min(500),
            label_terms: 3,
            keywords_per_cluster: 20,
            linkage: clustering::Linkage::Ward,
            seed: 42,
        };

        group.bench_with_input(BenchmarkId::from_parameter(size), &texts, |b, t| {
            b.iter(|| black_box(discover::discover_taxonomy(t, &config)))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_tokenize,
    bench_simhash,
    bench_corpus_build,
    bench_lsh_index,
    bench_distance_matrix,
    bench_hac,
    bench_discover_taxonomy,
);
criterion_main!(benches);
