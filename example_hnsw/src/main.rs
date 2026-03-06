use hnsw_rs::prelude::*; // Hnsw, DistL2, etc.
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct EmbeddingResponse {
    model: String,
    embeddings: Vec<Vec<f32>>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ---- 1) Example input: you can paste your JSON here or load from a file ----
    // For real use, read from file: std::fs::read_to_string("emb.json")?
    //let json = r#"
    //{
    //  "model": "llama3.2",
    //  "embeddings": [
    //    [0.0, 1.0, 2.0, 3.0]
    //  ]
    //}
    //"#;
    //


    let json = std::fs::read_to_string("emb.json")?;
    let payload: EmbeddingResponse = serde_json::from_str(&json)?;
    println!("Model: {}", payload.model);

    // ---- 2) Data: "array of vectors" ----
    // This is exactly the shape you provided: Vec<Vec<f32>>.
    let vectors: Vec<Vec<f32>> = payload.embeddings;

    // Optional safety: ensure all vectors have the same dimension
    let dim = vectors.first().map(|v| v.len()).unwrap_or(0);
    assert!(dim > 0, "No embeddings found");
    assert!(vectors.iter().all(|v| v.len() == dim), "Inconsistent dimensions");

    // ---- 3) Create the HNSW index ----
    // Hnsw::new(max_nb_connection, max_elements, max_layer, ef_construction, distance_fn). [1](https://github.com/jean-pierreBoth/hnswlib-rs/blob/master/src/hnsw.rs)
    let max_nb_connection = 16;
    let max_elements = vectors.len();
    let max_layer = 16;
    let ef_construction = 200;

    let hnsw: Hnsw<f32, DistL2> = Hnsw::new(
        max_nb_connection,
        max_elements,
        max_layer,
        ef_construction,
        DistL2::default(), // L2 distance. 
    );

    // ---- 4) Insert vectors with stable external IDs ----
    // insert_slice takes (&[T], usize). [1](https://github.com/jean-pierreBoth/hnswlib-rs/blob/master/src/hnsw.rs)
    for (id, v) in vectors.iter().enumerate() {
        hnsw.insert_slice((v.as_slice(), id));
    }

    // ---- 5) Search (kNN) ----
    // Search for the nearest neighbors of a query vector.
    let query = vectors[0].clone(); // for demo: search the first vector

    let k = 5;
    let ef_search = 50; // must be > k; controls search width. [1](https://github.com/jean-pierreBoth/hnswlib-rs/blob/master/src/hnsw.rs)
    let results = hnsw.search(query.as_slice(), k, ef_search); // [1](https://github.com/jean-pierreBoth/hnswlib-rs/blob/master/src/hnsw.rs)

    println!("Top-{k} neighbors:");
    for n in results {
        // Common fields are: n.d_id (external id) and n.distance.
        println!("  id={}  dist={:.6}", n.d_id, n.distance);
    }

    Ok(())
}
