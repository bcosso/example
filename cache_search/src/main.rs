use actix_web::{cookie::time::Error, post, web::{get, Data, Json, Path}, App, HttpResponse, HttpServer};

use data_struc::Order;
use data_struc::PriceRanges;
use rsocket_rust::Client;
use serde_json::{Value, to_string};
use tokio::runtime::Runtime;
use tokio::time::{self, Duration, Instant};
use rsocket_rust::prelude::*;
use rsocket_rust::Result;
use rsocket_rust_transport_tcp::TcpClientTransport;
use std::{env, io, ptr::null};
use std::collections::{HashMap, BTreeMap};
use std::hash::Hash;
use std::fmt::Write;
use std::result::Result as OtherResult;
use ordered_float::OrderedFloat;
use http::{Request, Response};
use reqwest;
use hnsw_rs::prelude::*; // Hnsw, DistL2, etc.
use serde::{de::Error as OtherError, Deserialize, Serialize};
use once_cell::sync::{Lazy};
use std::sync::{Arc, atomic::AtomicBool, atomic::Ordering};
use tokio::sync::RwLock;
use tokio::sync::{OnceCell};
use thiserror::Error;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write as OtherWrite};
use std::path::Path as OtherPath;
use std::any::Any;
use crate::conn_manager::ConnManager;


mod conn_manager;
mod configs;
mod data_struc;

#[derive(Debug, Deserialize)]
struct EmbeddingResponse {
    model: String,
    embeddings: Vec<Vec<f32>>,
}

#[derive(Error, Debug)]
pub enum SearchError {
    #[error("No matching previous search found")]
    NotFound,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

const PATH_VEC: &str = "/home/henrico/projects/rust/example/cache_search/";
static DIST: DistL2 = DistL2 {};


static CONN: OnceCell<Arc<RwLock<ConnManager>>> = OnceCell::const_new();
static HNSW_INDEX: OnceCell<Arc<RwLock<Hnsw<'static, f32, DistL2>>>> = OnceCell::const_new();
static ANSWERS: OnceCell<Arc<RwLock<HashMap<String, String>>>> = OnceCell::const_new();

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AnswerType{
    pub response: String
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RowPersist {
    pub vector: Vec<f32> ,
    pub id_vector: usize,
    pub answer: AnswerType,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MemRow{
    pub Rows: RowPersist
}

async fn load_table_nimpha() -> Result<String>{
    let peers = configs::read_config_file().unwrap();

    let conn_lock = CONN.get().unwrap(); 

    let mut conn = conn_lock.write().await;    

    let table_str = "table_graph".to_string();
    let alias_str = "".to_string();

    //let json_vector = serde_json::to_string(&vector).expect("Serialization failed");
    let payload = format!("{{\"table\":\"{table_str}\",\"alias\":\"{alias_str}\"}}");

    // Debug: See what keys are actually available
    println!("Available keys in connections: {:?}", conn.connections.keys().collect::<Vec<_>>());
   
    println!("Passou");
    let result = execute_in_cluster("select_table", &payload, peers[0].clone(), &conn).await.unwrap();
    println!("{:?}", result);
    Ok(result)
}


async fn add_new_vector(vec: Vec<f32>, answer: String, mut index_vector: usize) {
    let arc_index = HNSW_INDEX.get().expect("HNSW_INDEX not initialized");
    let mut hnsw_list = arc_index.write().await;
    if index_vector == 0{ 
        index_vector = hnsw_list.get_nb_point();
    }
    
    hnsw_list.insert_slice((vec.clone().as_slice(), index_vector.clone()));
    
    let arc_answers = ANSWERS.get().expect("HNSW_INDEX not initialized");
    let mut answers_write = arc_answers.write().await;
    answers_write.insert(index_vector.to_string(), answer.clone());

    //let basename = "hnsw.dump"; // Must match the dump prefix exactly
    
    //let path_buf = std::path::PathBuf::from(PATH_VEC);

    // Pass the &Path and the &str prefix separately
    //if let Err(e) = hnsw_list.file_dump(&path_buf, basename) {
    //    eprintln!("HNSW dump failed: {}", e);
    //}
   
    //let f_ans = File::create("answers.json").expect("Failed to create answers file");
    //serde_json::to_writer(BufWriter::new(f_ans), &*answers_write).expect("Failed to write answers");

    create_payload_save_nimpha(vec.clone(), index_vector.clone(), answer.clone()).await;
    println!("\nNew vector added. Total points: {}", index_vector + 1);
}
pub async fn add_vector(vec:Vec<f32>, answer: String, mut index_vector: usize){

    let arc_index = HNSW_INDEX.get().expect("HNSW_INDEX not initialized");
    let mut hnsw_list = arc_index.write().await;
    if index_vector == 0{ 
        index_vector = hnsw_list.get_nb_point();
    }
    
    hnsw_list.insert_slice((vec.clone().as_slice(), index_vector.clone()));


    let arc_answers = ANSWERS.get().expect("HNSW_INDEX not initialized");
    let mut answers_write = arc_answers.write().await;
    answers_write.insert(index_vector.to_string(), answer.clone());


}

async fn create_payload_save_nimpha(vector: Vec<f32>, id:usize, answer: String){
    let peers = configs::read_config_file().unwrap();

    let conn_lock = CONN.get().unwrap(); 

    let mut conn = conn_lock.write().await;    

    let id_str = id.to_string();

    let json_vector = serde_json::to_string(&vector).expect("Serialization failed");
    let payload = format!("{{\"table\":\"table_graph\", \"body\":{{\"vector\":{json_vector},\"id_vector\":{id_str},\"answer\":{answer}}}}}");

    let result = execute_in_cluster("insert_endpoint", &payload, peers[0].clone(), &conn).await.unwrap();
    println!("{:?}", result);

}

#[actix_rt::main]
async fn main() -> io::Result<()> {
    init_conn().await;
    //load_table_nimpha().await;
//match CONN.set(Arc::new(RwLock::new(connec))) {
//    Ok(_) => println!("Successfully initialized CONN"),
//    Err(_) => println!("CONN was already initialized"),
//}
        HttpServer::new(|| {
//            let counter : u8 = 0;
//            let mutex_counter = Data::new(Mutex::new(counter));
//            let peers = configs::read_config_file().unwrap();
//            let conn = ConnectionManager::create_instance(peers);
//            let mutex_connections = Data::new(Mutex::new(conn));

                   
            App::new()
//                .app_data(Data::clone(&mutex_counter))
//                .app_data(Data::clone(&mutex_connections))
//                .wrap(middleware::Logger::default())
                .service(request_search)

        })
    .bind("0.0.0.0:9090")?
    .run()
    .await
}

pub async fn init_conn() {
    let peers = configs::read_config_file().unwrap();
    let connec = conn_manager::create_instance(peers.clone()).await.unwrap();
    let conn_arc = Arc::new(RwLock::new(connec));

    // 1. Initialize CONN
    CONN.set(conn_arc.clone()).ok();

    if let Ok(contents) = load_table_nimpha().await{
    
        
        // 2. Logic for HNSW_INDEX (Using conn_arc if needed)
        let directory = std::path::Path::new(PATH_VEC);
        let hnsw =
            Hnsw::new(16, 1000, 16, 200, DIST);
        HNSW_INDEX.set(Arc::new(RwLock::new(hnsw))).ok();

        // 3. Logic for ANSWERS
        let map =
            HashMap::new();
        ANSWERS.set(Arc::new(RwLock::new(map))).ok();
        if contents != "null"{
let rows: Vec<MemRow> = serde_json::from_str(&contents).expect("PArsing to object error");
        for row in rows{

            let row_value = row.clone();
            add_vector(row.Rows.vector, row.Rows.answer.response, row.Rows.id_vector).await;

        }
        }
    }
}

#[post("/request_search")]
pub async fn request_search(post_data: Json<data_struc::PostQuery>) -> HttpResponse {
    let mut peers = configs::read_config_file().unwrap();
    let connec = conn_manager::create_instance(peers.clone()).await.unwrap();

    let mut text_result: &str = "";
    let mut cli = reqwest::Client::new();
    
    //{'model':'llama3.2', 'input': 'what is the derivative of x ^ 2? ', 'stream':False}
    let mut query = format!("{{\"model\":\"llama3.2\", \"input\": {:?}, \"stream\":false}}", post_data.query.clone());
    let result_response = cli.post("http://127.0.0.1:11434/api/embed").body(reqwest::Body::from(query.clone())).send().await; 
    if let Ok(text_response) = result_response.unwrap().text().await{
        //println!("{:?}",text_response);
        
        let payload: EmbeddingResponse = serde_json::from_str(&text_response).expect("Not satisfied");
        //println!("Model: {}", payload.model);

        let vectors: Vec<Vec<f32>> = payload.embeddings;
        match check_previous_searches_main(vectors[0].clone()).await{
        
            Ok(answer_is_inmem) => {
            
                if answer_is_inmem == ""{
                    print!("answer not in mem");
                    query = format!("{{\"model\":\"llama3.2\", \"prompt\": {:?}, \"stream\":false}}", post_data.query.clone());
                    let result_response_search = cli.post("http://127.0.0.1:11434/api/generate").body(reqwest::Body::from(query.clone())).send().await;
                    //insert into cache (HNSW_INDEX and ANSWERS)
                    let answer = result_response_search.unwrap().text().await;
                    let answer_cloned = answer.expect("REASON").clone();
                    add_new_vector(vectors[0].clone(), answer_cloned.clone(), 0).await;
                    return HttpResponse::Ok()
                        .content_type("application/json")
                       .json(answer_cloned.clone());
                }else{

                    return HttpResponse::Ok()
                        .content_type("application/json")
                       .json(answer_is_inmem);
                }

            },
            Err(_) => {

                return HttpResponse::Ok()
                        .content_type("application/json")
                       .json("Error on checking previous searches");
            }
       }
    }
   
    HttpResponse::NoContent().content_type("application/json")
            .await
            .unwrap()

}

fn check_previous_searches(search_text : String) -> Result<()> {

    let json = std::fs::read_to_string("emb.json")?;
    let payload: EmbeddingResponse = serde_json::from_str(&json)?;
    println!("Model: {}", payload.model);

    let vectors: Vec<Vec<f32>> = payload.embeddings;

    // Optional safety: ensure all vectors have the same dimension
    let dim = vectors.first().map(|v| v.len()).unwrap_or(0);
    assert!(dim > 0, "No embeddings found");
    assert!(vectors.iter().all(|v| v.len() == dim), "Inconsistent dimensions");

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


    for (id, v) in vectors.iter().enumerate() {
        hnsw.insert_slice((v.as_slice(), id));
    }


    let query = vectors[0].clone(); // for demo: search the first vector

    let k = 5;
    let ef_search = 50; 
    let results = hnsw.search(query.as_slice(), k, ef_search); 
    println!("Top-{k} neighbors:");
    for n in results {
       println!("  id={}  dist={:.6}", n.d_id, n.distance);
    }

    Ok(())
}


async fn check_previous_searches_main(search_text : Vec<f32>) -> Result<String> {

    //let query = vectors[0].clone(); // for demo: search the first vector
    //let json = std::fs::read_to_string("emb.json")?;
    //let payload: EmbeddingResponse = serde_json::from_str(&json)?;
    //println!("Model: {}", payload.model);

    //let vectors: Vec<Vec<f32>> = payload.embeddings;

    let k = 5;
    let ef_search = 50;
    let arc_index = HNSW_INDEX.get().expect("HNSW_INDEX not initialized");
    let mut searches = arc_index.write().await;
    //let hnsw: Hnsw<f32, DistL2> = Hnsw::new(
    
    //let mut owned_map: Hnsw<f32, DistL2> = std::mem::take(&mut *searches);
    //let query = search_text;
    
    //*sec = owned_map;

    //let results = searches.search(query.as_slice(), k, ef_search); 
    let mut results = searches.search(&search_text, k, ef_search);
    results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal));
    //*searches = owned_map; 
    println!("Top-{k} neighbors:");
    for n in results {
       if n.distance < 0.3 {
            //return answer, should be in the global
            let arc_answers = ANSWERS.get().expect("HNSW_INDEX not initialized");
            let mut answer = arc_answers.read().await;
            
            if let Some(result) = answer.get(&(n.d_id.to_string())){
                return Ok(result.clone());
            }

        }else{
            //should only check the first that fits, if it doesn't, break
            print!("Rola!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            break;

        }
        println!("  id={}  dist={:.6}", n.d_id, n.distance);
    }

    Ok("".to_string())
}


async fn execute_in_cluster(name_method: &str, data_json: &str, peer: configs::Peer, conn: &conn_manager::ConnManager) -> Result<String>{

    let name_peer = peer.name;
    let port_peer = peer.port;
    let host_peer = peer.ip;
    let host_server = format!("{host_peer}:{port_peer}");
    println!("peer name {}", name_peer);
    for (k, v) in conn.connections.clone(){
        println!("{:?}", k);
    }

    if let Some(cli) = conn.connections.get(&name_peer){    

        let method = "{\"method\":\"execute_something\"}";
        let data = format!("{{\"method\":\"/{name_peer}/{name_method}\",\"payload\":{data_json}}}");
        let req = Payload::builder()
            .set_data_utf8(&data)
            .set_metadata_utf8(method)
            .build();

        let res = cli.request_response(req).await?;

        println!("{:?}", res);

        let result1 = match res{
            Some(resp) => resp,
            _ => { return Ok("Error in the connection".to_string()); }
        };
        let result3 = match result1.data_utf8(){
            Some(data) => data,
            _ => { return Ok("Error in UTF 8".to_string()); }
        };
        Ok(String::from(result3))
    }else{
        Ok("No connection found".to_string())
    }
}
