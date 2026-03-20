use actix_web::{cookie::time::Error, post, web::{get, Data, Json, Path}, App, HttpResponse, HttpServer};

use data_struc::Order;
use data_struc::PriceRanges;
use rsocket_rust::Client;
use serde_json::to_string;
use tokio::runtime::Runtime;
use tokio::time::{self, Duration, Instant};
use rsocket_rust::prelude::*;
use rsocket_rust::Result;
use rsocket_rust_transport_tcp::TcpClientTransport;
use std::{env, io};
use std::collections::{HashMap, BTreeMap};
use std::hash::Hash;
use std::fmt::Write;
use std::result::Result as OtherResult;
use ordered_float::OrderedFloat;
use http::{Request, Response};
use reqwest;
use hnsw_rs::prelude::*; // Hnsw, DistL2, etc.
use serde::{de::Error as OtherError, Deserialize};


mod conn_manager;
mod configs;
mod data_struc;

use once_cell::sync::Lazy;
use std::sync::{Arc, atomic::AtomicBool, atomic::Ordering};
use tokio::sync::RwLock;


use thiserror::Error;

#[derive(Error, Debug)]
pub enum SearchError {
    #[error("No matching previous search found")]
    NotFound,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}


static HNSW_INDEX: Lazy<Arc<RwLock<Hnsw<f32, DistL2>>>> = Lazy::new(|| {
    let json = std::fs::read_to_string("emb.json").expect("Failed to read emb.json");
    let payload: EmbeddingResponse = serde_json::from_str(&json).expect("Failed to parse JSON");
    
    let mut hnsw = Hnsw::new(16, payload.embeddings.len(), 16, 200, DistL2::default());

    for (i, vec) in payload.embeddings.into_iter().enumerate() {
        hnsw.insert_slice((vec.as_slice(), i));
    }

    Arc::new(RwLock::new(hnsw))
});

pub static ANSWERS: Lazy<Arc<RwLock<HashMap<String, String>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));



#[actix_rt::main]
async fn main() -> io::Result<()> {
    


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

#[post("/request_search")]
pub async fn request_search(post_data: Json<data_struc::PostQuery>) -> HttpResponse {
    let mut text_result: &str = "";
    let mut cli = reqwest::Client::new();
    
    //{'model':'llama3.2', 'input': 'what is the derivative of x ^ 2? ', 'stream':False}
    let mut query = format!("{{\"model\":\"llama3.2\", \"input\": {:?}, \"stream\":false}}", post_data.query.clone());
    let result_response = cli.post("http://127.0.0.1:11434/api/embed").body(reqwest::Body::from(query.clone())).send().await; 
    if let Ok(text_response) = result_response.unwrap().text().await{
        println!("{:?}",text_response);
        
        let payload: EmbeddingResponse = serde_json::from_str(&text_response).expect("Not satisfied");
        println!("Model: {}", payload.model);

        let vectors: Vec<Vec<f32>> = payload.embeddings;
            match check_previous_searches_main(vectors[0].clone()).await{
            
                Ok(answer_is_inmem) => {
                
                    if answer_is_inmem == ""{
                        query = format!("{{\"model\":\"llama3.2\", \"prompt\": {:?}, \"stream\":false}}", post_data.query.clone());
                        let result_response_search = cli.post("http://127.0.0.1:11434/api/whatever").body(reqwest::Body::from(query.clone())).send().await;
                        //insert into cache (HNSW_INDEX and ANSWERS)
                        let answer = result_response_search.unwrap().text().await;
                        let answer_cloned = answer.expect("REASON").clone();
                        add_new_vector(vectors[0].clone(), answer_cloned.clone());
                        return HttpResponse::Ok()
                        .content_type("application/json")
                        // .json((*lock_result).to_string())
                        .json(answer_cloned.clone());
                    }else{
                        return HttpResponse::Ok()
                        .content_type("application/json")
                        // .json((*lock_result).to_string())
                        .json(answer_is_inmem);
                    }

                },
                Err(_) => {}
           }
    }
   
    HttpResponse::NoContent().content_type("application/json")
            .await
            .unwrap()

}

#[derive(Debug, Deserialize)]
struct EmbeddingResponse {
    model: String,
    embeddings: Vec<Vec<f32>>,
}

async fn add_new_vector(vec: Vec<f32>, answer: String){
    let mut hnsw_list = HNSW_INDEX.write().await;
    let mut index_vector: usize = hnsw_list.get_nb_point();
    hnsw_list.insert_slice((vec.as_slice(), index_vector));
    let mut answers_write = ANSWERS.write().await;
    answers_write.insert(index_vector.to_string(), answer);


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
    let json = std::fs::read_to_string("emb.json")?;
    let payload: EmbeddingResponse = serde_json::from_str(&json)?;
    println!("Model: {}", payload.model);

    let vectors: Vec<Vec<f32>> = payload.embeddings;

    let k = 5;
    let ef_search = 50; 
    let mut searches = HNSW_INDEX.write().await;
    //let hnsw: Hnsw<f32, DistL2> = Hnsw::new(
    
    //let mut owned_map: Hnsw<f32, DistL2> = std::mem::take(&mut *searches);
    let query = vectors[0].clone();
    
    //*sec = owned_map;

    //let results = searches.search(query.as_slice(), k, ef_search); 
    let results = searches.search(&search_text, k, ef_search);

    //*searches = owned_map; 
    println!("Top-{k} neighbors:");
    for n in results {
       if n.distance < 0.3 {
            //return answer, should be in the global
            let mut answer = ANSWERS.read().await;
            
            if let Some(result) = answer.get(&(n.distance.to_string())){
                return Ok(result.clone());
            }

        }else{
            //should only check the first that fits, if it doesn't, break
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
    println!("{}", name_peer);

    if let Some(cli) = conn.connections.get(&name_peer){    
        println!("GOT THE CONNECTION IN HASH");
        let method = "{\"method\":\"execute_something\"}";
        let data = format!("{{\"method\":\"/{name_peer}/{name_method}\",\"payload\":{{\"query\":\"{data_json}\"}}}}");
        let req = Payload::builder()
            .set_data_utf8(&data)
            .set_metadata_utf8(method)
            .build();

        let res = cli.request_response(req).await?;

        println!("GOT A RESPONSE!");
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
