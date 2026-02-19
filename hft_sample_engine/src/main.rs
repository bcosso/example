use actix_web::{App, HttpResponse, HttpServer, post, web::{Json, Path, Data, get}};

use data_struc::Order;
use data_struc::PriceRanges;
use rsocket_rust::Client;
use tokio::runtime::Runtime;
use tokio::time::{self, Duration, Instant};
use rsocket_rust::prelude::*;
use rsocket_rust::Result;
use rsocket_rust_transport_tcp::TcpClientTransport;
use std::{env, io};
use std::collections::{HashMap, BTreeMap};
use std::hash::Hash;


mod conn_manager;
mod configs;
mod data_struc;


#[actix_rt::main]
async fn main() -> io::Result<()> {
    
    //let rt = Runtime::new().unwrap();
    //rt.block_on(execute_program());
    //
    tokio::spawn(async {
        println!("Task started on Tokio thread");
        loop{
            execute_program().await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        println!("Task finished");
    });

//    env::set_var("RUST_LOG", "actix_web=debug,actix_server=info");
//    env_logger::init();
//    let name = ConnectionManager::getName();

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
//                .service(request_methods::get)
                .service(request_range)

        })
    .bind("0.0.0.0:9090")?
    .run()
    .await
}

#[post("/request_range")]
pub async fn request_range() -> HttpResponse {
    let text_result: &str = "";
    
    match text_result {
        "res" => HttpResponse::Ok()
            .content_type("application/json")
            .json(text_result),
        "" => HttpResponse::NoContent()
            .content_type("application/json")
            .await
            .unwrap(),
        _ => panic!("Error")
    }
}

async fn execute_program(){

    let mut peers = configs::read_config_file().unwrap();
    let connec = conn_manager::create_instance(peers.clone()).await.unwrap();
   
    let result = match execute_in_cluster("execute_something", "{}", peers[0].clone(), &connec).await{
        Ok(data) => data,
        _ => { return (); }
    };

    //println!("{}", result);

    //let mut prices = data_struc::PriceRanges::new(); 
    
    //let mut prices: data_struc::PriceRanges = serde_json::from_str(&result)?; 
    let mut security: HashMap<String, data_struc::PriceRanges> = serde_json::from_str(&result).unwrap(); 
   
    let result2 = match execute_in_cluster("execute_something", "{}", peers[0].clone(), &connec).await{
        Ok(data) => data,
        _ => { return (); }
    };

    //println!("{}", result2);

    //let mut prices = data_struc::PriceRanges::new(); 
    
    //let mut prices: data_struc::PriceRanges = serde_json::from_str(&result).unwrap(); 
    let security2: HashMap<String, data_struc::PriceRanges> = serde_json::from_str(&result2).unwrap();
    //serde_json::from_str(result)
    //println!("{:?}", security2);
    
    let start = Instant::now();    
let security_wrapped: HashMap<String, Value<String, data_struc::PriceRanges>> =
    security
        .into_iter()
        .map(|(k, v)| (k, Value::Leaf(v)))
        .collect();

let security_wrapped2: HashMap<String, Value<String, data_struc::PriceRanges>> =
    security2
        .into_iter()
        .map(|(k, v)| (k, Value::Leaf(v)))
        .collect();

    let src = AnyMap::Hash(security_wrapped2);
    let mut dst = AnyMap::Hash(security_wrapped);


    build(&"AMZ".to_string() , &src, &mut dst);
    let duration = start.elapsed();

    println!("{:?}", dst);
    println!("Elapsed time: {:.2?}", duration);
}

#[derive(Clone, Debug)]
enum AnyMap<K, V> {
    Hash(HashMap<K, V>),
    BTree(BTreeMap<K, V>),
}


impl<K, V> AnyMap<K, V> {
    fn get(&self, key: &K) -> Option<&V>
    where
        K: Eq + Hash + Ord,
    {
        match self {
            AnyMap::Hash(m) => m.get(key),
            AnyMap::BTree(m) => m.get(key),
        }
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V>
    where
        K: Eq + Hash + Ord,
    {
        match self {
            AnyMap::Hash(m) => m.get_mut(key),
            AnyMap::BTree(m) => m.get_mut(key),
        }
    }
    fn contains_key(&self, key: &K) -> bool
    where
        K: Eq + Hash + Ord,
    {
        match self {
            AnyMap::Hash(m) => m.contains_key(key),
            AnyMap::BTree(m) => m.contains_key(key),
        }
    }

    fn insert(&mut self, key: K, value: V)
    where
        K: Eq + Hash + Ord,
    {
        match self {
            AnyMap::Hash(m) => { m.insert(key, value); }
            AnyMap::BTree(m) => { m.insert(key, value); }
        }
    }

    /// Iterate keys (returned as a boxed iterator to unify types)
    fn keys<'a>(&'a self) -> Box<dyn Iterator<Item = &'a K> + 'a> {
        match self {
            AnyMap::Hash(m) => Box::new(m.keys()),
            AnyMap::BTree(m) => Box::new(m.keys()),
        }
    }

    /// Create an empty map of the same variant as `self`.
    fn empty_like(&self) -> Self {
        match self {
            AnyMap::Hash(_) => AnyMap::Hash(HashMap::new()),
            AnyMap::BTree(_) => AnyMap::BTree(BTreeMap::new()),
        }
    }
}



#[derive(Clone, Debug)]
enum Value<K, Leaf> {
    Leaf(Leaf),
    Map(AnyMap<K, Value<K, Leaf>>),
    //OOrder(Order),
}


fn build<'b, K, L>(
    key: &K,
    src: &AnyMap<K, Value<K, L>>,
    dst: &mut AnyMap<K, Value<K, L>>,
)
where
    K: Eq + std::hash::Hash + Clone + Ord,
    L: Clone,
{
    //if dst.contains_key(key) {
    //    return;
    //}

    if let Some(v) = src.get(key) {
        match v {
            Value::Leaf(l) => {
                if dst.contains_key(key){
                    if let Some(val) = dst.get_mut(key) {
                        *val = Value::Leaf(l.clone());
                    }
                }else{
                    dst.insert(key.clone(), Value::Leaf(l.clone()));
                }
            }
            Value::Map(map) => {
                if dst.contains_key(key){

                    if let Some(Value::Map(new_map)) = dst.get_mut(key){
                        for child_key in map.keys() {

                            //let mut value_map = Value::Map(new_map);
                            build(child_key, map, new_map);
                        }
                        //dst.insert(key.clone(), Value::Map(new_map));
                    }
                }else{
                    let mut new_map = map.empty_like();
                    for child_key in map.keys() {
                        build(child_key, map, &mut new_map);
                    }
                    dst.insert(key.clone(), Value::Map(new_map));
                }
            }
            //Value::OOrder(ord) => {
                
            //}
        }
    }
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
