use std::fmt;
use crate::configs;
use crate::configs::Peer;
use serde::{Deserialize, Serialize};
use rsocket_rust::prelude::*;
use rsocket_rust::Result;
use rsocket_rust::Client;
use rsocket_rust_transport_tcp::TcpClientTransport;
use std::collections::HashMap;
use tokio::runtime::Runtime;

pub struct ConnManager{
    pub connections: HashMap<String, Client>,
}

impl ConnManager {
    pub fn new() -> Self {
        ConnManager{connections : HashMap::new(),}
    }

    //Receive peers and iterate creation of clients
    
    pub fn getName(&self) -> String{
        return "Test".to_string();
    }

    pub async fn InitConnections(&mut self, peers :Vec<Peer>) -> Result<String>{

        println!("ANOTHER");
        for item in peers {
            println!("BeforeCC");
            self.createClient(item).await;
            println!("ClientCreation");
        }
        println!("After {}",self.connections.len());
        Ok(String::from("Ok"))
    }

    pub async fn createClient(&mut self, peer: configs::Peer) -> Result<String>{

        let name_peer = peer.name;
        let port_peer = peer.port;
        let host_peer = peer.ip;
        let host_server = format!("{host_peer}:{port_peer}");
        
        let cli = RSocketFactory::connect()
            .transport(TcpClientTransport::from(host_server))
            .setup(Payload::from("READY!"))
            .mime_type("text/plain", "text/plain")
            .on_close(Box::new(|| println!("connection closed")))
            .start()
            .await?;
        self.connections.insert(name_peer.to_string(), cli);
        Ok(String::from("Ok"))
        
        //add cli to the list
    }
}


pub async  fn create_instance(peer: Vec<Peer>) -> Result<ConnManager> {
    let mut conn = ConnManager::new();

    println!("TEST");

    conn.InitConnections(peer).await;
        //println!("got: {:?}", res);
        //let var_garbage = "Anything";
        //let result2 = res.unwrap();    
        //conn.InitConnections(peer);

    //let co = conn.connections.get("server_source").unwrap();
    //let method = "{\"method\":\"execute_something\"}";
    //let data = "{\"method\":\"/execute_something\",\"payload\":{}}";
    //    let req = Payload::builder()
    //        .set_data_utf8(&data)
    //        .set_metadata_utf8(method)
    //        .build();

     //   let rt = Runtime::new().unwrap();

      //  let res = rt.block_on(co.request_response(req));
        
    Ok(conn)
}


pub fn getName() -> String {
    // let mut conn = create_instance();
    // return conn.getName();
    return "Ok".to_string();
}






