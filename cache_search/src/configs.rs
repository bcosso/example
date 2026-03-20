use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::fs;
use std::clone::Clone;

#[derive(Serialize, Deserialize, Clone)]
pub struct Peer {
    pub name: String,
    pub port: String,
    pub ip: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub instance_name: String,
    pub load_balancing_algorithm: Option<String>,
    pub peers: Vec<Peer>,
}



pub fn read_config_file() -> Result<Vec<Peer>> {
    let file_path = "configfile.json".to_owned();
    let contents = fs::read_to_string(file_path).expect("Couldn't find or load that file.");
    let p: Config = serde_json::from_str(&contents)?;

    // println!("Test Read {}  {}", p.peers[0].port, p.peers[0].ip);

    Ok(p.peers)
}


