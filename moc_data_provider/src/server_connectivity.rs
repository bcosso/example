use std::iter;
use std::sync::Arc;
use rsocket_rust::{prelude::*, Result, async_trait};
use rsocket_rust_transport_tcp::*;
use log::info;


use futures::stream;
use futures::StreamExt;

use std::collections::{HashMap, BTreeMap};
use rand::prelude::*;
use ordered_float::OrderedFloat;
use serde::{Serialize,Deserialize};
use serde_json::*;
use serde_with::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;

use crate::FILLED;
use crate::SECURITIES;

//#[derive(Serialize, Deserialize, Clone)]
//#[derive(Default)]
#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Order {
    pub name: String,
    pub order_id: i32,
    pub buy_sell: i8,
    pub price: f64,
    pub qty: i32
}



//#[derive(Clone)]
pub struct RRResponder{



}

impl RRResponder{
    //pub fn new() -> Self{
    //    Self{
    //        price : PriceRanges::new(), 
    //    }
    //}
    //pub fn get_prc(&self) -> &PriceRanges{
    //    return &self.price;
    //}
}

#[async_trait] // ← required to implement async methods in the trait
impl RSocket for RRResponder {

    async fn request_response(&self, payload: Payload) -> Result<Option<Payload>> {
        let data = payload.data_utf8().unwrap_or_default();
        let meta = payload.metadata_utf8().unwrap_or_default();
        info!("RR received: data='{}', metadata='{}'", data, meta);
        if data.contains("execute_something"){

            if FILLED.load(Ordering::Acquire) {

                let mut sec = SECURITIES.write().await;
                //.update_price_ranges();
                let price = &get_updated_price(&mut sec);

                let body = format!("{:}", price);
                
                let resp = Payload::builder()
                .set_data_utf8(&body)
                // .set_metadata_utf8("optional-meta") // if you want metadata
                .build();

                Ok(Some(resp))
            }else{

                
                let mut sec = SECURITIES.write().await;
                let price = &get_price(&mut sec);

                //let price = &get_price(&(self.securities));

                // Return a single response (echo example)
                
                let body = format!("{}", price);
                
                let resp = Payload::builder()
                .set_data_utf8(&body)
                // .set_metadata_utf8("optional-meta") // if you want metadata
                .build();

                FILLED.store(true, Ordering::Release);
                Ok(Some(resp))
            }
        }else{
            // Return a single response (echo example)
            
            let body = format!("echo: {}", data);
            
            let resp = Payload::builder()
            .set_data_utf8(&body)
            // .set_metadata_utf8("optional-meta") // if you want metadata
            .build();

            Ok(Some(resp))
        }

        
    }

    // --- You DON'T use these; keep them as NO-OPs so the trait is fully implemented ---
    async fn fire_and_forget(&self, _req: Payload) -> Result<()> {
        // no-op
        Ok(())
    }

    async fn metadata_push(&self, _req: Payload) -> Result<()> {
        // no-op
        Ok(())
    }

    // If your installed trait version still defines these (older API may differ),
    // provide trivial implementations. If your trait variant uses async versions
    // returning Result<Flux<Payload>>, change these signatures accordingly.
    fn request_stream(&self, _req: Payload) -> Flux<Result<Payload>> {
        // empty stream
        Box::pin(stream::empty::<Result<Payload>>())
    }

    fn request_channel(&self, _reqs: Flux<Result<Payload>>) -> Flux<Result<Payload>> {
        // echo-nothing stream
        Box::pin(stream::empty::<Result<Payload>>())
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriceRanges {

    #[serde_as(as = "HashMap<_, BTreeMap<DisplayFromStr, _>>")]
    // This tells Serde that data for Order<'b> may be borrowed from the inputg
    //#[serde(borrow)]
    pub ranges: HashMap<String, BTreeMap<OrderedFloat<f64>,Order>>,
}


pub fn get_price(security: &mut HashMap<String, PriceRanges>) -> String {
    //let mut security: HashMap<String, PriceRanges> = HashMap::new();

    let mut price= PriceRanges::new();
    price.get_price_ranges();
    security.insert("AMZ".to_string(), price.clone());
    let st = serde_json::to_string(&security);
    println!("Result :: {:?}", st);
    return st.expect("REASON");
}
pub fn get_updated_price(security: &mut HashMap<String, PriceRanges>) -> String {
    //let mut security: HashMap<String, PriceRanges> = HashMap::new();
    for val in security.values_mut(){
        val.update_price_ranges();

    }
       let st = serde_json::to_string(&security);
    println!("Result :: {:?}", st);
    return st.expect("REASON");
}
impl PriceRanges {
    pub fn new() -> Self {
        PriceRanges{ranges : HashMap::new(),}
    }


    pub fn get_price_ranges(&mut self){
        let mut rnd = rand::rng();
        let mut result: PriceRanges;
        let mut count :i32;
        count = 1;
        let mut range: BTreeMap<OrderedFloat<f64>, Order> = BTreeMap::new();
        while count < 1000000{
            let mut nums: Vec<i32> = (1..50).collect();
            nums.shuffle(&mut rnd);

            let qtyNum = nums.choose(&mut rnd);
            let rnd_price:f64 = (*qtyNum.unwrap() as f64)/10.0 + 10.0;
            let mut order = Order {name: "AMZ".to_string(), buy_sell: 0, price: rnd_price, qty: *(qtyNum.unwrap()), order_id: count }; 
            range.insert(OrderedFloat(order.price), order);
            count = count + 1;
        }
        self.ranges.insert("15".to_string(), range);

        count = 1;
        let mut range2: BTreeMap<OrderedFloat<f64>, Order> = BTreeMap::new();
        while count < 1000000{

            let mut nums: Vec<i32> = (1..100).collect();
            nums.shuffle(&mut rnd);
            let qtyNum = nums.choose(&mut rnd);
            let rnd_price:f64 = ((*qtyNum.unwrap()) as f64) /10.0 + 15.0;
            let mut order = Order {name: "AMZ".to_string(), buy_sell: 0, price: rnd_price, qty: *(qtyNum.unwrap()), order_id: count };
            range2.insert(OrderedFloat(order.price), order);
            count = count + 1;
        }
        self.ranges.insert("20".to_string(), range2);

    }

    pub fn update_price_ranges(&mut self){
        let mut rnd = rand::rng();
        let mut nums: Vec<i32> = (1..50).collect();
        nums.shuffle(&mut rnd);


        for range in self.ranges.values_mut(){
            for order in range.values_mut(){
               

                    order.qty = order.qty + 1;

                    
                
            }
        }
    }
}


