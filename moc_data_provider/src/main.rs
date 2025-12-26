use std::collections::{HashMap, BTreeMap};
use rand::prelude::*;
use ordered_float::OrderedFloat;
use serde::{Serialize,Deserialize};
use serde_json::*;
use serde_with::*;

//#[derive(Serialize, Deserialize, Clone)]
//#[derive(Default)]
#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub struct Order<'b> {
    pub name: &'b str,
    pub order_id: i32,
    pub buy_sell: i8,
    pub price: f64,
    pub qty: i32
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub struct PriceRanges <'b> {

    #[serde_as(as = "HashMap<_, BTreeMap<_, _>>")]
    // This tells Serde that data for Order<'b> may be borrowed from the input
    #[serde(borrow)]
    pub ranges: HashMap<String, BTreeMap<OrderedFloat<f64>,Order<'b>>>,
}


fn main() {
    let mut security: HashMap<String, PriceRanges> = HashMap::new();

    let mut price= PriceRanges::new();
    price.get_price_ranges();
    security.insert("AMZ".to_string(), price);
    let st = serde_json::to_string(&security);
    println!("Hello {:?}", st);
}

impl <'b> PriceRanges <'b> {
    pub fn new() -> Self {
        PriceRanges{ranges : HashMap::new(),}
    }


    pub fn get_price_ranges(&mut self){
        let mut rnd = rand::rng();
        let mut result: PriceRanges;
        let mut count :i32;
        count = 1;
        let mut range: BTreeMap<OrderedFloat<f64>, Order<'b>> = BTreeMap::new();
        while count < 1001{
            let mut nums: Vec<i32> = (1..50).collect();
            nums.shuffle(&mut rnd);
// And take a random pick (yes, we didn't need to shuffle first!):
            let qtyNum = nums.choose(&mut rnd);
            let rnd_price:f64 = (*qtyNum.unwrap() as f64)/10.0 + 10.0;
            let mut order = Order {name: "AMZ", buy_sell: 0, price: rnd_price, qty: *(qtyNum.unwrap()), order_id: count }; 
            range.insert(OrderedFloat(order.price), order);
            count = count + 1;
        }
        self.ranges.insert("15".to_string(), range);

        count = 1;
        let mut range2: BTreeMap<OrderedFloat<f64>, Order<'b>> = BTreeMap::new();
          while count < 1001{

              let mut nums: Vec<i32> = (1..100).collect();
            nums.shuffle(&mut rnd);
            // And take a random pick (yes, we didn't need to shuffle first!):
            let qtyNum = nums.choose(&mut rnd);
            let rnd_price:f64 = ((*qtyNum.unwrap()) as f64) /10.0 + 15.0;
             let mut order = Order {name: "AMZ", buy_sell: 0, price: rnd_price, qty: *(qtyNum.unwrap()), order_id: count };
             range2.insert(OrderedFloat(order.price), order);
             count = count + 1;
          }
          self.ranges.insert("20".to_string(), range2);

    }
}

