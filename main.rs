use std::collections::{HashMap, BTreeMap};
use rand::prelude::*;
use ordered_float::OrderedFloat;

//#[derive(Serialize, Deserialize, Clone)]
#[derive(Default)]
pub struct Order<'b> {
    pub name: &'b str,
    pub order_id: i32,
    pub buy_sell: i8,
    pub price: f64,
    pub qty: i32
}

#[derive(Default)]
pub struct PriceRanges <'b> {
    pub ranges: HashMap<String, BTreeMap<OrderedFloat<f64>,Order<'b>>>,
}


fn main() {
    let mut security: HashMap<String, PriceRanges> = HashMap::new();

    let mut price= PriceRanges::new();
    price.get_price_ranges();
    security.insert("AMZ".to_string(), price);
    println!("Hello, world!");
}

impl <'b> PriceRanges <'b> {
    pub fn new() -> Self {
        PriceRanges{ranges : HashMap::new(),}
    }


    pub fn get_price_ranges(&mut self){
        let mut result: PriceRanges;
        let mut count :i32;
        count = 1;
        let mut range: BTreeMap<OrderedFloat<f64>, Order<'b>> = BTreeMap::new();
        while count < 1001{
            let mut order = Order {name: "AMZ", buy_sell: 0, price: 10.5, qty: 100, order_id: count }; 
            range.insert(OrderedFloat(order.price), order);
            count = count + 1;
        }
        self.ranges.insert("15".to_string(), range);
    }
}

