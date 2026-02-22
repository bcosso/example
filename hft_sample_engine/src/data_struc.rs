use std::iter;

use std::collections::{HashMap, BTreeMap};
use rand::prelude::*;
use ordered_float::OrderedFloat;
use serde::{Serialize,Deserialize};
use serde_json::*;
use serde_with::*;


#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Order {
    pub name: String,
    pub order_id: i32,
    pub buy_sell: i8,
    pub price: f64,
    pub qty: i32
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriceRanges {

    //#[serde_as(as = "HashMap<_, BTreeMap<_, _>>")]
    // This tells Serde that data for Order<'b> may be borrowed from the input
    //#[serde(borrow)]
    #[serde_as(as = "HashMap<_, BTreeMap<DisplayFromStr, _>>")]
    pub ranges: HashMap<String, BTreeMap<OrderedFloat<f64>,Order>>,
}

impl PriceRanges {
    pub fn new() -> Self {
        PriceRanges{ranges : HashMap::new(),}
    }
}

#[derive(Serialize, Deserialize,Debug)]
pub struct PostQuery {
    method: String,
    query: String
}
