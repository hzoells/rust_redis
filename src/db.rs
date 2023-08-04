use bytes::Bytes;
use std::{collections::HashMap, sync::{Arc, Mutex}};

#[derive(Clone, Debug)]
pub struct Db {
  entries: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl Db {
  pub fn new() -> Db {
    Db {
      entries: Arc::new(Mutex::new(HashMap::new())),
    }
  }

  pub fn write(&self, arr: &[String]) -> Result<&str, &'static str> {
    let key = &arr[1];
    let value = &arr[2];
    
    let val = value.clone();
    let res: &Option<Bytes> = &self.entries.lock().unwrap().insert(String::from(key), Bytes::from(val));

    match res {
      Some(_res) => Ok("r Ok"),
      None => Ok("Ok"),
    }
  }

  pub fn read(&self, arr: &[String]) -> Result<Bytes, &'static str> {
    let key = &arr[1];
    let entries_ref = self.entries.lock().unwrap();
    let query_result = entries_ref.get(key);

    match query_result {
      Some(value) => Ok(value.clone()),
      None => Err("no such key found"),
    }
  }
}