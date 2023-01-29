<p align="center">
<img 
    src="logo.png" 
    width="336" border="0" alt="REDCON">
<br>
<a href="LICENSE"><img src="https://img.shields.io/crates/l/redcon.svg?style=flat-square"></a>
<a href="https://crates.io/crates/redcon"><img src="https://img.shields.io/crates/d/redcon.svg?style=flat-square"></a>
<a href="https://crates.io/crates/redcon/"><img src="https://img.shields.io/crates/v/redcon.svg?style=flat-square"></a>
<a href="https://docs.rs/redcon/"><img src="https://img.shields.io/badge/docs-rustdoc-369?style=flat-square"></a>
</p>

<p align="center">Redis compatible server framework for Rust</p>

## Features

- Create a fast custom Redis compatible server in Rust
- Simple API.
- Support for pipelining and telnet commands.
- Works with Redis clients such as [redis-rs](https://github.com/redis-rs/redis-rs), [redigo](https://github.com/garyburd/redigo), [redis-py](https://github.com/andymccurdy/redis-py), [node_redis](https://github.com/NodeRedis/node_redis), and [jedis](https://github.com/xetorthio/jedis)
- Multithreaded

*This library is also avaliable for [Go](https://github.com/tidwall/redcon) and [C](https://github.com/tidwall/redcon.c).*

## Example

Here's a full [example](examples/kvstore) of a Redis clone that accepts:

- SET key value
- GET key
- DEL key
- PING
- QUIT

```rust
use std::collections::HashMap;
use std::sync::Mutex;

fn main() {
    let db: Mutex<HashMap<Vec<u8>, Vec<u8>>> = Mutex::new(HashMap::new());

    let mut s = redcon::listen("127.0.0.1:6380", db).unwrap();
    s.command = Some(|conn, db, args|{
        let name = String::from_utf8_lossy(&args[0]).to_lowercase();
        match name.as_str() {
            "ping" => conn.write_string("PONG"),
            "set" => {
                if args.len() < 3 {
                    conn.write_error("ERR wrong number of arguments");
                    return;
                }
                let mut db = db.lock().unwrap();
                db.insert(args[1].to_owned(), args[2].to_owned());
                conn.write_string("OK");
            }
            "get" => {
                if args.len() < 2 {
                    conn.write_error("ERR wrong number of arguments");
                    return;
                }
                let db = db.lock().unwrap();
                match db.get(&args[1]) {
                Some(val) => conn.write_bulk(val),
                None => conn.write_null(),
                }
            }
            _ => conn.write_error("ERR unknown command"),
        }
    });
    println!("Serving at {}", s.local_addr());
    s.serve().unwrap();
}
```
