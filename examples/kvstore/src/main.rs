use std::collections::HashMap;
use std::sync::Mutex;

fn main() {
    let db: Mutex<HashMap<Vec<u8>, Vec<u8>>> = Mutex::new(HashMap::new());

    let mut s = redcon::listen("127.0.0.1:6380", db).unwrap();
    s.command = Some(|conn, db, args|{
        let name = String::from_utf8_lossy(&args[0]).to_lowercase();
        match name.as_str() {
            "ping" => conn.write_string("PONG"),
            "quit" => {
				conn.write_string("OK");
				conn.close();
            }
            "set" => {
                if args.len() != 3 {
                    conn.write_error("ERR wrong number of arguments");
                    return;
                }
                let mut db = db.lock().unwrap();
                db.insert(args[1].to_owned(), args[2].to_owned());
                conn.write_string("OK");
            }
            "get" => {
                if args.len() != 2 {
                    conn.write_error("ERR wrong number of arguments");
                    return;
                }
                let db = db.lock().unwrap();
                match db.get(&args[1]) {
                Some(val) => conn.write_bulk(val),
                None => conn.write_null(),
                }
            }
            "del" => {
                if args.len() != 2 {
                    conn.write_error("ERR wrong number of arguments");
                    return;
                }
                let mut db = db.lock().unwrap();
                match db.remove(&args[1]) {
                Some(_) => conn.write_integer(1),
                None => conn.write_integer(0),
                }
            }
            _ => conn.write_error("ERR unknown command"),
        }
    });
    println!("Serving at {}", s.local_addr());
    s.serve().unwrap();
}
