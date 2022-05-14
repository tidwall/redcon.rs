use super::*;
use std::net::TcpStream;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

struct Data {
    value1: usize,
    value2: usize,
}

#[test]
fn connections() {
    const N: usize = 11;
    const ADDR: &str = "127.0.0.1:11099";
    let db = Arc::new(Mutex::new(Data {
        value1: 0,
        value2: 0,
    }));
    for i in 0..N {
        thread::spawn(move || test_conn(i, ADDR).unwrap());
    }
    let mut s = listen(ADDR, db.clone()).unwrap();
    s.command = Some(|conn, data, args| {
        let name = String::from_utf8_lossy(&args[0]).to_lowercase();
        match name.as_str() {
            "shutdown" => {
                conn.write_string("OK");
                conn.shutdown();
            }
            "incr" => {
                data.lock().unwrap().value2 += 1;
                conn.write_string("OK");
            }
            _ => {
                conn.write_error(&format!("ERR unknown command '{}'", name));
            }
        }
    });
    s.opened = Some(|conn, data| {
        data.lock().unwrap().value1 += 1;
        if conn.id() == 5 {
            conn.write_error("ERR unauthorized");
            conn.close();
        }
        // println!("opened: {}", conn.id());
    });
    s.closed = Some(|_conn, data, _| {
        data.lock().unwrap().value1 -= 1;
        // println!("closed: {}", conn.id());
    });
    s.tick = Some(|data| {
        if data.lock().unwrap().value2 == N-1 {
            None
        } else {
            Some(Duration::from_millis(10))
        }
    });
    s.serve().unwrap();
    assert_eq!(db.lock().unwrap().value1, 0);
    assert_eq!(s.serve().is_err(), true);
}

fn make_conn(addr: &str) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        thread::sleep(Duration::from_millis(10));
        if start.elapsed() > Duration::from_secs(5) {
            return Err(From::from("Connection timeout"));
        }
        if let Ok(stream) = TcpStream::connect(addr) {
            return Ok(stream);
        }
    }
}

fn test_conn(_i: usize, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut wr = make_conn(addr)?;
    let mut rd = BufReader::new(wr.try_clone()?);
    wr.write("NooP 1 2 3\r\n".as_bytes())?;
    let mut line = String::new();
    rd.read_line(&mut line)?;
    if line == "-ERR unauthorized\r\n" {
        return Ok(());
    }
    assert_eq!(line, "-ERR unknown command 'noop'\r\n");
    wr.write("incr\r\n".as_bytes())?;
    let mut line = String::new();
    rd.read_line(&mut line)?;
    assert_eq!(line, "+OK\r\n");
    Ok(())
}
