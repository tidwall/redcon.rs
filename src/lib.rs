// Copyright 2022 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//! Redis compatible server framework for Rust

#[cfg(test)]
mod test;

use std::any::Any;
use std::collections::HashMap;
use std::io;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// A error type that is returned by the [`listen`] and [`Server::serve`]
/// functions and passed to the [`Server::closed`] handler.
#[derive(Debug)]
pub enum Error {
    // A Protocol error that was caused by malformed input by the client
    // connection.
    Protocol(String),
    // An I/O error that was caused by the network, such as a closed TCP
    // connection, or a failure or listen on at a socket address.
    IoError(io::Error),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IoError(error)
    }
}

impl Error {
    fn new(msg: &str) -> Error {
        Error::Protocol(msg.to_owned())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Error::Protocol(s) => write!(f, "{}", s),
            Error::IoError(e) => write!(f, "{}", e),
        }
    }
}

/// A client connection.
pub struct Conn {
    id: u64,
    addr: SocketAddr,
    reader: BufReader<Box<dyn Read>>,
    wbuf: Vec<u8>,
    writer: Box<dyn Write>,
    closed: bool,
    shutdown: bool,
    cmds: Vec<Vec<Vec<u8>>>,
    conns: Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>>,
    /// A custom user-defined context.
    pub context: Option<Box<dyn Any>>,
}

impl Conn {
    /// A distinct identifier for the connection.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The connection socket address.
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    /// Read the next command in the pipeline, if any.
    ///
    /// This method is not typically needed, but it can be used for reading
    /// additional incoming commands that may be present. Which, may come in
    /// handy for specialized stuff like batching operations or for optimizing
    /// a locking strategy.
    pub fn next_command(&mut self) -> Option<Vec<Vec<u8>>> {
        self.cmds.pop()
    }

    /// Write a RESP Simple String to client connection.
    ///
    /// <https://redis.io/docs/reference/protocol-spec/#resp-simple-strings>
    pub fn write_string(&mut self, msg: &str) {
        if !self.closed {
            self.extend_lossy_line(b'+', msg);
        }
    }
    /// Write a RESP Null Bulk String to client connection.
    ///
    /// <https://redis.io/docs/reference/protocol-spec/#resp-bulk-strings>
    pub fn write_null(&mut self) {
        if !self.closed {
            self.wbuf.extend("$-1\r\n".as_bytes());
        }
    }
    /// Write a RESP Error to client connection.
    ///
    /// <https://redis.io/docs/reference/protocol-spec/#resp-errors>
    pub fn write_error(&mut self, msg: &str) {
        if !self.closed {
            self.extend_lossy_line(b'-', msg);
        }
    }
    /// Write a RESP Integer to client connection.
    ///
    /// <https://redis.io/docs/reference/protocol-spec/#resp-errors>
    pub fn write_integer(&mut self, x: i64) {
        if !self.closed {
            self.wbuf.extend(format!(":{}\r\n", x).as_bytes());
        }
    }
    /// Write a RESP Array to client connection.
    ///
    /// <https://redis.io/docs/reference/protocol-spec/#resp-arrays>
    pub fn write_array(&mut self, count: usize) {
        if !self.closed {
            self.wbuf.extend(format!("*{}\r\n", count).as_bytes());
        }
    }
    /// Write a RESP Bulk String to client connection.
    ///
    /// <https://redis.io/docs/reference/protocol-spec/#resp-simple-strings>
    pub fn write_bulk(&mut self, msg: &[u8]) {
        if !self.closed {
            self.wbuf.extend(format!("${}\r\n", msg.len()).as_bytes());
            self.wbuf.extend(msg);
            self.wbuf.push(b'\r');
            self.wbuf.push(b'\n');
        }
    }

    /// Write raw bytes to the client connection.
    pub fn write_raw(&mut self, raw: &[u8]) {
        if !self.closed {
            self.wbuf.extend(raw);
        }
    }

    /// Close the client connection.
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Shutdown the server that was started by [`Server::serve`].
    ///
    /// This operation will gracefully shutdown the server by closing the all
    /// client connections, stopping the server listener, and waiting for the
    /// server resources to free. 
    pub fn shutdown(&mut self) {
        self.closed = true;
        self.shutdown = true;
    }

    /// Close a client connection that is not this one.
    ///
    /// The identifier is for a client connection that was connection to the
    /// same server as `self`. This operation can safely be called on the same
    /// identifier multiple time.
    pub fn cross_close(&mut self, id: u64) {
        if let Some(xcloser) = self.conns.lock().unwrap().get(&id) {
            xcloser.store(true, Ordering::SeqCst);
        }
    }

    fn pl_read_array(&mut self, line: Vec<u8>) -> Result<Option<Vec<Vec<u8>>>, Error> {
        let n = match String::from_utf8_lossy(&line[1..]).parse::<i32>() {
            Ok(n) => n,
            Err(_) => {
                return Err(Error::new("invalid multibulk length"));
            }
        };
        let mut arr = Vec::new();
        for _ in 0..n {
            let line = match self.pl_read_line()? {
                Some(line) => line,
                None => return Ok(None),
            };
            if line.len() == 0 {
                return Err(Error::new("expected '$', got ' '"));
            }
            if line[0] != b'$' {
                return Err(Error::new(&format!(
                    "expected '$', got '{}'",
                    if line[0] < 20 || line[0] > b'~' {
                        ' '
                    } else {
                        line[0] as char
                    },
                )));
            }
            let n = match String::from_utf8_lossy(&line[1..]).parse::<i32>() {
                Ok(n) => n,
                Err(_) => -1,
            };
            if n < 0 || n > 536870912 {
                // Spec limits the number of bytes in a bulk.
                // https://redis.io/docs/reference/protocol-spec
                return Err(Error::new("invalid bulk length"));
            }
            let mut buf = vec![0u8; n as usize];
            self.reader.read_exact(&mut buf)?;
            let mut crnl = [0u8; 2];
            self.reader.read_exact(&mut crnl)?;
            // Actual redis ignores the last two characters even though
            // they should be looking for '\r\n'.
            arr.push(buf);
        }
        Ok(Some(arr))
    }
    fn pl_read_line(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let mut line = Vec::new();
        let size = self.reader.read_until(b'\n', &mut line)?;
        if size == 0 {
            return Ok(None);
        }
        if line.len() > 1 && line[line.len() - 2] == b'\r' {
            line.truncate(line.len() - 2);
        } else {
            line.truncate(line.len() - 1);
        }
        Ok(Some(line))
    }
    fn pl_read_inline(&mut self, line: Vec<u8>) -> Result<Option<Vec<Vec<u8>>>, Error> {
        const UNBALANCED: &str = "unbalanced quotes in request";
        let mut arr = Vec::new();
        let mut arg = Vec::new();
        let mut i = 0;
        loop {
            if i >= line.len() || line[i] == b' ' || line[i] == b'\t' {
                if arg.len() > 0 {
                    arr.push(arg);
                    arg = Vec::new();
                }
                if i >= line.len() {
                    break;
                }
            } else if line[i] == b'\'' || line[i] == b'\"' {
                let quote = line[i];
                i += 1;
                loop {
                    if i == line.len() {
                        return Err(Error::new(UNBALANCED));
                    }
                    if line[i] == quote {
                        i += 1;
                        break;
                    }
                    if line[i] == b'\\' && quote == b'"' {
                        if i == line.len() - 1 {
                            return Err(Error::new(UNBALANCED));
                        }
                        i += 1;
                        match line[i] {
                            b't' => arg.push(b'\t'),
                            b'n' => arg.push(b'\n'),
                            b'r' => arg.push(b'\r'),
                            b'b' => arg.push(8),
                            b'v' => arg.push(11),
                            b'x' => {
                                if line.len() < 3 {
                                    return Err(Error::new(UNBALANCED));
                                }
                                let hline = &line[i + 1..i + 3];
                                let hex = String::from_utf8_lossy(hline);
                                match u8::from_str_radix(&hex, 16) {
                                    Ok(b) => arg.push(b),
                                    Err(_) => arg.extend(&line[i..i + 3]),
                                }
                                i += 2;
                            }
                            _ => arg.push(line[i]),
                        }
                    } else {
                        arg.push(line[i]);
                    }
                    i += 1;
                }
                if i < line.len() && line[i] != b' ' && line[i] != b'\t' {
                    return Err(Error::new(UNBALANCED));
                }
            } else {
                arg.push(line[i]);
            }
            i += 1;
        }
        Ok(Some(arr))
    }

    // Read a pipeline of commands.
    // Each command will *always* have at least one argument.
    fn read_pipeline(&mut self) -> Result<Vec<Vec<Vec<u8>>>, Error> {
        let mut cmds = Vec::new();
        loop {
            // read line
            let line = match self.pl_read_line()? {
                Some(line) => line,
                None => {
                    self.closed = true;
                    break;
                }
            };
            if line.len() == 0 {
                // empty lines are ignored.
                continue;
            }
            let args = if line[0] == b'*' {
                // read RESP array
                self.pl_read_array(line)?
            } else {
                // read inline array
                self.pl_read_inline(line)?
            };
            let args = match args {
                Some(args) => args,
                None => {
                    self.closed = true;
                    break;
                }
            };
            if args.len() > 0 {
                cmds.push(args);
            }
            if cmds.len() > 0 && self.reader.buffer().len() == 0 {
                break;
            }
        }
        Ok(cmds)
    }

    fn extend_lossy_line(&mut self, prefix: u8, msg: &str) {
        self.wbuf.push(prefix);
        for b in msg.bytes() {
            self.wbuf.push(if b < b' ' { b' ' } else { b })
        }
        self.wbuf.push(b'\r');
        self.wbuf.push(b'\n');
    }

    fn flush(&mut self) -> Result<(), Error> {
        if self.wbuf.len() > 0 {
            self.writer.write_all(&self.wbuf)?;
            if self.wbuf.len() > 1048576 {
                self.wbuf = Vec::new();
            } else {
                self.wbuf.truncate(0);
            }
        }
        Ok(())
    }
}

pub struct Server<T> {
    listener: Option<TcpListener>,
    data: Option<T>,
    local_addr: SocketAddr,

    /// Handle incoming RESP commands.
    pub command: Option<fn(&mut Conn, &T, Vec<Vec<u8>>)>,

    /// Handle incoming connections.
    pub opened: Option<fn(&mut Conn, &T)>,

    /// Handle closed connections. 
    /// 
    /// If the connection was closed due to an error then that error is
    /// provided.
    pub closed: Option<fn(&mut Conn, &T, Option<Error>)>,
    
    /// Handle ticks at intervals as defined by the returned [`Duration`].
    /// 
    /// The next tick will happen following the elapsed returned `Duration`.
    /// 
    /// Returning `None` will shutdown the server.
    pub tick: Option<fn(&T) -> Option<Duration>>,
}


/// Creates a new `Server` which will be listening for incoming connections on
/// the specified address using the provided `data`.
///
/// The returned server is ready for serving.
///
/// # Examples
///
/// Creates a Redcon server listening at `127.0.0.1:6379`:
///
/// ```no_run
/// let my_data = "hello";
/// let server = redcon::listen("127.0.0.1:6379", my_data).unwrap();
/// ```
pub fn listen<A: ToSocketAddrs, T>(addr: A, data: T) -> Result<Server<T>, Error> {
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;
    let svr = Server {
        data: Some(data),
        listener: Some(listener),
        local_addr: local_addr,
        command: None,
        opened: None,
        closed: None,
        tick: None,
    };
    Ok(svr)
}

impl<T: Send + Sync + 'static> Server<T> {
    pub fn serve(&mut self) -> Result<(), Error> {
        serve(self)
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

fn serve<T: Send + Sync + 'static>(s: &mut Server<T>) -> Result<(), Error> {
    // Take all of the server fields at once.
    let listener = match s.listener.take() {
        Some(listener) => listener,
        None => return Err(Error::IoError(io::Error::from(io::ErrorKind::Other))),
    };
    let data = s.data.take().unwrap();
    let command = s.command.take();
    let opened = s.opened.take();
    let closed = s.closed.take();
    let tick = s.tick.take();
    let laddr = s.local_addr;
    drop(s);

    let conns: HashMap<u64, Arc<AtomicBool>> = HashMap::new();
    let conns = Arc::new(Mutex::new(conns));
    let data = Arc::new(data);
    let mut next_id: u64 = 1;
    let shutdown = Arc::new(AtomicBool::new(false));
    let init_shutdown = |shutdown: Arc<AtomicBool>, laddr: &SocketAddr| {
        let aord = Ordering::SeqCst;
        if shutdown.compare_exchange(false, true, aord, aord).is_err() {
            // Shutdown has already been initiated.
            return;
        }
        // Connect to self to force initiate a shutdown.
        let _ = TcpStream::connect(&laddr);
    };
    let mut threads = Vec::new();
    if let Some(tick) = tick {
        let data = data.clone();
        let shutdown = shutdown.clone();
        threads.push(thread::spawn(move || {
            while !shutdown.load(Ordering::SeqCst) {
                match (tick)(&data) {
                    Some(delay) => thread::sleep(delay),
                    None => init_shutdown(shutdown.clone(), &laddr),
                }
            }
        }));
    }
    for stream in listener.incoming() {
        let shutdown = shutdown.clone();
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        match stream {
            Ok(stream) => {
                if stream
                    .set_read_timeout(Some(Duration::from_millis(100)))
                    .is_err()
                {
                    continue;
                }
                let addr = match stream.peer_addr() {
                    Ok(addr) => addr,
                    _ => continue,
                };
                // create two streams (input, output)
                let streams = (
                    match stream.try_clone() {
                        Ok(stream) => stream,
                        _ => continue,
                    },
                    stream,
                );
                let data = data.clone();
                let conn_id = next_id;
                next_id += 1;
                let xcloser = Arc::new(AtomicBool::new(false));
                let conns = conns.clone();
                conns.lock().unwrap().insert(conn_id, xcloser.clone());
                threads.push(thread::spawn(move || {
                    let mut conn = Conn {
                        id: conn_id,
                        cmds: Vec::new(),
                        context: None,
                        addr,
                        reader: BufReader::new(Box::new(streams.0)),
                        wbuf: Vec::new(),
                        writer: Box::new(streams.1),
                        closed: false,
                        shutdown: false,
                        conns: conns.clone(),
                    };
                    let mut final_err: Option<Error> = None;
                    if let Some(opened) = opened {
                        (opened)(&mut conn, &data);
                    }
                    loop {
                        if let Err(e) = conn.flush() {
                            if final_err.is_none() {
                                final_err = Some(From::from(e));
                            }
                            conn.closed = true;
                        }
                        if conn.closed {
                            break;
                        }
                        match conn.read_pipeline() {
                            Ok(cmds) => {
                                conn.cmds = cmds;
                                conn.cmds.reverse();
                                while let Some(cmd) = conn.next_command() {
                                    if let Some(command) = command {
                                        (command)(&mut conn, &data, cmd);
                                    }
                                    if conn.closed {
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                if let Error::Protocol(msg) = &e {
                                    // Write the protocol error to the
                                    // client before closing the connection.
                                    conn.write_error(&format!("ERR Protocol error: {}", msg));
                                } else if let Error::IoError(e) = &e {
                                    if let io::ErrorKind::WouldBlock = e.kind() {
                                        // Look to see if there is a pending
                                        // server shutdown or a cross close
                                        // request.
                                        if shutdown.load(Ordering::SeqCst) {
                                            conn.closed = true;
                                        }
                                        if xcloser.load(Ordering::SeqCst) {
                                            conn.closed = true;
                                        }
                                        continue;
                                    }
                                }
                                final_err = Some(e);
                                conn.closed = true;
                            }
                        }
                    }
                    if conn.shutdown {
                        init_shutdown(shutdown.clone(), &laddr);
                    }
                    if let Some(closed) = closed {
                        (closed)(&mut conn, &data, final_err);
                    }
                    conns.lock().unwrap().remove(&conn.id);
                }));
            }
            Err(_) => {}
        }
    }
    // Wait for all connections to complete and for their threads to terminate.
    for thread in threads {
        thread.join().unwrap();
    }
    Ok(())
}
