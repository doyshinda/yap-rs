use env_logger;
use log::debug;
use std::{
    io::{Error, ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread::{sleep, spawn},
    time::Duration,
};

fn string_addr(addr: Result<SocketAddr, Error>) -> String {
    match addr {
        Ok(ref a) => format!("{}:{}", a.ip(), a.port()),
        Err(e) => format!("unknown: {:?}", e),
    }
}

fn handle_read(stream: &mut TcpStream, buf: &mut Vec<u8>) -> usize {
    match stream.read(buf) {
        Ok(n) => {
            if n > 0 {
                debug!(
                    "[{:>21}] <--- [{:<21}] recv {} bytes",
                    string_addr(stream.local_addr()),
                    string_addr(stream.peer_addr()),
                    n,
                );
            }
            n
        }
        Err(e) => {
            if e.kind() == ErrorKind::WouldBlock {
                0
            } else {
                panic!("read error: {:?}", e);
            }
        }
    }
}

fn handle_write(stream: &mut TcpStream, buf: &mut [u8]) {
    match stream.write(buf) {
        Ok(_) => {
            debug!(
                "[{:>21}] ---> [{:<21}] sent {} bytes",
                string_addr(stream.local_addr()),
                string_addr(stream.peer_addr()),
                buf.len(),
            );
        }
        Err(e) => panic!("write error: {:?}", e),
    };
}

fn downstream_receiver(
    mut downstream: TcpStream,
    mut upstream: TcpStream,
    buf_len: usize,
) {
    downstream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut buf = Vec::with_capacity(buf_len);
    buf.resize(buf_len, 0);

    loop {
        let last_read = handle_read(&mut downstream, &mut buf);

        if last_read > 0 {
            handle_write(&mut upstream, &mut buf[..last_read]);
        } else {
            break;
        }
    }
}

fn handle_connection(
    mut upstream: TcpStream,
    addr: &str,
    num_conn: Arc<AtomicUsize>,
    buf_len: usize,
) {
    num_conn.fetch_add(1, Ordering::Relaxed);
    debug!("connecting to downstream {}", addr);
    let mut downstream = TcpStream::connect(addr).unwrap();
    let d_clone = downstream.try_clone().unwrap();
    let u_clone = upstream.try_clone().unwrap();
    spawn(move || downstream_receiver(d_clone, u_clone, buf_len));

    let mut buf = Vec::with_capacity(buf_len);
    buf.resize(buf_len, 0);

    upstream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    downstream
        .set_write_timeout(Some(Duration::from_millis(10)))
        .unwrap();

    loop {
        let bytes_recv = handle_read(&mut upstream, &mut buf);

        if bytes_recv > 0 {
            handle_write(&mut downstream, &mut buf[..bytes_recv]);
        } else {
            break;
        }
    }
    debug!("connection to {} closed", addr);
    num_conn.fetch_sub(1, Ordering::Relaxed);
}

pub fn run() {
    env_logger::init();
    let port = 12345;
    let listener = TcpListener::bind(format!("0.0.0.0:{}", &port)).unwrap();
    let num_conn = Arc::new(AtomicUsize::new(0));
    let max_conn = 1000;
    let buf_size = 1024 * 4; // 16K
    // let downstream = "127.0.0.1:8000";
    let downstream = vec!["192.168.0.101:8000", "192.168.0.101:8001", "192.168.0.101:8002"];
    loop {
        if num_conn.load(Ordering::Relaxed) < max_conn {
            let n = num_conn.load(Ordering::Relaxed);
            let idx = n % downstream.len();
            let addr = downstream[idx];
            let accepted = listener.accept();
            match accepted {
                Ok((s, a)) => {
                    debug!("new connection from: {}", string_addr(Ok(a)));
                    let n_conn = num_conn.clone();
                    spawn(move || handle_connection(s, &addr, n_conn, buf_size));
                }
                Err(e) => println!("no stream: {:?}", e),
            }
        } else {
            sleep(Duration::from_millis(5));
        }
    }
}

fn main() {
    run();
}
