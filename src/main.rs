
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    // Shared database for all clients
    let database = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let database = Arc::clone(&database);

                thread::spawn(move || {
                    let mut buf = [0; 512];

                    loop {
                        let bytes_read = stream.read(&mut buf).unwrap();

                        if bytes_read == 0 {
                            break;
                        }

                        let message = String::from_utf8_lossy(&buf[..bytes_read]);
                        let parts: Vec<&str> = message.split("\r\n").collect();

                        if parts.len() >= 4 && parts[2].eq_ignore_ascii_case("PING") {
                            stream.write_all(b"+PONG\r\n").unwrap();
                        } else if parts.len() >= 5 && parts[2].eq_ignore_ascii_case("ECHO") {
                            let argument = parts[4];
                            let response = format!("${}\r\n{}\r\n", argument.len(), argument);
                            stream.write_all(response.as_bytes()).unwrap();
                        } else if parts.len() >= 7 && parts[2].eq_ignore_ascii_case("SET") {
                            let key = parts[4].to_string();
                            let value = parts[6].to_string();

                            database.lock().unwrap().insert(key, value);
                            stream.write_all(b"+OK\r\n").unwrap();
                        } else if parts.len() >= 5 && parts[2].eq_ignore_ascii_case("GET") {
                            let key = parts[4];
                            let db = database.lock().unwrap();

                            match db.get(key) {
                                Some(value) => {
                                    let response = format!("${}\r\n{}\r\n", value.len(), value);
                                    stream.write_all(response.as_bytes()).unwrap();
                                }
                                None => {
                                    stream.write_all(b"$-1\r\n").unwrap();
                                }
                            }
                        }
                    }
                });
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }
}
