
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    // Shared storage for strings and their expiration times
    let database = Arc::new(Mutex::new(
        HashMap::<String, (String, Option<Instant>)>::new()
    ));

    // Shared storage for lists
    let lists = Arc::new(Mutex::new(
        HashMap::<String, Vec<String>>::new()
    ));

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let database = Arc::clone(&database);
                let lists = Arc::clone(&lists);

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

                            let expiry = if parts.len() >= 11
                                && parts[8].eq_ignore_ascii_case("PX")
                            {
                                let milliseconds = parts[10].parse::<u64>().unwrap();
                                Some(Instant::now() + Duration::from_millis(milliseconds))
                            } else {
                                None
                            };

                            database.lock().unwrap().insert(key, (value, expiry));

                            stream.write_all(b"+OK\r\n").unwrap();

                        } else if parts.len() >= 5 && parts[2].eq_ignore_ascii_case("GET") {
                            let key = parts[4];
                            let mut db = database.lock().unwrap();

                            let expired = db.get(key).is_some_and(|(_, expiry)| {
                                expiry.is_some_and(|deadline| Instant::now() >= deadline)
                            });

                            if expired {
                                db.remove(key);
                            }

                            match db.get(key) {
                                Some((value, _)) => {
                                    let response = format!("${}\r\n{}\r\n", value.len(), value);
                                    stream.write_all(response.as_bytes()).unwrap();
                                }
                                None => {
                                    stream.write_all(b"$-1\r\n").unwrap();
                                }
                            }

                        } else if parts.len() >= 5 && parts[2].eq_ignore_ascii_case("RPUSH") {
                            let key = parts[4].to_string();

                            let mut db = lists.lock().unwrap();
                            let list = db.entry(key).or_insert_with(Vec::new);

                            // Add each provided element to the list
                            for i in (6..parts.len()).step_by(2) {
                                if !parts[i].is_empty() {
                                    list.push(parts[i].to_string());
                                }
                            }

                            let response = format!(":{}\r\n", list.len());
                            stream.write_all(response.as_bytes()).unwrap();
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
