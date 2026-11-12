
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    // Shared storage for strings and optional expiration times
    let database = Arc::new(Mutex::new(
        HashMap::<String, (String, Option<Instant>)>::new()
    ));

    // Shared storage for Redis lists
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
                        let bytes_read = match stream.read(&mut buf) {
                            Ok(0) => break,
                            Ok(n) => n,
                            Err(_) => break,
                        };

                        let message = String::from_utf8_lossy(&buf[..bytes_read]);
                        let parts: Vec<&str> = message.split("\r\n").collect();

                        // PING
                        if parts.len() >= 4 && parts[2].eq_ignore_ascii_case("PING") {
                            stream.write_all(b"+PONG\r\n").unwrap();

                        // ECHO
                        } else if parts.len() >= 5 && parts[2].eq_ignore_ascii_case("ECHO") {
                            let argument = parts[4];
                            let response = format!("${}\r\n{}\r\n", argument.len(), argument);
                            stream.write_all(response.as_bytes()).unwrap();

                        // SET
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

                        // GET
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

                        // RPUSH
                        } else if parts.len() >= 5 && parts[2].eq_ignore_ascii_case("RPUSH") {
                            let key = parts[4].to_string();

                            let mut db = lists.lock().unwrap();
                            let list = db.entry(key).or_insert_with(Vec::new);

                            for i in (6..parts.len()).step_by(2) {
                                if !parts[i].is_empty() {
                                    list.push(parts[i].to_string());
                                }
                            }

                            let response = format!(":{}\r\n", list.len());
                            stream.write_all(response.as_bytes()).unwrap();

                        // LRANGE (positive and negative indexes)
                        } else if parts.len() >= 9 && parts[2].eq_ignore_ascii_case("LRANGE") {
                            let key = parts[4];
                            let start = parts[6].parse::<i64>().unwrap();
                            let stop = parts[8].parse::<i64>().unwrap();

                            let db = lists.lock().unwrap();

                            if let Some(list) = db.get(key) {
                                let len = list.len() as i64;

                                // Convert negative indexes to positive positions
                                let start = if start < 0 { len + start } else { start };
                                let stop = if stop < 0 { len + stop } else { stop };

                                // Keep indexes inside valid boundaries
                                let start = start.max(0).min(len);
                                let stop = stop.min(len - 1);

                                let elements: Vec<&String> = if start <= stop {
                                    list[start as usize..=stop as usize].iter().collect()
                                } else {
                                    Vec::new()
                                };

                                // RESP array response
                                let mut response = format!("*{}\r\n", elements.len());

                                for element in elements {
                                    response.push_str(&format!(
                                        "${}\r\n{}\r\n",
                                        element.len(),
                                        element
                                    ));
                                }

                                stream.write_all(response.as_bytes()).unwrap();
                            } else {
                                stream.write_all(b"*0\r\n").unwrap();
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
