
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
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
