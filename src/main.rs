use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, Sender},
    },
    thread,
    time::Duration,
};

fn main() -> std::io::Result<()> {
    /*
    let f1 = || { loop {println!("hi"); sleep(Duration::from_secs(1)); } ()};
    let f2 = || { loop {println!("hello"); sleep(Duration::from_secs(2));} ()};
    let f3 = || {
         loop {
            let mut input = String::new();
            io::stdin().read_line(&mut input).expect("some error");

            println!("{}", input);
            sleep(Duration::from_secs(2));
        }
        ()
    };

    let a= spawn(f1);
    let b = spawn(f2);
    let c = spawn(f3);
    a.join().unwrap();
    b.join().unwrap();
    c.join().unwrap();
    return Ok(()); */
    let (tx, rx) = mpsc::channel();
    let rx2 = Arc::new(Mutex::new(rx));
    server(tx, rx2);

    Ok(())
}

fn server(tx: Sender<String>, rx: Arc<Mutex<Receiver<String>>>) -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    tx.send("test".to_string());
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("someone connected!");
                let tx = tx.clone();
                let rx = Arc::clone(&rx);
                let stream2 = stream.try_clone().unwrap();
                let s = thread::spawn(move || handle_client_read(&stream2, &tx));
                let t = thread::spawn(move || handle_client_write(&stream.try_clone().unwrap(), rx));

                //handle_client(stream);
            }
            Err(e) => {
                println!("{}", e);
            }
        }
    }

    Ok(())
}

fn handle_client_read(mut stream: &TcpStream, tx: &Sender<String>) {
    // read 20 bytes at a time from stream echoing back to stream
    loop {
        let mut read = [0; 1028];
        match stream.read(&mut read) {
            Ok(n) => {
                if n == 0 {
                    // connection was closed
                    // idk
                    println!("disconnected!");
                    return;
                }
                stream.write(&read[0..n]).unwrap();
                let string_thing = String::from_utf8_lossy(&read);
                let a = tx.send(string_thing.to_string());
                match a {
                    Ok(val) => {}
                    Err(err) => {
                        println!("error! {}", err);
                    }
                }

                println!(r#"{}"#, string_thing);
            }
            Err(err) => {
                println!("there was an error!");
                panic!("{}", err);
            }
        }
    }
}


fn handle_client_write(mut stream: &TcpStream, rx: Arc<Mutex<Receiver<String>>>) {
    let r = rx.lock();
    match r {
        Ok(val) => {
            let val2 = val.recv();
            match val2 {
                Ok(val) => {
                    stream.write(&val.as_bytes()).unwrap();
                },
                Err(err) => {
                    println!("{}", err);
                }
            }
        },
        Err(err) => {
            println!("{}", err);
        }
    }
}