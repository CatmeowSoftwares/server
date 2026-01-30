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
    let threat = thread::spawn(||server());
    threat.join();
    Ok(())
}

struct Server {
    clients: Vec<Client>,
}

struct Client {
    stream: Arc<Mutex<TcpStream>>,
}
impl Client {
    fn new(stream: Arc<Mutex<TcpStream>>) -> Self {
        Self {
            stream: stream,
        }
    }
    fn run() {

    }
}
impl Server {
    fn new() -> Self{
        let (tx, rx) = mpsc::channel::<String>();
        Self {
            clients: Vec::new(),
        }
    }
}
fn server() -> std::io::Result<()> {
    let server = Server::new();
    let mut server = Arc::new(Mutex::new(server));
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    let mut id = 0usize;
    let (sender, receiver) = mpsc::channel();
    let cloned_server = Arc::clone(&server);
    thread::spawn(move ||write(&receiver, &cloned_server));
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let cloned_server_again = Arc::clone(&server);
                let not_mutable_server = cloned_server_again.lock().unwrap();
                let mut server = not_mutable_server;
                let arc_stream = Arc::new(Mutex::new(stream));
                let cloned_arc_stream = Arc::clone(&arc_stream);
                server.clients.push(Client::new(arc_stream));
                //let client = Arc::new(Mutex::new(Client::new(stream)));
                println!("someone connected!");
                let sender = sender.clone();
                let _ = thread::spawn(move ||{
                    let mut stream = cloned_arc_stream.lock().unwrap();
                    let mut read = [0u8; 1024];
                    match stream.read(&mut read) {
                        Ok(val) => {
                            let bytes = read.bytes();
                            //stream.write(&read[0..val]);
                            let string_thing = String::from_utf8_lossy(&read);
                            let string = string_thing.to_string();
                            println!("{}", string);
                            let res = sender.send(string);
                            match res {
                                Ok(val) => {
                                    println!("success!!!!");
                                }
                                Err(err) => {
                                    println!("there was an error: {}", err);
                                }
                            }

                        },
                        Err(err) => {
                            
                        }
                    }
                });
                id += 1;
                //handle_client(stream);
            }
            Err(e) => {
                println!("{}", e);
            }
        }
    }

    Ok(())
}

fn write(rx: &Receiver<String>, server: &Arc<Mutex<Server>>) {
    loop {

        let a = rx.recv();
        match a {
            Ok(val) => {
                let server = server.lock().unwrap();
                println!("works!: {val}");
                for client in &server.clients {
                    let mut stream = client.stream.lock().unwrap();
                    let b = stream.write(val.as_bytes());
                }
            },
            Err(err) => {
                println!("{err}");
            }
        }
    }

}

fn run_client(client: Client, server: Arc<Mutex<Server>>) {
    let server = server.lock().unwrap();
    let stream = Arc::clone(&client.stream);
    let mut stream = stream.lock().unwrap();
    // read 20 bytes at a time from stream echoing back to stream
    let mut id: u32 = 0;
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
                /*
                match a {
                    Ok(val) => {}
                    Err(err) => {
                        println!("error! {}", err);
                    }
                } */
                id += 1;

                println!(r#"{}"#, string_thing);
            }
            Err(err) => {
                println!("there was an error!");
                panic!("{}", err);
            }
        }
    }
}


fn run(server: &Server) {
}


fn handle_client_read(client_id: usize, server: Arc<Mutex<Server>>) {
    // read 20 bytes at a time from stream echoing back to stream
    
    loop {
        let mut read = [0; 1028];
        let server = server.lock().unwrap();
        let client = &server.clients[client_id];
        let mut stream = client.stream.lock().unwrap();
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

                println!(r#"{}"#, string_thing);
            }
            Err(err) => {
                println!("there was an error!");
                panic!("{}", err);
            }
        }
    }
}

