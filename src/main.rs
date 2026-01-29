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
    sender: Sender<String>,
    receiver: Receiver<String>,
}

struct Client {
    stream: Arc<Mutex<TcpStream>>,
}
impl Client {
    fn new(stream: TcpStream) -> Self {
        let s = Arc::new(Mutex::new(stream));
        Self {
            stream: s,
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
            sender: tx, 
            receiver: rx,
        }
    }
    fn add(&mut self, stream: TcpStream) {
        self.clients.push(Client::new(stream));
    }

    fn send(&self, msg: impl Into<String>) {
        self.sender.send(msg.into());
    }
}
fn server() -> std::io::Result<()> {
    let server = Server::new();
    let mut server = Arc::new(Mutex::new(server));
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    let mut id = 0usize;
    thread::spawn(||write(rx, server));
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                //let client = Arc::new(Mutex::new(Client::new(stream)));
                println!("someone connected!");
                let s = server.lock();
                s.unwrap().clients.push(Client::new(stream));
                let server = Arc::clone(&server);
                let server2 = Arc::clone(&server);

                let _ = thread::spawn(move || handle_client_read(id,Arc::clone(&server)));

                let _ = thread::spawn(move || handle_client_write(id,Arc::clone(&server2)));
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

fn write(rx: Receiver<String>, server: &Server) {
    let a = rx.recv();
    match a {
        Ok(val) => {
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

fn run_client(client: Client, server: Arc<Mutex<Server>>) {
    let server = server.lock().unwrap();
    let tx = &server.sender;
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
                let a = tx.send(string_thing.to_string());
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
        let tx = &server.sender;
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

fn handle_client_write(client_id: usize, server: Arc<Mutex<Server>>) {

    let server = server.lock().unwrap();
    let client = &server.clients[client_id];
    let a = &server.receiver;
    let mut stream = client.stream.lock().unwrap();

    let r = a.recv();
    match r {
        Ok(val) => {
            let a = val;
            stream.write(&a.as_bytes()).unwrap();
        }
        Err(err) => {
            println!("{}", err);
        }
    }
}
