use std::{f32::consts::PI, io::Error, os::windows::process, sync::Arc, time::Duration};

use num_enum::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{self, TcpListener, TcpSocket, TcpStream, UdpSocket},
    sync::{
        Mutex,
        broadcast::{self, Receiver, Sender},
    },
    time::sleep,
};

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum RequestType {
    ChatSend = 0,
    ChatRemove,
    GetPlayerPos,
    SetPlayerPos,
    GetPlayers,
    PlayerJoined,
    PlayerLeft,
    PlayerDisconnected,
    PlayerLogin,
    PlayerRegister,
}

struct Player {
    position: [f32; 2],
    username: String,
    id: u8,
}

impl Player {
    fn new(username: impl Into<String>) -> Self {
        let id = 0;
        Self {
            position: [0.0, 0.0],
            username: username.into(),
            id: id,
        }
    }
}
#[tokio::main]
async fn main() -> Result<(), Error> {
    let a = TcpListener::bind("127.0.0.1:8080").await?;
    let a2 = Arc::new(Mutex::new(UdpSocket::bind("127.0.0.1:8080").await?));
    let (tx, _) = broadcast::channel::<String>(16);
    let sock = Arc::clone(&a2);
    let mut players: Vec<Player> = Vec::new();

    loop {
        let (mut stream, addr) = a.accept().await?;

        let tx = tx.clone();
        let mut rx = tx.subscribe();
        tokio::spawn(async move {
            println!("Welcome!");
            let (mut reader, mut writer) = stream.split();
            let text = String::new();
            loop {
                let mut r: Vec<u8> = vec![0; 1024];
                tokio::select! {
                    result = reader.read(&mut r) => {
                        match result {
                            Ok(n) => {
                                if n == 0 {
                                    println!("bye!");
                                    break;
                                }
                                match RequestType::try_from(r[0]).unwrap() {
                                    RequestType::ChatSend=> {
                                        let t = String::from_utf8(r).unwrap();
                                        tx.send(t).unwrap();
                                    },
                                    RequestType::PlayerLogin=>todo!(),
                                    RequestType::ChatRemove=>todo!(),
                                    RequestType::GetPlayerPos=> {
                                        
                                    },
                                    RequestType::SetPlayerPos=>todo!(),
                                    RequestType::PlayerJoined=>todo!(),
                                    RequestType::PlayerLeft=>todo!(),
                                    RequestType::PlayerDisconnected=>todo!(),
                                    RequestType::GetPlayers=>{}
                                    RequestType::PlayerRegister => todo!(),
                                }
                            },
                            Err(_) => ()
                        }
                    }
                    result = rx.recv() => {
                        let msg = result.unwrap();
                        println!("bbb{msg}");
                        writer.write_all(&msg.as_bytes()).await;
                    }
                    result = sleep(Duration::from_mins(20)) => {
                        println!("no person, bye!");
                        break;
                    }



                }
            }
        });

        let sock = Arc::clone(&sock);
        tokio::spawn(async move {
            println!("someone connected!");
            loop {
                let sock = Arc::clone(&sock);
                let mut v = vec![0u8; 1024];
                let bytes = sock.lock().await.recv(&mut v).await.unwrap();
                if bytes == 0 {
                    println!("disconected!!!!!");
                    break;
                }
                let data = &v[..bytes];
                let str_data = String::from_utf8_lossy(&data).to_string();
                println!("{:?}", str_data);
            }
        });
    }
    Ok(())
}
