use std::{f32::consts::PI, io::Error, os::windows::process};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{self, TcpListener, TcpSocket, TcpStream, UdpSocket}, sync::broadcast::{self, Receiver, Sender}};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let a = TcpListener::bind("127.0.0.1:8080").await?;
    let a2 = UdpSocket::bind("127.0.0.1:8080").await?;
    let (tx, _) = broadcast::channel::<String>(16);
    loop {
        let mut rx2 = tx.subscribe();
        

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
                                let t = String::from_utf8(r).unwrap();
                                println!("aaa{t}");
                                tx.send(t).unwrap();
                            },
                            Err(_) => ()
                        }
                    }
                    result = rx.recv() => {
                        let msg = result.unwrap();
                        println!("bbb{msg}");
                        writer.write_all(&msg.as_bytes()).await;
                    }



                }
        
            }
            



        });


        tokio::spawn(async move {



            loop {
                let mut v = vec![0u8; 1024];
                tokio::select! {
                    result = a2.recv_from(&mut v) => {
                        match result {
                            Ok(ok) => {
                                let msg = result.unwrap();
                                a2.send_to(&msg.as_bytes(), addr).await;
                            },
                            Err(err) => {

                            }
                        }
                    }
                    result = a2.send_to(&v[0..1024]) => {

                    }
                }
            }
        });
    }
    Ok(())
}


