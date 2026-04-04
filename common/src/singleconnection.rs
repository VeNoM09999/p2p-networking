use super::{Header, Packet, PacketType, SentPacket};
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, Receiver, UnboundedReceiver};

pub struct AckInfo {
    ack_num: u32,
    peer_addr: SocketAddr,
}
pub struct ReliableOneWayUDP {
    pub socket: Arc<UdpSocket>,
    state: Arc<SharedState>,
    pub output_tx: mpsc::Sender<(Packet, SocketAddr)>,
    ack_tx: mpsc::UnboundedSender<AckInfo>,
}
impl ReliableOneWayUDP {
    pub fn new(
        socket: Arc<UdpSocket>,
    ) -> (
        Self,
        ReliableOneWayUDPHandle,
        Receiver<(Packet, SocketAddr)>,
        UnboundedReceiver<AckInfo>,
    ) {
        let (output_tx, output_rx) = mpsc::channel::<(Packet, SocketAddr)>(100);
        let (ack_tx, ack_rx) = mpsc::unbounded_channel();
        let state = SharedState::new();

        let receiver = Self {
            socket: socket.clone(),
            output_tx,
            ack_tx,
            state: state.clone(),
        };

        let handle = ReliableOneWayUDPHandle {
            socket: socket,
            state: state,
        };
        (receiver, handle, output_rx, ack_rx)
    }
    pub async fn recv(&mut self, peer_addr: std::net::SocketAddr) {
        let mut buf = [0u8; 6600];
        loop {
            if let Ok((bytes_read, addr)) = self.socket.recv_from(&mut buf).await {
                if addr != peer_addr {
                    // trace!("Ignoring packet from non-peer: {}", addr);
                    continue;
                }
                match deserialize(&buf[..bytes_read]) {
                    Err(_) => {}
                    Ok(packet) => {
                        let packet_seq = packet.header.seq;
                        let packet_ack = packet.header.ack;

                        let expected = &self.state.expected_sequence.load(Acquire);
                        // Matching seq for incoming packets
                        match packet_seq.cmp(expected) {
                            std::cmp::Ordering::Equal => {
                                self.output_tx.send((packet.clone(), addr)).await;

                                // Update Expected sequence
                                self.state.expected_sequence.fetch_add(1, Release);
                                self.state.receiver_acknowledged.store(packet_seq, Release);

                                // Notifying sender about ack
                                let _ = self.ack_tx.send(AckInfo {
                                    ack_num: packet_seq,
                                    peer_addr: addr,
                                });

                                let mut next_expected = packet_seq + 1;
                                // Checking next packet in buffer
                                while let Some((_, p)) =
                                    self.state.recv_buffer.remove(&next_expected)
                                {
                                    // Update ACK & Expected Seq & Send packet down the stream
                                    self.state.receiver_acknowledged.fetch_add(1, Release);
                                    self.state.expected_sequence.fetch_add(1, Release);
                                    let _ = self.output_tx.send((p, addr)).await;

                                    next_expected += 1;
                                }
                            }
                            std::cmp::Ordering::Greater => {
                                self.state
                                    .recv_buffer
                                    .insert(packet.header.seq, packet.clone());
                                let ack_packet = Packet {
                                    header: Header {
                                        seq: 0,
                                        ack: self.state.receiver_acknowledged.load(Acquire),
                                    },
                                    payload: None,
                                };
                                // Send Instant Ack
                                let _ = self.socket.send_to(&serialize(&ack_packet), addr).await;
                            }
                            std::cmp::Ordering::Less => {
                                // duplicate -- ignore
                                if packet.payload.is_none()
                                    || packet
                                        .payload
                                        .as_ref()
                                        .map(|p| p.is_empty())
                                        .unwrap_or(false)
                                {
                                    let _ = self.ack_tx.send(AckInfo {
                                        ack_num: packet_ack,
                                        peer_addr: addr,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

struct SharedState {
    // Receiver State
    expected_sequence: AtomicU32,
    receiver_acknowledged: AtomicU32, // -! For send acks to sender
    recv_buffer: DashMap<u32, Packet>,

    // SenderState
    next_seq: AtomicU32,
    sender_acknowdgeded: AtomicU32, // -! For Handling unacked packets
    unacked: DashMap<u32, SentPacket>,
}
impl SharedState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            expected_sequence: AtomicU32::new(0),
            receiver_acknowledged: AtomicU32::new(0),
            recv_buffer: DashMap::new(),
            next_seq: AtomicU32::new(0),
            sender_acknowdgeded: AtomicU32::new(0),
            unacked: DashMap::new(),
        })
    }
}

#[derive(Clone)]
pub struct ReliableOneWayUDPHandle {
    pub socket: Arc<UdpSocket>,
    state: Arc<SharedState>,
}

impl ReliableOneWayUDPHandle {
    pub async fn send(&mut self, addr: std::net::SocketAddr, mut rx: mpsc::Receiver<PacketType>) {
        let mut retransmit_interval = tokio::time::interval(Duration::from_millis(200));

        loop {
            tokio::select! {
                Some(packet_type) = rx.recv()=> {
                    match packet_type {
                        PacketType::Ack { ack } => {
                            eprintln!("GOT ACK ON SENDER");
                            // let ack_packet = Packet {
                            //     header: Header {
                            //         seq: 0,
                            //         ack: ack,
                            //     },
                            //     payload: None,
                            // };
                            // let _ = self
                            //     .socket
                            //     .send_to(&serialize(&ack_packet),addr)
                            //     .await;
                        }
                        PacketType::Data { payload } => {
                            let seq = self.state.next_seq.fetch_add(1, Release) + 1;
                            let p = Packet {
                                header: Header {
                                    seq: seq,
                                    ack: self.state.receiver_acknowledged.load(Acquire),
                                },
                                payload: Some(payload),
                            };
                            self.state.unacked.insert(
                                seq,
                                SentPacket {
                                    packet: p.clone(),
                                    time_sent: Instant::now(),
                                },
                            );
                            self.socket.send_to(&serialize(&p), addr).await;
                        }
                    }
                }
                _ = retransmit_interval.tick() => {
                self.check_retransmission(addr).await;
                }
            }
        }
    }

    pub async fn check_retransmission(&self, addr: std::net::SocketAddr) {
        let now = Instant::now();
        let timeout = Duration::from_millis(2000); // Retransmit after 500ms

        let mut to_retransmit = Vec::new();

        // Collect packets that need retransmission
        for mut entry in self.state.unacked.iter_mut() {
            if now.duration_since(entry.time_sent) > timeout {
                // Update timestamp and mark for retransmission
                entry.time_sent = now;
                to_retransmit.push(entry.packet.clone());
            }
        }

        // Retransmit collected packets
        for packet in to_retransmit {
            // trace!("Retransmitting packet seq={}", packet.header.seq);
            if let Err(e) = self.socket.send_to(&serialize(&packet), addr).await {
                // error!("Failed to retransmit: {}", e);
            }
        }
    }

    pub async fn periodic_acks(
        socket: Arc<UdpSocket>,
        mut ack_rx: mpsc::UnboundedReceiver<AckInfo>,
    ) {
        while let Some(ack_info) = ack_rx.recv().await {
            let ack_packet = Packet {
                header: Header {
                    seq: 0,
                    ack: ack_info.ack_num,
                },
                payload: None,
            };
            let _ = socket
                .send_to(&serialize(&ack_packet), ack_info.peer_addr)
                .await;
        }
    }
}

fn serialize(packet: &Packet) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(8 + packet.payload.as_ref().map_or(0, |p| p.len()));
    buf.extend(&packet.header.seq.to_be_bytes());
    buf.extend(&packet.header.ack.to_be_bytes());
    if let Some(payload) = &packet.payload {
        buf.extend(payload);
    }
    buf
}

fn deserialize(buf: &[u8]) -> Result<Packet> {
    if buf.len() < 8 {
        return Err(anyhow!(
            "Buffer too short: expected at least 8 bytes, got {}",
            buf.len()
        ));
    }
    let seq = u32::from_be_bytes(buf[0..4].try_into()?);
    let ack = u32::from_be_bytes(buf[4..8].try_into()?);

    let payload = if buf.len() > 8 {
        Some(buf[8..].to_vec())
    } else {
        None
    };

    Ok(Packet {
        header: Header { seq, ack: ack },
        payload,
    })
}
