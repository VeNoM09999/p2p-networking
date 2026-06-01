// #![allow(dead_code, unused, unused_variables)]

use std::time::Instant;

#[cfg(feature = "multiconnection")]
pub mod multiconnection;

#[cfg(feature = "singleconnection")]
pub mod singleconnection;

pub enum PacketType {
    Ack { ack: u32 },
    Data { payload: Vec<u8> },
}

#[derive(Clone, Debug)]
pub struct Header {
    pub seq: u32, //sequence number
    pub ack: u32, //cumulative ACK
}

#[derive(Clone, Debug)]
pub struct Packet {
    pub header: Header,
    pub payload: Option<Vec<u8>>,
}
struct SentPacket {
    pub packet: Packet,
    pub time_sent: Instant,
}

#[cfg(feature = "relay")]
pub mod relay {
    tonic::include_proto!("relay");
}
