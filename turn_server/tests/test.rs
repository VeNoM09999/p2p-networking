// #![allow(dead_code, unused_variables)]

#[cfg(test)]
pub mod tests {
    use std::{net::SocketAddr, str::FromStr, time::Duration};

    use heapless::spsc::{self};
    use turn_server::network::{Packet, Session, Shard, SqeType};

    #[test]
    fn shard_test() {
        // Initializing Queues
        let total_packet_to_be_sent = 10;
        let shard_q = Box::leak(Box::new(spsc::Queue::<Packet, 100>::new()));
        let sqe_q = Box::leak(Box::new(spsc::Queue::<SqeType, 100>::new()));



        let (mut shard_producer, shard_consumer) = shard_q.split();
        let (producer_sqe, mut cosumer_sqe) = sqe_q.split();

        let mut shard = Shard::new(0, shard_consumer, producer_sqe);
        let src: SocketAddr =
            SocketAddr::from_str("127.0.0.1:9090").expect("Failed to parse socket addr ");
        let dest: SocketAddr =
            SocketAddr::from_str("127.0.0.1:9090").expect("Failed to parse socket addr ");

        // spawning shard thread
        let shard_thread = std::thread::spawn(move || {
            let session = Session {
                host_addr: src,
                peer_addr: dest,
            };
            shard.add_session(session);
            shard.run_n(total_packet_to_be_sent);
        });

        let p: Packet = Packet {
            buf_idx: 0,
            len: 10,
            src,
        };

        //
        // spawning sqe thread to run until total_packet_to_be_sent is received
        //

        let sqe_thread = std::thread::spawn(move || {
            let starting = std::time::Instant::now();
            let timeout = Duration::from_millis(100);
            let mut processed: usize = 0;
            while processed < total_packet_to_be_sent {
                if starting.elapsed() > timeout {
                    break;
                }
                if let Some(_sqe) = cosumer_sqe.dequeue() {
                    processed += 1;
                }
            }
            processed
        });

        // sending entry to shards thinking we get the actual packet from client
        for _i in 0..total_packet_to_be_sent {
            let _ = shard_producer.enqueue(p);
        }

        // Waiting for all the threads to finish processing
        shard_thread.join().expect("shard thread panicked");
        let results = sqe_thread.join().expect("sqe thread panicked");
        assert_eq!(results, total_packet_to_be_sent);
    }
}
