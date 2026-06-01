use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const MAX_PAYLOAD: usize = 1000;
const SEND_INTERVAL: Duration = Duration::from_micros(1); // ~10,000 pps per side
const SAMPLE_DURATION: Duration = Duration::from_secs(1); // update every 1 second
const TURN_SERVER_ADDR: &str = "192.168.1.40:8080";

fn main() {
    let host = Arc::new(UdpSocket::bind("192.168.1.42:9001").unwrap());
    let peer = Arc::new(UdpSocket::bind("192.168.1.42:9002").unwrap());

    host.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    let host_count = Arc::new(AtomicU64::new(0));
    let peer_count = Arc::new(AtomicU64::new(0));

    // Peer thread
    let peer_count_clone = Arc::clone(&peer_count);
    thread::spawn(move || {
        let peer = Arc::clone(&peer);
        let mut payload_buf = [0u8; MAX_PAYLOAD];
        let mut i = 0u64;

        thread::sleep(Duration::from_secs(1)); // wait for host setup

        println!("[test] peer 9001 → proxy 8080: streaming (pps on one line)");

        loop {
            let len = 1 + (i % (MAX_PAYLOAD as u64 - 1)) as usize;
            let payload = &mut payload_buf[..len];
            payload.fill((i % 256) as u8);

            if let Err(e) = peer.send_to(payload, TURN_SERVER_ADDR) {
                eprintln!("peer send error: {}", e);
            }

            peer_count_clone.fetch_add(1, Ordering::Relaxed);
            i = i.wrapping_add(1);

            thread::sleep(SEND_INTERVAL);
        }
    });

    let mut buf = [0u8; MAX_PAYLOAD];

    match host.recv_from(&mut buf) {
        Ok((len, src)) => {
            println!("[test] host received: {} bytes from {}", len, src);
            println!("[test] host 9002 → proxy 8080: streaming (pps on one line)");

            let host = Arc::clone(&host);
            let host_count = Arc::clone(&host_count);
            let peer_count = Arc::clone(&peer_count);
            let host_count_thread = Arc::clone(&host_count);

            let host_thread = thread::spawn(move || {
                let mut payload_buf = [0u8; MAX_PAYLOAD];
                let mut i = 0u64;

                loop {
                    let len = 1 + (i % (MAX_PAYLOAD as u64 - 1)) as usize;
                    let payload = &mut payload_buf[..len];
                    payload.fill((i % 256) as u8);

                    if let Err(e) = host.send_to(payload, TURN_SERVER_ADDR) {
                        eprintln!("host send error: {}", e);
                    }

                    host_count_thread.fetch_add(1, Ordering::Relaxed);
                    i = i.wrapping_add(1);

                    thread::sleep(SEND_INTERVAL);
                }
            });

            // PPS display loop (clears and reprints on same line)
            let mut last_print = Instant::now();
            loop {
                thread::sleep(Duration::from_millis(100));

                let now = Instant::now();
                if now.duration_since(last_print) >= SAMPLE_DURATION {
                    let h = host_count.swap(0, Ordering::Relaxed);
                    let p = peer_count.swap(0, Ordering::Relaxed);

                    let elapsed = now.duration_since(last_print).as_secs_f64();
                    let host_pps = h as f64 / elapsed;
                    let peer_pps = p as f64 / elapsed;
                    let combined_pps = host_pps + peer_pps;

                    // Clear current line and print fresh pps
                    print!(
                        "\r[test] HOST: {:.0} pps | PEER: {:.0} pps | COMBINED: {:.0} pps       ",
                        host_pps, peer_pps, combined_pps
                    );
                    let _ = std::io::Write::flush(&mut std::io::stdout()); // force flush

                    last_print = now;
                }
            }

            // host_thread.join().unwrap(); // unreachable in this loop
        }
        Err(e) => println!("[test] host got nothing: {}", e),
    }
}
