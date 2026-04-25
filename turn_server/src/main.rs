use heapless::spsc::Queue;
use std::os::fd::AsRawFd;
use tracing::Level;
use tracing::event;
use turn_server::network::BufferPool;
use turn_server::network::Session;
use turn_server::network::io_uring_loop;
use turn_server::network::{
    BUFFER_SIZE, NUM_SHARDS, POOL_SIZE, Packet, Shard, ShardRouter, SqeType,
};

// ============================================================
// MAIN
// ============================================================

static mut SHARD_QUEUES: [Queue<Packet, BUFFER_SIZE>; NUM_SHARDS] =
    [const { Queue::new() }; NUM_SHARDS];
static mut ROUTER_QUEUES: [Queue<SqeType, BUFFER_SIZE>; NUM_SHARDS] =
    [const { Queue::new() }; NUM_SHARDS];

fn init_tracing() {
    use std::fs::File;
    use tracing_subscriber::{
        EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt,
    };

    let file = File::create("runtime.log").unwrap();
    let (non_blocking, _guard) = tracing_appender::non_blocking(file);
    Box::leak(Box::new(_guard)); // Keep guard alive

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_target(false)
                .without_time()
                .with_thread_ids(false)
                .with_thread_names(false)
                .with_file(false)
                .with_line_number(false)
                .with_filter(EnvFilter::new("info")), // .compact(), // .with_filter(EnvFilter::new("debug")),
                                                      // .with_filter(EnvFilter::new("debug")), // .compact(), // .with_filter(EnvFilter::new("debug")),
        )
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    event!(Level::INFO, "Starting TURN server");
    // Build shard channels
    let mut shard_prods = Vec::with_capacity(NUM_SHARDS);
    let mut shards: Vec<Shard> = Vec::with_capacity(NUM_SHARDS);
    let mut ring_consumers = Vec::with_capacity(NUM_SHARDS);

    for id in 0..NUM_SHARDS {
        let shards_ref = unsafe { &mut *(&raw mut SHARD_QUEUES) };
        let shardrouter_ref = unsafe { &mut *(&raw mut ROUTER_QUEUES) };
        let (ring_producer, ring_consumer) = shards_ref[id].split();
        let (shard_producer, shard_consumer) = shardrouter_ref[id].split();
        ring_consumers.push(shard_consumer);
        shard_prods.push(ring_producer);
        shards.push(Shard::new(id, ring_consumer, shard_producer));
    }

    let socket = std::net::UdpSocket::bind("0.0.0.0:8080").expect("Failed to bind port");
    let fd = socket.as_raw_fd();

    let mut shard_router = ShardRouter::new(shard_prods);

    let mut buffer_pool = BufferPool::default();
    let mut io_uring = io_uring::IoUring::builder()
        .setup_sqpoll(2)
        .setup_sqpoll_cpu(2)
        .build(POOL_SIZE as u32)?;
    // let mut io_uring = io_uring::IoUring::new(2048).expect("Failed to set ioring");

    for mut shard in shards {
        std::thread::spawn(move || {
            if shard.id == 0 {
                shard.add_session(Session {
                    host_addr: "127.0.0.1:9001".parse().unwrap(),
                    peer_addr: "127.0.0.1:9002".parse().unwrap(),
                });
            }
            shard.run();
        });
    }
    io_uring_loop(
        &mut io_uring,
        ring_consumers,
        &mut buffer_pool,
        &mut shard_router,
        fd,
    );

    Ok(())
}
