use common::relay::relay_control_server::RelayControlServer;
use heapless::spsc::Queue;
use std::os::fd::AsRawFd;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tracing::Level;
use tracing::event;
use turn_server::network::BufferPool;
use turn_server::network::MessageType;
use turn_server::network::RelayControlService;
use turn_server::network::io_uring_loop;
use turn_server::network::{BUFFER_SIZE, POOL_SIZE, Shard, ShardRouter};

// ============================================================
// MAIN
// ============================================================

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
const SHARD_NUM: usize = 2;
static mut SHARD_QUEUES: [Queue<MessageType, BUFFER_SIZE>; SHARD_NUM] = [const { Queue::new() }; 2];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let relay_service =
    let cores = core_affinity::get_core_ids().expect("Failed to get the cores id ");

    init_tracing();
    event!(Level::INFO, "Starting TURN server");
    // Build shard channels
    let mut shard_prods = Vec::with_capacity(SHARD_NUM);
    let (tx, rx) = mpsc::channel(1024 * SHARD_NUM);

    for id in 0..SHARD_NUM {
        let core = cores[id];
        let cloned = tx.clone();
        let shard_ref = unsafe { &mut *(&raw mut SHARD_QUEUES) };
        let (ring_producer, ring_consumer) = shard_ref[id].split_const();
        shard_prods.push(ring_producer);
        std::thread::spawn(move || {
            core_affinity::set_for_current(core);
            let mut shard = Shard::new(id, ring_consumer, cloned.clone());
            shard.run();
        });
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

    let (control_tx, control_rx) = mpsc::channel(1024);

    let grpc_service = RelayControlService { control_tx };

    std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            event!(Level::INFO, "gRPC service started");

            Server::builder()
                .add_service(RelayControlServer::new(grpc_service))
                .serve("0.0.0.0:50051".parse().unwrap())
                .await
                .unwrap()
        });
    });

    core_affinity::set_for_current(cores[cores.len() - 1]);
    io_uring_loop(
        &mut io_uring,
        rx,
        &mut buffer_pool,
        &mut shard_router,
        fd,
        control_rx,
    );

    Ok(())
}
