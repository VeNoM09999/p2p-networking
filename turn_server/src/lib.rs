// #![allow(dead_code, unused)]

pub mod network {
    use core::time;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::os::raw::c_void;
    use std::os::unix::io::RawFd;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use heapless::spsc::{Consumer, Producer};
    use io_uring::{opcode, types};
    use tracing::event;

    // #[cfg(target_os = "linux")]
    use tracing::{Level, instrument};

    //                           LEN   IDX
    pub type SqeType = (SocketAddr, usize, u16);
    // ============================================================
    // CONSTANTS
    // ============================================================

    pub const NUM_SHARDS: usize = 1; // one per CPU core ideally
    pub const BUFFER_SIZE: usize = 1500; // MTU size per buffer
    pub const POOL_SIZE: usize = 1024; // buffers per shard pool

    // ============================================================
    // PACKET
    // A packet coming off the network.
    // `src` tells us who sent it, `data` is the raw UDP payload.
    // ============================================================

    #[derive(Clone, Copy)]
    pub struct Packet {
        pub buf_idx: u16,
        pub len: usize,
        pub src: SocketAddr,
    }

    // ============================================================
    // SESSION
    // One session = one (host, peer) pair being relayed.
    // ============================================================

    pub struct Session {
        pub host_addr: SocketAddr,
        pub peer_addr: SocketAddr,
    }

    impl Session {
        fn route_to(&self, src: &SocketAddr) -> Option<SocketAddr> {
            if *src == self.host_addr {
                Some(self.peer_addr)
            } else if *src == self.peer_addr {
                Some(self.host_addr)
            } else {
                None
            }
        }
    }

    // ============================================================
    // BUFFER POOL
    // Pre-allocated, fixed-size buffers.
    // acquire() → O(1) free-list pop
    // release() → O(1) free-list push
    // ============================================================
    struct PacketTiming {
        recv_cqe_at: Instant,
        shard_done_at: Option<Instant>,
        send_sqe_at: Option<Instant>,
    }

    pub struct BufferPool {
        memory: Vec<u8>,
        in_flight: usize,
        registered: bool,
        free_list: Vec<u16>,
        timestamp: HashMap<u16, PacketTiming>,

        live_slots: HashMap<u16, Box<MsgHdrSlot>>,
    }

    impl BufferPool {
        pub fn default() -> Self {
            Self {
                memory: vec![0u8; POOL_SIZE * BUFFER_SIZE],
                in_flight: 0,
                registered: false,
                live_slots: HashMap::new(),
                free_list: Vec::new(),
                timestamp: HashMap::new(),
            }
        }

        fn register_buffer_kernel(&mut self, submitter: &io_uring::Submitter) {
            let mut io_vec_list = Vec::with_capacity(POOL_SIZE);

            for i in 0..POOL_SIZE {
                let start = i * BUFFER_SIZE;
                let end = start + BUFFER_SIZE;
                let iovec = libc::iovec {
                    iov_base: self.memory[start..end].as_mut_ptr() as *mut c_void,
                    iov_len: BUFFER_SIZE,
                };
                io_vec_list.push(iovec);
                self.free_list.push(i as u16);
            }

            unsafe {
                if submitter.register_buffers(&io_vec_list).is_err() {
                    event!(Level::ERROR, "Error Registering Buffer");
                    panic!("Failed to to register buffer");
                } else {
                    self.registered = true;
                }
            };

            event!(Level::INFO, "bufferpool registered");
        }
        fn post_recv_sqe(
            &mut self,
            sqe: &mut io_uring::SubmissionQueue,
            listen_fd: RawFd,
            idx: u16,
            addr: Option<SocketAddr>,
        ) -> u16 {
            // SAFETY: pool.memory is pinned for the program lifetime;
            // the slot box is kept in live_slots until the CQE fires.
            let (msghdr_ptr, slot) = unsafe { self.msghdr_ptr_for(idx, addr) };
            self.live_slots.insert(idx, slot);

            let entry = opcode::RecvMsg::new(types::Fd(listen_fd), msghdr_ptr)
                .build()
                .user_data(encode_token(OpType::Recv, idx));

            // TODO: send the submission to manager and handle if submission fails, free pool
            if unsafe { sqe.push(&entry) }.is_err() {
                event!(Level::ERROR, "failed to push entry into sqe");
            }
            // if let Err(e) = tx_submission.try_send(entry) {
            //     eprintln!("SQE Channel backpressure: {:?}", e);
            //     live_slots.remove(&idx);
            //     pool.release(idx);
            // };
            idx
        }

        fn post_send_sqe(
            &mut self,
            sqe: &mut io_uring::SubmissionQueue,
            listen_fd: RawFd,
            len: usize,
            idx: u16,
            dest: SocketAddr,
        ) -> u16 {
            let (msghdr_ptr, slot) = unsafe { self.msghdr_ptr_for(idx, Some(dest)) };
            self.live_slots.insert(idx, slot);
            let entry = opcode::SendMsgZc::new(types::Fd(listen_fd), msghdr_ptr)
                .build()
                .user_data(encode_token(OpType::Send, idx));

            // TODO: send the submission to manager and handle if submission fails, free pool
            if unsafe { sqe.push(&entry) }.is_err() {
                event!(Level::ERROR, "failed to push entry into sqe");
            }
            idx
        }

        fn acquire(&mut self) -> Option<u16> {
            self.free_list.pop()
        }

        fn release(&mut self, idx: u16, listen_fd: RawFd, sqe: &mut io_uring::SubmissionQueue) {
            self.post_recv_sqe(sqe, listen_fd, idx, None);
        }

        fn mark_recv_for(&mut self, buf_idx: u16, timestamp: Instant) {
            self.timestamp.insert(
                buf_idx,
                PacketTiming {
                    recv_cqe_at: timestamp,
                    shard_done_at: None,
                    send_sqe_at: None,
                },
            );
        }
        fn mark_shard_done_at(&mut self, buf_idx: &u16, timestamp: Instant) {
            self.timestamp.get_mut(buf_idx).map(|stamp| {
                stamp.shard_done_at = Some(timestamp);
                stamp
            });
        }
        fn mark_send_sqe_at(&mut self, buf_idx: &u16, timestamp: Instant) {
            self.timestamp.get_mut(buf_idx).map(|stamp| {
                stamp.send_sqe_at = Some(timestamp);
                stamp
            });
        }
        fn take_recv_for(&mut self, buf_idx: &u16) -> Option<PacketTiming> {
            self.timestamp.remove(buf_idx)
        }

        fn get_peer_addr(&self, buf_idx: u16) -> SocketAddr {
            self.live_slots
                .get(&buf_idx)
                .map(|s| extract_src_addr(s))
                .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap())
        }
        fn slice(&self, idx: u16, len: usize) -> &[u8] {
            let start = idx as usize * BUFFER_SIZE;
            &self.memory[start..start + len]
        }
        fn slice_mut(&mut self, idx: u16) -> &mut [u8] {
            let start = idx as usize * BUFFER_SIZE;
            &mut self.memory[start..start + BUFFER_SIZE]
        }
    }

    // ============================================================
    // SHARD
    // Each shard owns its channel receiver, session table,
    // and buffer pool — no Mutex/RwLock needed anywhere.
    // ============================================================

    pub struct Shard<'a> {
        pub id: usize,
        recv: Consumer<'a, Packet>,
        session_table: HashMap<SocketAddr, Arc<Session>>,
        tx_out: Producer<'a, SqeType>,
    }

    impl<'a> Shard<'a> {
        pub fn new(id: usize, recv: Consumer<'a, Packet>, tx_out: Producer<'a, SqeType>) -> Self {
            Self {
                id,
                recv,
                session_table: HashMap::new(),
                tx_out,
            }
        }

        pub fn add_session(&mut self, session: Session) {
            let arc = Arc::new(session);
            self.session_table.insert(arc.host_addr, Arc::clone(&arc));
            self.session_table.insert(arc.peer_addr, Arc::clone(&arc));
            event!(
                Level::INFO,
                "[shard {}] registered session {} ↔ {}",
                self.id,
                arc.host_addr,
                arc.peer_addr
            );
        }

        pub fn run(&mut self) {
            event!(Level::INFO, "[shard {}] started", self.id);
            loop {
                if let Some(packet) = self.recv.dequeue() {
                    self.handle_packet(packet);
                } else {
                    std::thread::yield_now();
                }
            }
            // event!(Level::INFO, "[shard {}] channel closed, exiting", self.id);
        }

        pub fn run_n(&mut self, n: usize) {
            let mut finished = 0;
            let timeout = Duration::from_secs(5);
            let start = Instant::now();
            while finished < n {
                if start.elapsed() > timeout {
                    break;
                }
                if let Some(packet) = self.recv.dequeue() {
                    self.handle_packet(packet);
                    finished += 1;
                } else {
                    std::thread::yield_now();
                }
            }
        }

        #[instrument(skip(self ,packet), fields(shard_id = self.id))]
        fn handle_packet(&mut self, packet: Packet) {
            event!(Level::DEBUG,src = %packet.src,len = packet.len,  "got packet");
            if let Some(session) = self.session_table.get(&packet.src) {
                if let Some(dst) = session.route_to(&packet.src) {
                    match self.tx_out.enqueue((dst, packet.len, packet.buf_idx)) {
                        Ok(_) => {
                            event!(Level::DEBUG, "shard -> io_uring loop message sent");
                        }
                        Err(_e) => {
                            event!(Level::DEBUG, "Error sending channel");
                        }
                    }
                } else {
                    event!(Level::ERROR, "route_to returned None for {}", packet.src);
                }
            };
        }
    }

    // ============================================================
    // SHARD ROUTER
    // Routes every packet to the correct shard via a stable hash
    // of the source address — guaranteeing all traffic from one
    // client always lands on the same shard with zero locks.
    // ============================================================

    pub struct ShardRouter<'a> {
        senders: Vec<Producer<'a, Packet>>,
    }

    impl<'a> ShardRouter<'a> {
        pub fn new(senders: Vec<Producer<'a, Packet>>) -> Self {
            Self { senders }
        }

        fn route(&mut self, packet: Packet) {
            let shard_idx = self.shard_for(&packet.src);
            let channel = &mut self.senders[shard_idx];
            if channel.enqueue(packet).is_err() {
                event!(Level::ERROR, "shard {} channel closed", shard_idx);
            }
        }

        fn shard_for(&self, _addr: &SocketAddr) -> usize {
            use std::hash::{Hash, Hasher};
            // ahash is a fast non-crypto hasher — much better distribution than
            // the manual ip^port XOR which clusters on low port numbers.
            let mut hasher = ahash::AHasher::default();
            _addr.hash(&mut hasher);
            (hasher.finish() as usize) % self.senders.len()
        }
    }

    // ============================================================
    // OP TOKEN  (used by the io_uring CQE loop below)
    // Packs OpType + buffer index into the 64-bit user_data field.
    // ============================================================

    // FIX: derive Copy + Clone so the enum can be used in match
    // without being consumed (needed in encode_token).
    #[derive(Copy, Clone)]
    enum OpType {
        Recv,
        Send,
        SendNotif,
    }

    fn encode_token(op: OpType, buf_idx: u16) -> u64 {
        let op_bits = match op {
            OpType::Recv => 0u64,
            OpType::Send => 1u64,
            OpType::SendNotif => 2u64,
        };
        (op_bits << 16) | buf_idx as u64
    }

    fn decode_token(token: u64) -> (OpType, u16) {
        let op = match token >> 16 {
            0 => OpType::Recv,
            1 => OpType::Send,
            2 => OpType::SendNotif,
            _ => OpType::Send,
        };
        (op, (token & 0xFFFF) as u16)
    }

    // ============================================================
    // IO_URING EVENT LOOP  (Linux 5.1+, io-uring crate required)
    //
    // This compiles with `io-uring = "0.6"` on Linux.
    // On other platforms gate it with `#[cfg(target_os = "linux")]`.
    // ============================================================

    // ── MsgHdrSlot ──────────────────────────────────────────────
    // RecvMsg::new() requires a *mut libc::msghdr, NOT *mut iovec.
    // msghdr is the POSIX recvmsg header; it *contains* a pointer
    // to one or more iovecs plus a name buffer for the peer address.
    //
    // We box the entire bundle so the pointers inside msghdr
    // remain stable while the kernel is writing into it.
    //
    // Layout (all fields kernel-visible via raw pointers):
    //
    //   ┌─────────────────────────────────────────────────────┐
    //   │  MsgHdrSlot                                          │
    //   │  ┌──────────────────┐   ┌──────────────────────────┐│
    //   │  │ libc::msghdr     │──▶│ iov: libc::iovec         ││
    //   │  │  msg_name ───────┼──▶│ addr: sockaddr_storage   ││
    //   │  │  msg_namelen     │   └──────────────────────────┘│
    //   │  │  msg_iov ────────┘                               │
    //   │  │  msg_iovlen = 1                                  │
    //   │  └──────────────────┘                               │
    //   └─────────────────────────────────────────────────────┘
    //
    // iov.iov_base points directly into BufferPool::memory —
    // the kernel DMA's (or copies) the packet payload there.
    struct MsgHdrSlot {
        hdr: libc::msghdr,
        iov: libc::iovec,
        addr: libc::sockaddr_storage,
    }

    impl MsgHdrSlot {
        /// Build a zeroed slot whose iov_base points at `buf`.
        ///
        /// SAFETY: `buf` must outlive this slot (both live inside
        /// BufferPool::memory for the duration of the program).
        unsafe fn new(buf: *mut u8, addr: Option<SocketAddr>) -> Box<Self> {
            unsafe {
                let mut slot = Box::new(MsgHdrSlot {
                    addr: std::mem::zeroed(),
                    hdr: std::mem::zeroed(),
                    iov: std::mem::zeroed(),
                });

                slot.iov.iov_base = buf as *mut _;
                slot.iov.iov_len = BUFFER_SIZE;

                slot.hdr.msg_iov = &mut slot.iov as *mut _;
                slot.hdr.msg_iovlen = 1;
                if let Some(addr) = addr {
                    let len = write_socket_addr(&mut slot.addr, addr);
                    slot.hdr.msg_namelen = len;
                    slot.hdr.msg_name = &mut slot.addr as *mut _ as *mut c_void;
                } else {
                    slot.hdr.msg_name = &mut slot.addr as *mut _ as *mut c_void;
                    slot.hdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
                }

                slot
            }
        }

        /// Return a raw *mut msghdr for submission to io_uring.
        fn as_msghdr_ptr(&mut self) -> *mut libc::msghdr {
            &mut self.hdr as *mut libc::msghdr
        }
    }

    // ── msghdr_slots extension on BufferPool ────────────────────
    // We keep one MsgHdrSlot per pool buffer, allocated lazily on
    // first use.  Production code pre-allocates the whole Vec up
    // front in BufferPool::new().
    impl BufferPool {
        /// Return a *mut msghdr suitable for RecvMsg::new().
        ///
        /// The slot's iov_base already points into our memory slab
        /// at the correct offset for `idx`.
        ///
        /// SAFETY: The returned pointer is valid as long as both
        /// `self` and the returned Box live — store the Box alongside
        /// the ring entries in production.
        unsafe fn msghdr_ptr_for(
            &mut self,
            idx: u16,
            addr: Option<SocketAddr>,
        ) -> (*mut libc::msghdr, Box<MsgHdrSlot>) {
            let buf_ptr = self.slice_mut(idx).as_mut_ptr();
            let mut slot = unsafe { MsgHdrSlot::new(buf_ptr, addr) };
            let ptr = slot.as_msghdr_ptr();
            (ptr, slot)
        }
    }

    // ── extract_src_addr ────────────────────────────────────────
    // After a RecvMsg CQE fires, the kernel has written the sender's
    // address into msg_name.  Parse it into a std::net::SocketAddr.
    //
    // We take the MsgHdrSlot (not the CQE — the CQE carries only
    // the byte count and flags) because the address lives in the
    // slot's `addr` field, not in the CQE itself.
    fn extract_src_addr(slot: &MsgHdrSlot) -> SocketAddr {
        use libc::{AF_INET, AF_INET6};
        // SAFETY: sockaddr_storage is large enough for both v4 and v6;
        // the kernel wrote a valid sockaddr here.
        unsafe {
            let family = slot.addr.ss_family as i32;
            if family == AF_INET {
                let sin = &*(&slot.addr as *const _ as *const libc::sockaddr_in);
                let ip = std::net::Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
                let port = u16::from_be(sin.sin_port);
                SocketAddr::from((ip, port))
            } else if family == AF_INET6 {
                let sin6 = &*(&slot.addr as *const _ as *const libc::sockaddr_in6);
                let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
                let port = u16::from_be(sin6.sin6_port);
                SocketAddr::from((ip, port))
            } else {
                // Fallback — should never happen on a UDP socket.
                "0.0.0.0:0".parse().unwrap()
            }
        }
    }

    unsafe fn write_socket_addr(
        storage: &mut libc::sockaddr_storage,
        addr: SocketAddr,
    ) -> libc::socklen_t {
        match addr {
            SocketAddr::V4(v4) => {
                let sin = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: v4.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from(*v4.ip()).to_be(),
                    },
                    sin_zero: [0; 8],
                };

                unsafe { std::ptr::write(storage as *mut _ as *mut libc::sockaddr_in, sin) };
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
            }

            SocketAddr::V6(v6) => {
                let sin6 = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: v6.port().to_be(),
                    sin6_flowinfo: v6.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: v6.ip().octets(),
                    },
                    sin6_scope_id: v6.scope_id(),
                };
                unsafe { std::ptr::write(storage as *mut _ as *mut libc::sockaddr_in6, sin6) };
                std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
            }
        }
    }
    // ── io_uring_loop ────────────────────────────────────────────
    //
    // WHY ring.split()?
    // -----------------
    // ring.completion() returns a CompletionQueue that holds &mut IoUring
    // for its entire lifetime.  Calling ring.submission().push() inside
    // that loop is a *second* &mut borrow → compile error.
    //
    // ring.split() decomposes the ring into three independently-borrowable
    // parts (Submitter, SubmissionQueue, CompletionQueue).  Because they
    // are separate struct references, the borrow checker sees no aliasing
    // and both can be used concurrently — which is safe because they touch
    // different regions of the shared memory-mapped ring.
    //
    // WHY collect CQEs before awaiting?
    // ----------------------------------
    // The CompletionQueue iterator also holds a borrow that cannot cross
    // an .await point (Future requires all held references to be Send or
    // dropped before suspension).  We drain all ready CQEs into a plain
    // Vec<(OpType, u16, usize)> first — releasing the CQ borrow — then
    // process them (submit new SQEs, route packets, await) freely.
    #[cfg(target_os = "linux")]
    pub fn io_uring_loop<'a>(
        ring: &mut io_uring::IoUring,
        mut sqe_rx: Vec<Consumer<'a, SqeType>>,
        pool: &mut BufferPool,
        router: &mut ShardRouter,
        listen_fd: RawFd,
    ) {
        // TODO: Buffer registeration with kernel

        // Split the ring into independently borrowable halves.
        // `sq`  → SubmissionQueue  (push SQEs)
        // `cq`  → CompletionQueue  (drain CQEs)
        // `sub` → Submitter        (syscall / SQPOLL flush)
        let (submitter, mut sq, mut cq) = ring.split();

        pool.register_buffer_kernel(&submitter); // Phase 1 : Registeration Entry sent to ring. Waiting
        // for submit
        let mut seeded = 0;
        for _ in 0..POOL_SIZE {
            if let Some(idx) = pool.acquire() {
                pool.post_recv_sqe(&mut sq, listen_fd, idx, None);
                seeded += 1;
            }
        }
        event!(Level::DEBUG, "[uring] pushed {} recv SQEs", seeded);

        sq.sync();
        submitter.submit().unwrap();

        // ── main event loop ────────────────────────────────────────
        loop {
            let mut pushed = 0;
            for queue in &mut sqe_rx {
                while let Some((addr, len, idx)) = queue.dequeue() {
                    let _entry = pool.post_send_sqe(&mut sq, listen_fd, len, idx, addr);
                    pool.mark_send_sqe_at(&idx, Instant::now());
                    event!(Level::DEBUG, "processing send sqe");
                    pushed += 1;
                }
            }
            if pushed > 0 {
                sq.sync();
                submitter.submit().unwrap();
            }
            // Wait for at least one CQE (blocks if ring is idle).
            // Use submit_and_wait(1) so we don't busy-spin; in SQPOLL
            // mode replace with submit() and a lightweight sleep/yield.
            // submitter.submit_and_wait(1);
            cq.sync(); // update the kernel-visible head pointer
            if cq.is_empty() {
                cq.sync();
            }

            //
            //     Phase 3 Processing CQE
            //
            let completed: Vec<(OpType, u16, usize, u32)> = {
                (&mut cq)
                    .map(|cqe| {
                        let (op, idx) = decode_token(cqe.user_data());
                        let sys_call = cqe.result().max(0) as usize;
                        let flags = cqe.flags();
                        (op, idx, sys_call, flags)
                    })
                    .collect()
                // cq released
            };
            event!(Level::DEBUG, "[uring] processed {} CQEs", completed.len());

            for (op, buf_idx, len, flags) in completed {
                match op {
                    OpType::Recv => {
                        pool.mark_recv_for(buf_idx, Instant::now());
                        // Extract peer address from the msghdr the kernel filled.
                        let src = pool.get_peer_addr(buf_idx);

                        // Route packet to the respective shard.
                        // .await is safe here — cq borrow is fully released.

                        router.route(Packet { buf_idx, src, len });
                        pool.mark_shard_done_at(&buf_idx, Instant::now());
                    }

                    OpType::Send => {
                        // Kernel finished sending — buffer is safe to reuse. # Reregistering buffer
                        if !io_uring::cqueue::more(flags) {
                            pool.release(buf_idx, listen_fd, &mut sq);
                            if let Some(t) = pool.take_recv_for(&buf_idx) {
                                // event!(Level::INFO, "processing time : {:#?}", instant.elapsed());
                                let send_cqe_at = Instant::now();
                                let shard_done_at =
                                    t.shard_done_at.unwrap().duration_since(t.recv_cqe_at);
                                let handoff = t
                                    .send_sqe_at
                                    .unwrap()
                                    .duration_since(t.shard_done_at.unwrap());
                                let kernel = send_cqe_at.duration_since(t.send_sqe_at.unwrap());
                                let total = send_cqe_at.duration_since(t.recv_cqe_at);
                                event!(Level::INFO, total = ?total, shard = ?shard_done_at, handoff = ?handoff, kernel = ?kernel);
                            };
                            event!(
                                Level::DEBUG,
                                "BufferPool with {} reregister for new RECV",
                                buf_idx
                            );
                        }
                    }
                    OpType::SendNotif => {
                        pool.release(buf_idx, listen_fd, &mut sq);
                    }
                }
            }

            // Phase 4 Send SQE for kernel to process
            // Listen for new sqe entry generated by shards and push them in sqe and later flush
            // let deadline = std::time::Instant::now() + std::time::Duration::from_micros(150);
            // loop {
            //     if std::time::Instant::now() > deadline {
            //         break;
            //     }
            //     std::thread::yield_now();
            // }
        }
    }
}
