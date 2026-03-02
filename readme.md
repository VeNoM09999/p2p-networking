# P2P Networking in PURE RUST [File Transfering]

## Library Used & Why
  - For structured events of logs - `tracing, tracing_subscriber`
  - Runtime for both peer & relay - `tokio`
  - Generating random session code - `uuid` 
  - Serializing packet struct into bytes - `serde`
  - Error Handling - `anyhow`

## Installation & Running


- Download the release for your platform from the repo

- Start the server -> `server.exe --port <PORT>` & open the port in the firewall.

- Start a Session -> `peer.exe --mode <MODE> --addr <IP:PORT>`
- Share the code with other peer -> `peer.exe --mode <MODE> --addr <ADDR> --code <UUID>`
- File transfering begins 

## Rendezvous Server ( Information Sharing ) + STUN SERVER
A information exchaning server. Its primary goal is to share two parties network information (IP:PORT) with each other using a UUID code.  

    - Host : Creates a session on server which in return get a UUID code.

    - Peer/Client : Connects to that session using UUID code, There network information gets exchanged. 

## Hole Punching (NAT)
This process runs on both side (simultaneous) whether its a host or peer, starts sending packets to other side using their IP:PORT which creates __mapping table__ (process in NAT which allows incoming packets coming from the same port from where we have send the initial request from) in their router, creating a connection with two side that stays open as long as we keep sending packets. This process needs to be done quickly on both side as nat keeps that mapping for approx 30-60 seconds.

## Fallback (TURN Server)
**TO BE IMPLEMENTED**