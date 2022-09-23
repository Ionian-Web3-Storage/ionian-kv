use std::net::SocketAddr;

#[derive(Clone)]
pub struct Config {
    pub enabled: bool,
    pub listen_address: SocketAddr,
    pub chunks_per_segment: usize,
    pub ionian_node_url: String,
    pub max_query_len_in_bytes: u64,
}
