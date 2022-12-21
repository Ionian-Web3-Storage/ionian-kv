# Run

## Run Storage Node
Please reference this [guidance](https://github.com/Ionian-Web3-Storage/ionian-rust/blob/main/doc/run.md) to run storage node.

## Run KV Node

### Build from source
[Optional] Reference [this](https://github.com/Ionian-Web3-Storage/ionian-rust/blob/main/doc/install.md#install-rust) to setup build environment if needed.

```shell
# Download code
$ git clone https://github.com/Ionian-Web3-Storage/ionian-kv
$ cd ionian-kv
$ git submodule update --init

# Build in release mode
$ cargo build --release
```

### Update configuration file
Update config **run/config_example.toml** as required, some items that you need to update:

```shell
# stream ids
stream_ids

# KV db directory
kv_db_dir

# KV RPC endpoint
rpc_listen_address

# storage node RPC endpoint
ionian_node_urls

# layer one blockchain RPC endpoint
blockchain_rpc_endpoint

# flow contract address
log_contract_address

# sync start block number
log_sync_start_block_number
```

### Run node
```shell
cd run
../target/release/ionian_kv --config config_example.toml
```