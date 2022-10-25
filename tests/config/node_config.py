from web3 import Web3

IONIAN_CONFIG = dict(log_config_file="log_config")

KV_CONFIG = dict(log_config_file="log_config")

BSC_CONFIG = dict(
    NetworkId=1000,
    HTTPPort=8545,
    HTTPHost="127.0.0.1",
    Etherbase="0x7df9a875a174b3bc565e6424a0050ebc1b2d1d82",
    DataDir="test/local_ethereum_blockchain/node1",
    Port=30303,
    Verbosity=5,
)

CONFLUX_CONFIG = dict(
    mode="test",
    chain_id=10,
    jsonrpc_http_eth_port=8545,
    tcp_port=32323,
    log_level="debug",
    log_file="./conflux.log",
    public_address="127.0.0.1",
    poll_lifetime_in_seconds=60,
    dev_allow_phase_change_without_peer="true",
    # dev_block_interval_ms=50,
)

BLOCK_SIZE_LIMIT = 200 * 1024
GENESIS_PRIV_KEY = "46b9e861b63d3509c88b7817275a30d22d62c8cd8fa6486ddee35ef0d8e0495f"
MINER_ID = "308a6e102a5829ba35e4ba1da0473c3e8bd45f5d3ffb91e31adb43f25463dddb"
GENESIS_ACCOUNT = Web3().eth.account.from_key(GENESIS_PRIV_KEY)
TX_PARAMS = {"gasPrice": 10_000_000_000, "from": GENESIS_ACCOUNT.address, "gas": 10_000_000}

GENESIS_PRIV_KEY1 = "9a6d3ba2b0c7514b16a006ee605055d71b9edfad183aeb2d9790e9d4ccced471"
GENESIS_ACCOUNT1 = Web3().eth.account.from_key(GENESIS_PRIV_KEY1)
TX_PARAMS1 = {"gasPrice": 10_000_000_000, "from": GENESIS_ACCOUNT1.address, "gas": 10_000_000}

NO_SEAL_FLAG = 0x1
NO_MERKLE_PROOF_FLAG = 0x2
