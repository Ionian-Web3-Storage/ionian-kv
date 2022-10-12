import base64
import os

from config.node_config import KV_CONFIG
from test_framework.blockchain_node import NodeType, TestNode
from utility.utils import (
    initialize_config,
    rpc_port,
    kv_rpc_port,
    blockchain_rpc_port,
    assert_equal
)


class KVNode(TestNode):
    def __init__(
        self,
        index,
        root_dir,
        binary,
        updated_config,
        log_contract_address,
        log,
        rpc_timeout=10,
        stream_ids=[]
    ):
        local_conf = KV_CONFIG.copy()

        indexed_config = {
            "stream_ids": stream_ids,
            "rpc_listen_address": f"127.0.0.1:{kv_rpc_port(index)}",
            "log_contract_address": log_contract_address,
            "blockchain_rpc_endpoint": f"http://127.0.0.1:{blockchain_rpc_port(0)}",
            "ionian_node_url": f"http://127.0.0.1:{rpc_port(0)}"
        }
        # Set configs for this specific node.
        local_conf.update(indexed_config)
        # Overwrite with personalized configs.
        local_conf.update(updated_config)
        data_dir = os.path.join(root_dir, "ionian_kv" + str(index))
        rpc_url = "http://" + local_conf["rpc_listen_address"]
        super().__init__(
            NodeType.KV,
            index,
            data_dir,
            rpc_url,
            binary,
            local_conf,
            log,
            rpc_timeout,
        )

    def setup_config(self):
        os.mkdir(self.data_dir)
        log_config_path = os.path.join(
            self.data_dir, self.config["log_config_file"])
        with open(log_config_path, "w") as f:
            f.write("info")
        initialize_config(self.config_file, self.config)

    def wait_for_rpc_connection(self):
        self._wait_for_rpc_connection(
            lambda rpc: rpc.kv_getStatus() is not None)

    def start(self):
        self.log.info("Start kv node %d", self.index)
        super().start()

    def check_equal(self, stream_id, key, value, version=None):
        i = 0
        while i < len(value):
            res = self.kv_get_value(stream_id, key, i, 1000, version)
            if i + 1000 < len(value):
                assert_equal(base64.b64decode(
                    res['data'].encode("utf-8")
                ), value[i: i + 1000])
            else:
                assert_equal(base64.b64decode(
                    res['data'].encode("utf-8")
                ), value[i:])
            i += 1000

    # rpc
    def kv_get_value(self, stream_id, key, start_index, size, version=None):
        return self.rpc.kv_getValue([stream_id, key, start_index, size, version])

    def kv_get_trasanction_result(self, tx_seq):
        return self.rpc.kv_getTransactionResult([tx_seq])