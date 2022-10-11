#!/usr/bin/env python3
import base64
import time
from os import access
from test_framework.test_framework import TestFramework
from utility.kv import (MAX_U64, op_with_address, op_with_key, STREAM_DOMAIN,
                        MAX_STREAM_ID, pad, to_key, to_stream_id, create_kv_data, AccessControlOps, rand_key)
from utility.submission import submit_data
from utility.submission import create_submission
from utility.utils import (
    assert_equal,
    wait_until,
)


class KVTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 1

    def run_test(self):
        # setup kv node, watch stream with id [0,100)
        self.stream_ids = [to_stream_id(i) for i in range(MAX_STREAM_ID)]
        self.setup_kv_node(0, self.stream_ids)
        self.next_tx_seq = 0
        # write empty stream
        self.write_empty_stream(self.stream_ids[0])

    def submit(self, version, reads, writes, access_controls):
        chunk_data, tags = create_kv_data(
            version, reads, writes, access_controls)
        submissions, data_root = create_submission(chunk_data, tags)
        self.log.info("data root: %s, submissions: %s", data_root, submissions)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions()
                   == self.next_tx_seq + 1)

        client = self.nodes[0]
        wait_until(lambda: client.ionian_get_file_info(data_root) is not None)
        assert_equal(client.ionian_get_file_info(
            data_root)["finalized"], False)

        segments = submit_data(client, chunk_data)
        self.log.info("segments: %s", [
                      (s["root"], s["index"], s["proof"]) for s in segments])
        wait_until(lambda: client.ionian_get_file_info(data_root)["finalized"])

    def write_empty_stream(self, stream_id):
        l = 256 * 1000 * 2 + 1
        writes = [[stream_id, rand_key(), l]]
        self.submit(MAX_U64, [], writes, [])
        wait_until(lambda: self.kv_nodes[0].kv_get_trasanction_result(
            self.next_tx_seq) == "Commit")
        self.next_tx_seq += 1

        self.kv_nodes[0].check_equal(writes[0][0], writes[0][1], writes[0][3])


if __name__ == "__main__":
    KVTest().main()
