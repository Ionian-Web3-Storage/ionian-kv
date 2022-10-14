#!/usr/bin/env python3
import base64
from this import d
import time
from os import access
import random
from test_framework.test_framework import TestFramework
from utility.kv import (MAX_U64, op_with_address, op_with_key, STREAM_DOMAIN,
                        MAX_STREAM_ID, pad, to_key, to_stream_id, create_kv_data, AccessControlOps, rand_key, rand_write)
from utility.submission import submit_data
from utility.submission import create_submission
from utility.utils import (
    assert_equal,
    wait_until,
)
from config.node_config import TX_PARAMS, TX_PARAMS1, GENESIS_ACCOUNT, GENESIS_ACCOUNT1


class KVAccessControlTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 1

    def run_test(self):
        # setup kv node, watch stream with id [0,100)
        self.stream_ids = [to_stream_id(i) for i in range(MAX_STREAM_ID)]
        self.setup_kv_node(0, self.stream_ids)
        # tx_seq and data mapping
        self.next_tx_seq = 0
        self.data = {}
        # write empty stream
        self.write_streams()

    def submit(self, version, reads, writes, access_controls, tx_params=TX_PARAMS, given_tags=None, trunc=False):
        chunk_data, tags = create_kv_data(
            version, reads, writes, access_controls)
        if trunc:
            chunk_data = chunk_data[:random.randrange(
                len(chunk_data) / 2, len(chunk_data))]
        submissions, data_root = create_submission(
            chunk_data, tags if given_tags is None else given_tags)
        self.log.info("data root: %s, submissions: %s", data_root, submissions)
        self.contract.submit(submissions, tx_params=tx_params)
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

    def update_data(self, writes):
        for write in writes:
            self.data[','.join([write[0], write[1]])] = write[3]

    def write_streams(self):
        # first put
        writes = [rand_write(self.stream_ids[0])]
        access_controls = [AccessControlOps.grant_writer_role(
            self.stream_ids[0], GENESIS_ACCOUNT1.address)]
        self.submit(MAX_U64, [], writes, access_controls)
        wait_until(lambda: self.kv_nodes[0].kv_get_trasanction_result(
            self.next_tx_seq) == "Commit")
        first_version = self.next_tx_seq
        self.next_tx_seq += 1

        # check data and role
        self.update_data(writes)
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(',')
            self.kv_nodes[0].check_equal(stream_id, key, value)
        assert_equal(self.kv_nodes[0].kv_is_admin(
            GENESIS_ACCOUNT.address, self.stream_ids[0]), True)
        assert_equal(self.kv_nodes[0].kv_is_writer_of_stream(
            GENESIS_ACCOUNT1.address, self.stream_ids[0]), True)


if __name__ == "__main__":
    KVAccessControlTest().main()