from enum import Enum
import random


class AccessControlOps(Enum):
    GRANT_ADMIN_ROLE = 0x00
    RENOUNCE_ADMIN_ROLE = 0x01
    SET_KEY_TO_SPECIAL = 0x10
    SET_KEY_TO_NORMAL = 0x11
    GRANT_WRITER_ROLE = 0x20
    REVOKE_WRITER_ROLE = 0x21
    RENOUNCE_WRITER_ROLE = 0x22
    GRANT_SPECIAL_WRITER_ROLE = 0x30
    REVOKE_SPECIAL_WRITER_ROLE = 0x31
    RENOUNCE_SPECIAL_WRITER_ROLE = 0x32


op_with_key = [
    AccessControlOps.SET_KEY_TO_SPECIAL,
    AccessControlOps.SET_KEY_TO_NORMAL,
    AccessControlOps.GRANT_SPECIAL_WRITER_ROLE,
    AccessControlOps.REVOKE_SPECIAL_WRITER_ROLE,
    AccessControlOps.RENOUNCE_SPECIAL_WRITER_ROLE]

op_with_address = [
    AccessControlOps.GRANT_ADMIN_ROLE,
    AccessControlOps.GRANT_WRITER_ROLE,
    AccessControlOps.REVOKE_WRITER_ROLE,
    AccessControlOps.GRANT_SPECIAL_WRITER_ROLE,
    AccessControlOps.REVOKE_SPECIAL_WRITER_ROLE]


MAX_STREAM_ID = 100
MAX_U64 = (1 << 64) - 1

STREAM_DOMAIN = bytes.fromhex(
    "df2ff3bb0af36c6384e6206552a4ed807f6f6a26e7d0aa6bff772ddc9d4307aa")


def pad(x, l):
    ans = hex(x)[2:]
    return '0' * (l - len(ans)) + ans


def to_stream_id(x):
    return pad(x, 64)


def to_key(x):
    return pad(x, 64)


def rand_key():
    return to_key(random.randrange(0, MAX_U64))

# reads: array of [stream_id, key]
# writes: array of [stream_id, key, data_length]


def create_kv_data(version, reads, writes, access_controls):
    # version
    data = bytes.fromhex(pad(version, 16))
    tags = []
    # read set
    data += bytes.fromhex(pad(len(reads), 8))
    for read in reads:
        data += bytes.fromhex(read[0])
        data += bytes.fromhex(read[1])
    # write set
    data += bytes.fromhex(pad(len(writes), 8))
    # write set meta
    for write in writes:
        data += bytes.fromhex(write[0])
        data += bytes.fromhex(write[1])
        data += bytes.fromhex(pad(write[2], 16))
        write_data = random.randbytes(write[2])
        tags.append(write[0])
        write.append(write_data)
    # write data
    for write in writes:
        data += write[3]
    # access controls
    data += bytes.fromhex(pad(len(access_controls), 8))
    for ac in access_controls:
        k = 0
        # type
        data += bytes.fromhex(pad(ac[k].value, 2))
        k += 1
        # stream_id
        data += ac[k]
        k += 1
        tags.append(ac[k])
        # key
        if ac[0] in op_with_key:
            data += ac[k]
            k += 1
        # address
        if ac[0] in op_with_address:
            data += ac[k]
            k += 1
    tags = STREAM_DOMAIN + bytes.fromhex(''.join(list(set(tags))))
    return data, tags
