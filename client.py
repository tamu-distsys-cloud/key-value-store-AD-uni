import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply


def nrand() -> int:
    return random.getrandbits(62)


class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg

    def send(self, op: str, args):
        replica = 0
        while True:
            shard = (ord(args.key[0]) + replica) % self.cfg.nservers
            replica = (replica + 1) % self.cfg.nreplicas
            try:
                reply = self.servers[shard].call(f"KVServer.{op}", args)
            except TimeoutError:
                continue
            if reply is None:
                raise Exception("Rejected by server.")
            return reply.value

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        # You will have to modify this function.d
        args = GetArgs(key, nrand())
        return self.send("Get", args)

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        # You will have to modify this function.
        if op not in {"Put", "Append"}:
            raise ValueError('Value must equal "put" or "append".')

        args = PutAppendArgs(key, value, nrand())
        return self.send(op, args)

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
