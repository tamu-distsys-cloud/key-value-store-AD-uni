import logging
import threading
from typing import Tuple, Any

debugging = True


# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)


# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key: str, value: str, rid: int):
        self.key = key
        self.value = value
        self.id = rid


class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value: str | None):
        self.value = value or ""


class GetArgs:
    # Add definitions here if needed
    def __init__(self, key: str, rid: int):
        self.key = key
        self.id = rid


class GetReply:
    # Add definitions here if needed
    def __init__(self, value: str | None):
        self.value = value or ""


Reply = GetReply | PutAppendReply
Arg = GetArgs | PutAppendArgs


class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg

        # Your definitions here.
        self.data: dict[str, str] = {}
        self.hist = {}

    def valid(self, args: Arg):
        shards = []
        for i in range(self.cfg.nreplicas):
            shard = (ord(args.key[0]) + i) % self.cfg.nservers
            shards.append(self.cfg.kvservers[shard])
        return self in shards

    def replicate(self, op: str, args: Arg):
        for n in range(self.cfg.nreplicas):
            shard = (ord(args.key[0]) + n) % self.cfg.nservers
            target = self.cfg.kvservers[shard]
            if target is self:
                continue
            match op:
                case "Get":
                    target.Get(args)
                case "Put":
                    target.Put(args)
                case "Append":
                    target.Append(args)

    def Get(self, args: GetArgs):
        if not self.valid(args):
            return None

        if args.id in self.hist:
            return self.hist[args.id]

        with self.mu:
            reply = GetReply(self.data.get(args.key))
            self.hist[args.id] = reply

            self.replicate("Get", args)

            return reply

    def Put(self, args: PutAppendArgs):
        if not self.valid(args):
            return None

        if args.id in self.hist:
            return self.hist[args.id]

        with self.mu:
            self.data[args.key] = args.value
            reply = PutAppendReply(self.data.get(args.key))
            self.hist[args.id] = reply

            self.replicate("Put", args)

            return reply

    def Append(self, args: PutAppendArgs):
        if not self.valid(args):
            return None

        if args.id in self.hist:
            return self.hist[args.id]

        with self.mu:
            reply = PutAppendReply(self.data.get(args.key))
            self.data[args.key] = reply.value + args.value
            self.hist[args.id] = reply
            self.replicate("Append", args)
            return reply
