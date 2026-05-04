"""Flooding baseline for a multi-hop wireless network."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pywisim import Node


class FloodNode(Node):
    def __init__(self, nid):
        super().__init__(nid)
        self.seen_data = set()

        # Metrics
        self.sent = 0
        self.received = 0
        self.forwarded = 0
        self.total_delay = 0

    def on_receive(self, msg, sender):
        if msg[0] != "DATA":
            return

        _, src, dest, payload, msg_id, send_time = msg

        # Duplicate suppression
        if msg_id in self.seen_data:
            return
        self.seen_data.add(msg_id)

        # Destination reached
        if self.nid == dest:
            self.received += 1
            delay = self.net.loop.time - send_time
            self.total_delay += delay
            self.net.log(f"{self.nid} received DATA from {src}, delay={delay:.2f}")
            return

        # Otherwise rebroadcast
        self.forwarded += 1
        self.broadcast(msg)

    def send_data(self, dest, payload, msg_id):
        send_time = self.net.loop.time
        msg = ("DATA", self.nid, dest, payload, msg_id, send_time)

        self.sent += 1
        self.seen_data.add(msg_id)
        self.broadcast(msg)