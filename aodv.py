"""AODV reactive routing protocol."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pywisim import Node


class AODVNode(Node):
    def __init__(self, nid):
        super().__init__(nid)
        self.seq = 0

        # dest -> (next_hop, seq, hops, expiry_time)
        self.routes = {}

        self.seen_rreqs = set()
        self.seen_data = set()

        self.pending = {}
        self.retry_count = {}
        self.discovery_in_progress = set()

        self.max_retries = 5
        self.retry_delay = 2.0
        self.route_lifetime = 20.0

        # metrics
        self.sent = 0
        self.received = 0
        self.forwarded = 0
        self.total_delay = 0

    def _route_valid(self, dest):
        route = self.routes.get(dest)
        if not route:
            return False

        next_hop, seq, hops, expiry_time = route
        if self.net.loop.time > expiry_time:
            self.routes.pop(dest, None)
            return False

        if next_hop not in self.net.neighbors(self.nid):
            self.routes.pop(dest, None)
            return False

        return True

    def on_receive(self, msg, sender):
        if msg[0] == 'RREQ':
            _, orig, dest, seq, rid, hops = msg
            key = (orig, dest, seq, rid)
            if key in self.seen_rreqs:
                return

            self.seen_rreqs.add(key)
            hops += 1
            self._update(orig, sender, seq, hops)

            if dest == self.nid:
                self.seq = max(self.seq, seq) + 1
                self._rrep(dest, orig)

            elif self._route_valid(dest):
                self._rrep(dest, orig)

            else:
                self.broadcast(('RREQ', orig, dest, seq, rid, hops))

        elif msg[0] == 'RREP':
            _, dest, dseq, orig, hops = msg
            hops += 1
            self._update(dest, sender, dseq, hops)

            if orig == self.nid:
                self.net.log(f"{self.nid}: route to {dest} via {self.routes[dest][0]}, hops={hops}")

                self.discovery_in_progress.discard(dest)
                self.retry_count.pop(dest, None)

                for pkt in self.pending.get(dest, []):
                    next_hop = self.routes[dest][0]
                    self.unicast(next_hop, pkt)
                self.pending[dest] = []

            elif orig in self.routes and self._route_valid(orig):
                self.unicast(self.routes[orig][0], ('RREP', dest, dseq, orig, hops))

        elif msg[0] == "DATA":
            _, src, dest, payload, msg_id, send_time = msg

            if msg_id in self.seen_data:
                return
            self.seen_data.add(msg_id)

            if self.nid == dest:
                self.received += 1
                delay = self.net.loop.time - send_time
                self.total_delay += delay
                self.net.log(f"{self.nid} received DATA from {src}, delay={delay:.2f}")
                return

            elif self._route_valid(dest):
                next_hop = self.routes[dest][0]
                self.forwarded += 1
                self.unicast(next_hop, msg)

            else:
                self.pending.setdefault(dest, []).append(msg)
                if dest not in self.discovery_in_progress:
                    self.discovery_in_progress.add(dest)
                    self.retry_count[dest] = 0
                    self.schedule(self.retry_delay, self._retry_pending, dest)
                    self.discover(dest)

    def _update(self, dest, via, seq, hops):
        expiry_time = self.net.loop.time + self.route_lifetime
        cur = self.routes.get(dest)
        if not cur or seq > cur[1] or (seq == cur[1] and hops < cur[2]):
            self.routes[dest] = (via, seq, hops, expiry_time)

    def _rrep(self, dest, orig):
        if orig in self.routes and self._route_valid(orig):
            self.unicast(self.routes[orig][0], ('RREP', dest, self.seq, orig, 0))

    def discover(self, dest):
        self.seq += 1
        self.seen_rreqs.add((self.nid, dest, self.seq, self.seq))
        self.net.log(f"{self.nid}: route discovery -> {dest}")
        self.broadcast(('RREQ', self.nid, dest, self.seq, self.seq, 0))

    def send_data(self, dest, payload, msg_id):
        send_time = self.net.loop.time
        msg = ("DATA", self.nid, dest, payload, msg_id, send_time)

        self.sent += 1

        if self._route_valid(dest):
            next_hop = self.routes[dest][0]
            self.unicast(next_hop, msg)
        else:
            self.pending.setdefault(dest, []).append(msg)
            if dest not in self.discovery_in_progress:
                self.discovery_in_progress.add(dest)
                self.retry_count[dest] = 0
                self.schedule(self.retry_delay, self._retry_pending, dest)
                self.discover(dest)

    def _retry_pending(self, dest):
        if dest not in self.pending or not self.pending[dest]:
            self.retry_count.pop(dest, None)
            self.discovery_in_progress.discard(dest)
            return

        if self._route_valid(dest):
            next_hop = self.routes[dest][0]
            for pkt in self.pending[dest]:
                self.unicast(next_hop, pkt)
            self.pending[dest] = []
            self.retry_count.pop(dest, None)
            self.discovery_in_progress.discard(dest)
            return

        count = self.retry_count.get(dest, 0)
        if count >= self.max_retries:
            self.net.log(f"{self.nid}: dropping pending packets for {dest} after {count} retries")
            self.pending[dest] = []
            self.retry_count.pop(dest, None)
            self.discovery_in_progress.discard(dest)
            return

        self.retry_count[dest] = count + 1
        self.net.log(f"{self.nid}: retry route discovery for {dest} ({count+1}/{self.max_retries})")
        self.discover(dest)
        self.schedule(self.retry_delay, self._retry_pending, dest)