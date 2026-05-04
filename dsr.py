"""DSR source-routing protocol for PyWiSim."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pywisim import Node


class DSRNode(Node):
    def __init__(self, nid):
        super().__init__(nid)

        # Route cache: dest -> (route_list, expiry_time)
        # route_list is always a full source route starting at this node
        self.route_cache = {}

        # Discovery / duplicate suppression
        self.seen_rreqs = set()   # (origin, req_id)
        self.seen_data = set()

        # Buffered packets while waiting for a route
        self.pending = {}
        self.discovery_in_progress = set()
        self.req_id = 0
        self.retry_count = {}
        self.max_retries = 3
        self.retry_delay = 2.0
        self.route_lifetime = 15.0

        # Metrics
        self.sent = 0
        self.received = 0
        self.forwarded = 0
        self.total_delay = 0

    def _route_valid(self, route):
        if not route or len(route) < 2:
            return False

        # Check that every hop in the route is still a current neighbor
        for a, b in zip(route, route[1:]):
            if b not in self.net.neighbors(a):
                return False
        return True

    def _cache_route(self, dest, route):
        if not route:
            return
        expiry = self.net.loop.time + self.route_lifetime
        cur = self.route_cache.get(dest)

        # Keep shorter routes if multiple are seen
        if not cur or len(route) < len(cur[0]) or self.net.loop.time > cur[1]:
            self.route_cache[dest] = (list(route), expiry)

    def _get_route(self, dest):
        entry = self.route_cache.get(dest)
        if not entry:
            return None

        route, expiry = entry
        if self.net.loop.time > expiry:
            self.route_cache.pop(dest, None)
            return None

        if not self._route_valid(route):
            self.route_cache.pop(dest, None)
            return None

        return route

    def _send_rrep(self, origin, dest, route):
        # route is a full source route from origin -> ... -> dest
        self._cache_route(dest, route)

        msg = ("RREP", origin, dest, route)

        if self.nid == origin:
            # origin has received the route; flush pending packets
            self.discovery_in_progress.discard(dest)
            self.retry_count.pop(dest, None)

            self.net.log(f"{self.nid}: route to {dest} found: {route}")

            # Cache the route at the origin
            self._cache_route(dest, route)

            for pkt in self.pending.get(dest, []):
                self.unicast(route[1], pkt)
            self.pending[dest] = []
            return

        idx = route.index(self.nid)
        if idx > 0:
            prev_hop = route[idx - 1]
            self.unicast(prev_hop, msg)

    def discover(self, dest):
        self.req_id += 1
        req = ("RREQ", self.nid, dest, self.req_id, [self.nid])
        self.seen_rreqs.add((self.nid, self.req_id))
        self.net.log(f"{self.nid}: route discovery -> {dest}")
        self.broadcast(req)

    def on_receive(self, msg, sender):
        kind = msg[0]

        if kind == "RREQ":
            _, origin, dest, req_id, path = msg
            key = (origin, req_id)

            if key in self.seen_rreqs:
                return
            self.seen_rreqs.add(key)

            path = list(path)
            if path[-1] != self.nid:
                path.append(self.nid)

            # Cache reverse route back to origin
            self._cache_route(origin, list(reversed(path)))

            # If we are the destination, send the full route back
            if self.nid == dest:
                self._send_rrep(origin, dest, path)
                return

            # If we already know a route to the destination, answer using it
            cached = self._get_route(dest)
            if cached:
                full_route = path + cached[1:]   # avoid duplicating current node
                self._send_rrep(origin, dest, full_route)
                return

            # Otherwise forward the discovery
            self.broadcast(("RREQ", origin, dest, req_id, path))

        elif kind == "RREP":
            _, origin, dest, route = msg
            route = list(route)

            # Cache both directions around this node
            if self.nid in route:
                idx = route.index(self.nid)

                to_dest = route[idx:]
                to_origin = list(reversed(route[:idx + 1]))
                self._cache_route(dest, to_dest)
                self._cache_route(origin, to_origin)

            if self.nid == origin:
                # Route has arrived at source
                self.discovery_in_progress.discard(dest)
                self.retry_count.pop(dest, None)

                self.net.log(f"{self.nid}: route to {dest} via {route}")

                for pkt in self.pending.get(dest, []):
                    self.unicast(route[1], pkt)
                self.pending[dest] = []
                return

            # Forward reply back along the reverse path
            if self.nid in route:
                idx = route.index(self.nid)
                if idx > 0:
                    prev_hop = route[idx - 1]
                    self.unicast(prev_hop, msg)

        elif kind == "DATA":
            _, src, dest, payload, msg_id, send_time, route = msg

            if msg_id in self.seen_data:
                return
            self.seen_data.add(msg_id)

            if self.nid == dest:
                self.received += 1
                delay = self.net.loop.time - send_time
                self.total_delay += delay
                self.net.log(f"{self.nid} received DATA from {src}, delay={delay:.2f}")
                return
            
            if route is None:
                self.pending.setdefault(dest, []).append(msg)
                if dest not in self.discovery_in_progress:
                    self.discovery_in_progress.add(dest)
                    self.retry_count[dest] = 0
                    self.schedule(self.retry_delay, self._retry_pending, dest)
                    self.discover(dest)
                return

            if self.nid in route:
                idx = route.index(self.nid)
                if idx < len(route) - 1:
                    next_hop = route[idx + 1]
                    if next_hop in self.net.neighbors(self.nid):
                        self.forwarded += 1
                        self.unicast(next_hop, msg)
                        return

            # Route broke or packet arrived unexpectedly
            self.route_cache.pop(dest, None)
            self.pending.setdefault(dest, []).append(msg)

            if dest not in self.discovery_in_progress:
                self.discovery_in_progress.add(dest)
                self.retry_count[dest] = 0
                self.schedule(self.retry_delay, self._retry_pending, dest)
                self.discover(dest)

    def send_data(self, dest, payload, msg_id):
        send_time = self.net.loop.time
        self.sent += 1

        route = self._get_route(dest)
        if route:
            msg = ("DATA", self.nid, dest, payload, msg_id, send_time, route)
            self.unicast(route[1], msg)
            return

        # No route yet: buffer and start discovery
        self.pending.setdefault(dest, [])
        self.pending[dest].append(("DATA", self.nid, dest, payload, msg_id, send_time, None))

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

        route = self._get_route(dest)
        if route:
            # Flush buffered packets now that we have a route
            for pkt in self.pending[dest]:
                _, src, d, payload, msg_id, send_time, _ = pkt
                msg = ("DATA", src, d, payload, msg_id, send_time, route)
                self.unicast(route[1], msg)

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
        self.net.log(f"{self.nid}: retry route discovery for {dest} ({count + 1}/{self.max_retries})")
        self.discover(dest)
        self.schedule(self.retry_delay, self._retry_pending, dest)