import random
from aodv import AODVNode
from dsr import DSRNode
from flood_project import FloodNode
from mobility import MobilityManager
from pywisim import EventLoop, WirelessNetwork

random.seed(42)

N = 30
AREA_X, AREA_Y = 10.0, 10.0
TX_RANGE = 2.0
TX_TIME = 0.8
MOB_SPEED = 0.4
MOB_INTERVAL = 0.5
SIM_TIME = 100
TRAFFIC_INTERVAL = 2.0

# Same initial node positions for all three runs
positions = [
    (random.uniform(0, AREA_X), random.uniform(0, AREA_Y))
    for _ in range(N)
]


def run_experiment(node_cls, label):
    loop = EventLoop()
    net = WirelessNetwork(loop, tx_range=TX_RANGE, tx_time=TX_TIME, seed=7, verbose=False)

    nodes = []
    for i, (x, y) in enumerate(positions):
        node = node_cls(i)
        net.add_node(node, x, y)
        nodes.append(node)

    mob = MobilityManager(net, interval=MOB_INTERVAL, speed=MOB_SPEED, bounds=(AREA_X, AREA_Y))
    mob.start("waypoint")

    msg_id = 0

    def generate_traffic():
        nonlocal msg_id
        src = random.choice(nodes)
        dest = random.choice(nodes)

        if src.nid != dest.nid:
            msg_id += 1
            src.send_data(dest.nid, "hello", msg_id)

        loop.schedule(TRAFFIC_INTERVAL, generate_traffic)

    loop.schedule(1.0, generate_traffic)
    loop.run(until=SIM_TIME)

    total_sent = sum(n.sent for n in nodes)
    total_received = sum(n.received for n in nodes)
    total_forwarded = sum(n.forwarded for n in nodes)
    total_delay = sum(n.total_delay for n in nodes)

    pdr = total_received / total_sent if total_sent else 0
    avg_delay = total_delay / total_received if total_received else 0

    return {
        "Protocol": label,
        "Sent": total_sent,
        "Received": total_received,
        "PDR": pdr,
        "Avg Delay": avg_delay,
        "Overhead": total_forwarded,
    }


results = [
    run_experiment(AODVNode, "AODV"),
    run_experiment(DSRNode, "DSR"),
    run_experiment(FloodNode, "Flooding"),
]

# Print one comparison table
print("\n==================== Comparison Table ====================")
print(f"{'Protocol':<10} {'Sent':>6} {'Recv':>6} {'PDR':>8} {'Avg Delay':>12} {'Overhead':>10}")
print("-" * 60)
for r in results:
    print(
        f"{r['Protocol']:<10} "
        f"{r['Sent']:>6} "
        f"{r['Received']:>6} "
        f"{r['PDR']:>8.2f} "
        f"{r['Avg Delay']:>12.2f} "
        f"{r['Overhead']:>10}"
    )