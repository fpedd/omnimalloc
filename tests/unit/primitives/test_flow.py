#
# SPDX-License-Identifier: Apache-2.0
#

import random
from collections import defaultdict, deque

from omnimalloc.primitives.flow import FlowNetwork


def edmonds_karp(edges: list[tuple[int, int, int]], source: int, sink: int) -> int:
    cap: dict[tuple[int, int], int] = defaultdict(int)
    adjacency: dict[int, set[int]] = defaultdict(set)
    for tail, head, capacity in edges:
        cap[tail, head] += capacity
        adjacency[tail].add(head)
        adjacency[head].add(tail)
    flow = 0
    while True:
        parent = {source: source}
        queue = deque([source])
        while queue:
            node = queue.popleft()
            for head in adjacency[node]:
                if head not in parent and cap[node, head] > 0:
                    parent[head] = node
                    queue.append(head)
        if sink not in parent:
            return flow
        bottleneck = float("inf")
        node = sink
        while node != source:
            bottleneck = min(bottleneck, cap[parent[node], node])
            node = parent[node]
        node = sink
        while node != source:
            cap[parent[node], node] -= bottleneck
            cap[node, parent[node]] += bottleneck
            node = parent[node]
        flow += int(bottleneck)


def test_single_edge() -> None:
    network = FlowNetwork(2)
    network.add_edge(0, 1, 7)
    assert network.max_flow(0, 1) == 7


def test_chain_takes_bottleneck() -> None:
    network = FlowNetwork(3)
    network.add_edge(0, 1, 5)
    network.add_edge(1, 2, 3)
    assert network.max_flow(0, 2) == 3


def test_parallel_edges_sum() -> None:
    network = FlowNetwork(2)
    network.add_edge(0, 1, 2)
    network.add_edge(0, 1, 3)
    assert network.max_flow(0, 1) == 5


def test_disjoint_paths_sum() -> None:
    network = FlowNetwork(4)
    network.add_edge(0, 1, 3)
    network.add_edge(1, 3, 3)
    network.add_edge(0, 2, 2)
    network.add_edge(2, 3, 2)
    assert network.max_flow(0, 3) == 5


def test_no_path_returns_zero() -> None:
    network = FlowNetwork(3)
    network.add_edge(0, 1, 5)
    assert network.max_flow(0, 2) == 0


def test_add_edge_creates_zero_capacity_reverse() -> None:
    network = FlowNetwork(2)
    forward = network.add_edge(0, 1, 9)
    assert network.reverse(forward)[1] == 0
    assert forward[1] == 9


def test_saturated_forward_matches_reverse_gain() -> None:
    network = FlowNetwork(2)
    forward = network.add_edge(0, 1, 4)
    network.max_flow(0, 1)
    assert forward[1] == 0
    assert network.reverse(forward)[1] == 4


def test_antiparallel_edges_independent() -> None:
    network = FlowNetwork(2)
    network.add_edge(0, 1, 6)
    network.add_edge(1, 0, 100)
    assert network.max_flow(0, 1) == 6


def test_clrs_reference_graph() -> None:
    edges = [
        (0, 1, 16),
        (0, 2, 13),
        (1, 2, 10),
        (2, 1, 4),
        (1, 3, 12),
        (3, 2, 9),
        (2, 4, 14),
        (4, 3, 7),
        (3, 5, 20),
        (4, 5, 4),
    ]
    network = FlowNetwork(6)
    for tail, head, capacity in edges:
        network.add_edge(tail, head, capacity)
    assert network.max_flow(0, 5) == 23


def test_matches_edmonds_karp_on_random_graphs() -> None:
    rng = random.Random(7)
    for _ in range(200):
        num_nodes = rng.randint(2, 9)
        edges = []
        network = FlowNetwork(num_nodes)
        for _ in range(rng.randint(0, 18)):
            tail = rng.randrange(num_nodes)
            head = rng.randrange(num_nodes)
            if tail == head:
                continue
            capacity = rng.randint(0, 12)
            edges.append((tail, head, capacity))
            network.add_edge(tail, head, capacity)
        assert network.max_flow(0, num_nodes - 1) == edmonds_karp(
            edges, 0, num_nodes - 1
        )
