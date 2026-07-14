#
# SPDX-License-Identifier: Apache-2.0
#

from collections import deque


class FlowNetwork:
    """Dinic max flow on an adjacency list of [head, residual, reverse-index]."""

    def __init__(self, num_nodes: int) -> None:
        self.adjacency: list[list[list[int]]] = [[] for _ in range(num_nodes)]

    def add_edge(self, tail: int, head: int, capacity: int) -> list[int]:
        edge = [head, capacity, len(self.adjacency[head])]
        self.adjacency[tail].append(edge)
        self.adjacency[head].append([tail, 0, len(self.adjacency[tail]) - 1])
        return edge

    def reverse(self, edge: list[int]) -> list[int]:
        return self.adjacency[edge[0]][edge[2]]

    def max_flow(self, source: int, sink: int) -> int:
        flow = 0
        while True:
            levels = self._levels(source, sink)
            if levels is None:
                return flow
            cursors = [0] * len(self.adjacency)
            while True:
                pushed = self._augment(source, sink, levels, cursors)
                if not pushed:
                    break
                flow += pushed

    def _levels(self, source: int, sink: int) -> list[int] | None:
        levels = [-1] * len(self.adjacency)
        levels[source] = 0
        queue = deque([source])
        while queue:
            node = queue.popleft()
            for head, residual, _ in self.adjacency[node]:
                if residual > 0 and levels[head] < 0:
                    levels[head] = levels[node] + 1
                    queue.append(head)
        return levels if levels[sink] >= 0 else None

    def _augment(
        self, source: int, sink: int, levels: list[int], cursors: list[int]
    ) -> int:
        path: list[tuple[int, list[int]]] = []
        node = source
        while node != sink:
            advanced = False
            while cursors[node] < len(self.adjacency[node]):
                edge = self.adjacency[node][cursors[node]]
                if edge[1] > 0 and levels[edge[0]] == levels[node] + 1:
                    path.append((node, edge))
                    node = edge[0]
                    advanced = True
                    break
                cursors[node] += 1
            if not advanced:
                levels[node] = -1
                if not path:
                    return 0
                node = path.pop()[0]
        bottleneck = min(edge[1] for _, edge in path)
        for _, edge in path:
            edge[1] -= bottleneck
            self.reverse(edge)[1] += bottleneck
        return bottleneck
