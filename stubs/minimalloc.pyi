#
# SPDX-License-Identifier: Apache-2.0
#

class Lifespan:
    lower: int
    upper: int
    def __init__(self, lower: int = ..., upper: int = ...) -> None: ...

class Buffer:
    id: str
    size: int
    lifespan: Lifespan
    def __init__(
        self, id: str = ..., size: int = ..., lifespan: Lifespan = ...
    ) -> None: ...

class Problem:
    buffers: list[Buffer]
    capacity: int
    def __init__(self, buffers: list[Buffer] = ...) -> None: ...

class SolverParams:
    timeout: int
    minimize_capacity: bool
    def __init__(self) -> None: ...

class Solution:
    offsets: list[int]

class Solver:
    def __init__(self, params: SolverParams) -> None: ...
    def solve(self, problem: Problem) -> Solution | None: ...
