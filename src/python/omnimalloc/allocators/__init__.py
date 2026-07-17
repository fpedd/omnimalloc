#
# SPDX-License-Identifier: Apache-2.0
#

from .base import BaseAllocator as BaseAllocator
from .best_fit import BestFitAllocator as BestFitAllocator
from .genetic import GeneticAllocator as GeneticAllocator
from .greedy import GreedyAllocator as GreedyAllocator
from .greedy import GreedyByAllAllocator as GreedyByAllAllocator
from .greedy import GreedyByAreaAllocator as GreedyByAreaAllocator
from .greedy import GreedyByConflictAllocator as GreedyByConflictAllocator
from .greedy import GreedyByConflictSizeAllocator as GreedyByConflictSizeAllocator
from .greedy import GreedyByDurationAllocator as GreedyByDurationAllocator
from .greedy import GreedyBySizeAllocator as GreedyBySizeAllocator
from .greedy import GreedyByStartAllocator as GreedyByStartAllocator
from .hillclimb import HillClimbAllocator as HillClimbAllocator
from .minimalloc import MinimallocAllocator as MinimallocAllocator
from .naive import NaiveAllocator as NaiveAllocator
from .omni import OmniAllocator as OmniAllocator
from .random import RandomAllocator as RandomAllocator
from .simulated_annealing import (
    SimulatedAnnealingAllocator as SimulatedAnnealingAllocator,
)
from .supermalloc import SupermallocAllocator as SupermallocAllocator
from .tabu_search import TabuSearchAllocator as TabuSearchAllocator
from .telamalloc import TelamallocAllocator as TelamallocAllocator
from .utils import DEFAULT_ALLOCATOR as DEFAULT_ALLOCATOR
from .utils import available_allocators as available_allocators
