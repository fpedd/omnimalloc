//
// SPDX-License-Identifier: Apache-2.0
//

#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/string_view.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/variant.h>
#include <nanobind/stl/vector.h>

#include <span>
#include <sstream>

#include "allocators/best_fit.hpp"
#include "allocators/first_fit.hpp"
#include "allocators/omni.hpp"
#include "allocators/simulated_annealing.hpp"
#include "allocators/supermalloc.hpp"
#include "allocators/tabu_search.hpp"
#include "allocators/telamalloc.hpp"
#include "analysis/antichain.hpp"
#include "analysis/closure.hpp"
#include "analysis/collision.hpp"
#include "analysis/conflicts.hpp"
#include "analysis/linearize.hpp"
#include "analysis/placement.hpp"
#include "common/parallel.hpp"
#include "primitives/allocation.hpp"
#include "primitives/allocation_kind.hpp"
#include "primitives/id_type.hpp"

namespace nb = nanobind;
using namespace nb::literals;
using namespace omnimalloc;

namespace {

std::string allocation_str(const Allocation& a) {
  std::ostringstream ss;
  ss << a;
  return ss.str();
}

// nanobind's default caster renders std::vector as list; the Python surface
// wants tuples so scalar/tuple time points stay consistent under == and hash.
// One component means a scalar: 1-element vectors normalize on construction.
nb::object time_to_python(std::span<const int64_t> time) {
  if (time.size() == 1) {
    return nb::int_(time[0]);
  }
  return nb::tuple(nb::cast(std::vector<int64_t>(time.begin(), time.end())));
}

}  // namespace

NB_MODULE(_cpp, m) {
  nb::enum_<AllocationKind>(m, "AllocationKind")
      .value("WORKSPACE", AllocationKind::WORKSPACE)
      .value("CONSTANT", AllocationKind::CONSTANT)
      .value("INPUT", AllocationKind::INPUT)
      .value("OUTPUT", AllocationKind::OUTPUT)
      .def_prop_ro("is_io", &is_io)
      .def("__str__", &to_string)
      .def("__repr__", &to_string);

  nb::class_<Allocation>(m, "Allocation")
      .def(nb::init<IdType, int64_t, TimePoint, TimePoint,
                    std::optional<int64_t>, std::optional<AllocationKind>>(),
           "id"_a, "size"_a, "start"_a, "end"_a, "offset"_a = nb::none(),
           "kind"_a = nb::none())
      .def_prop_ro("id", &Allocation::id)
      .def_prop_ro("size", &Allocation::size)
      .def_prop_ro(
          "start",
          [](const Allocation& a) { return time_to_python(a.start_vec()); },
          nb::for_getter(
              nb::sig("def start(self, /) -> int | tuple[int, ...]")))
      .def_prop_ro(
          "end",
          [](const Allocation& a) { return time_to_python(a.end_vec()); },
          nb::for_getter(nb::sig("def end(self, /) -> int | tuple[int, ...]")))
      .def_prop_ro("dim", &Allocation::dim)
      .def_prop_ro("offset", &Allocation::offset)
      .def_prop_ro("kind", &Allocation::kind)
      .def_prop_ro("is_allocated", &Allocation::is_allocated)
      .def_prop_ro("duration", &Allocation::duration)
      .def_prop_ro("height", &Allocation::height)
      .def_prop_ro("area", &Allocation::area)
      .def("conflicts_with", &Allocation::conflicts_with, "other"_a)
      .def("overlaps_spatially", &Allocation::overlaps_spatially, "other"_a)
      .def("overlaps", &Allocation::overlaps, "other"_a)
      .def("with_offset", &Allocation::with_offset, "offset"_a.none())
      .def("__str__", &allocation_str)
      .def("__repr__", &allocation_str)
      // is_operator: return NotImplemented for non-Allocation operands
      // instead of raising TypeError, per the Python equality protocol
      .def("__eq__", &Allocation::operator==, nb::is_operator())
      .def("__hash__", std::hash<Allocation>{})
      // Times route through time_to_python so pickled payloads match the
      // tuple form the start/end properties expose; old list-payload pickles
      // still load through __setstate__'s sequence caster.
      .def(
          "__getstate__",
          [](const Allocation& a) {
            return nb::make_tuple(nb::cast(a.id()), a.size(),
                                  time_to_python(a.start_vec()),
                                  time_to_python(a.end_vec()),
                                  nb::cast(a.offset()), nb::cast(a.kind()));
          },
          nb::sig("def __getstate__(self) -> tuple[int | str, int, int | "
                  "tuple[int, ...], int | tuple[int, ...], int | None, "
                  "AllocationKind | None]"))
      .def("__setstate__",
           [](Allocation& a,
              const std::tuple<IdType, int64_t, TimePoint, TimePoint,
                               std::optional<int64_t>,
                               std::optional<AllocationKind>>& state) {
             new (&a) Allocation(std::get<0>(state), std::get<1>(state),
                                 std::get<2>(state), std::get<3>(state),
                                 std::get<4>(state), std::get<5>(state));
           });

  // Worker ceiling for every native kernel; 0 lifts it to the usable cores
  m.def("set_max_threads", &set_max_threads, "value"_a);
  m.def("max_threads", &max_threads);

  m.def("find_collision", &find_collision, "allocations"_a,
        nb::call_guard<nb::gil_scoped_release>());

  // The conflict relation stays in CSR form on the C++ side and rows cross
  // the boundary one at a time; the Python `conflicts` map is built on this
  nb::class_<ConflictGraph>(m, "ConflictGraph")
      .def(nb::init<const std::vector<Allocation>&, std::optional<uint64_t>,
                    std::optional<uint64_t>>(),
           "allocations"_a, "work_budget"_a.none(),
           "max_entries"_a.none() = nb::none(),
           nb::call_guard<nb::gil_scoped_release>())
      .def("__len__", &ConflictGraph::size)
      .def_prop_ro("pair_count", &ConflictGraph::pair_count)
      .def("degree", &ConflictGraph::degree, "index"_a)
      .def("neighbors", &ConflictGraph::neighbors, "index"_a);
  m.def("conflict_degrees", &conflict_degrees, "allocations"_a,
        "work_budget"_a.none(), nb::call_guard<nb::gil_scoped_release>());
  // Budget exhaustion and "not an interval order" are different answers, so
  // the Python surface separates them the way every other budgeted analysis
  // does: raise past the budget, return None for the structural obstruction
  m.def(
      "try_linearize",
      [](const std::vector<Allocation>& allocations,
         std::optional<uint64_t> work_budget) {
        bool undecided = false;
        auto linearized = try_linearize(allocations, work_budget, &undecided);
        if (undecided) {
          throw std::runtime_error(
              "Linearize work exceeds work_budget; pass None to always "
              "decide linearizability");
        }
        return linearized;
      },
      "allocations"_a, "work_budget"_a.none(),
      nb::call_guard<nb::gil_scoped_release>());
  m.def("antichain_pressure", &antichain_pressure, "allocations"_a,
        "work_budget"_a.none(), nb::call_guard<nb::gil_scoped_release>());
  m.def("closure_pressure", &closure_pressure, "allocations"_a,
        "closure_cap"_a.none(), nb::call_guard<nb::gil_scoped_release>());
  m.def("antichain_pressure_per_allocation", &antichain_pressure_per_allocation,
        "allocations"_a, "work_budget"_a.none(),
        nb::call_guard<nb::gil_scoped_release>());
  m.def("closure_pressure_per_allocation", &closure_pressure_per_allocation,
        "allocations"_a, "closure_cap"_a.none(),
        nb::call_guard<nb::gil_scoped_release>());
  m.def("placement_pressure_per_allocation", &placement_pressure_per_allocation,
        "allocations"_a, "work_budget"_a.none(),
        nb::call_guard<nb::gil_scoped_release>());

  nb::enum_<GreedyOrder>(m, "GreedyOrder")
      .value("INPUT", GreedyOrder::INPUT)
      .value("SIZE", GreedyOrder::SIZE)
      .value("DURATION", GreedyOrder::DURATION)
      .value("AREA", GreedyOrder::AREA)
      .value("CONFLICT", GreedyOrder::CONFLICT)
      .value("CONFLICT_SIZE", GreedyOrder::CONFLICT_SIZE)
      .value("START", GreedyOrder::START);
  m.def(
      "greedy_order",
      [](const std::vector<Allocation>& allocations, GreedyOrder order) {
        return greedy_order(allocations, build_conflict_adjacency(allocations),
                            order);
      },
      "allocations"_a, "order"_a, nb::call_guard<nb::gil_scoped_release>());
  m.def(
      "greedy_place",
      [](const std::vector<Allocation>& allocations, GreedyOrder order) {
        return greedy_place(allocations, order);
      },
      "allocations"_a, "order"_a, nb::call_guard<nb::gil_scoped_release>());

  // Standing invariant for every gil_scoped_release-guarded method in this
  // module: const and stateless-or-synchronized, because releasing the GIL
  // admits concurrent same-object calls. Every guarded path is Python-free.
  nb::class_<FirstFitPlacer>(m, "FirstFitPlacer")
      .def(nb::init<std::vector<Allocation>>(), "allocations"_a,
           nb::call_guard<nb::gil_scoped_release>())
      .def("peak", &FirstFitPlacer::peak, "order"_a,
           nb::call_guard<nb::gil_scoped_release>())
      .def("place", &FirstFitPlacer::place, "order"_a,
           nb::call_guard<nb::gil_scoped_release>());

  // Placement functions: data types and flat functions cross the
  // boundary, nothing else. The config structs stay C++-internal; each
  // lambda constructs one from the flat parameters.
  m.def("best_fit_place", &best_fit_place, "allocations"_a,
        nb::call_guard<nb::gil_scoped_release>());
  m.def("omni_place", &omni_place, "allocations"_a, "linearize_budget"_a.none(),
        nb::call_guard<nb::gil_scoped_release>());
  m.def(
      "simulated_annealing_place",
      [](const std::vector<Allocation>& allocations, uint64_t seed,
         int max_iterations, double initial_temperature, double cooling_rate,
         std::optional<double> timeout) {
        return simulated_annealing_place(
            allocations,
            {seed, max_iterations, initial_temperature, cooling_rate, timeout});
      },
      "allocations"_a, "seed"_a, "max_iterations"_a, "initial_temperature"_a,
      "cooling_rate"_a, "timeout"_a.none(),
      nb::call_guard<nb::gil_scoped_release>());
  m.def(
      "tabu_search_place",
      [](const std::vector<Allocation>& allocations, uint64_t seed,
         int max_iterations, int neighborhood_size, int tabu_tenure,
         std::optional<double> timeout) {
        return tabu_search_place(
            allocations,
            {seed, max_iterations, neighborhood_size, tabu_tenure, timeout});
      },
      "allocations"_a, "seed"_a, "max_iterations"_a, "neighborhood_size"_a,
      "tabu_tenure"_a, "timeout"_a.none(),
      nb::call_guard<nb::gil_scoped_release>());
  m.def(
      "telamalloc_place",
      [](const std::vector<Allocation>& allocations, uint64_t seed,
         int max_backtracks, std::optional<double> timeout) {
        return telamalloc_place(allocations, {seed, max_backtracks, timeout});
      },
      "allocations"_a, "seed"_a, "max_backtracks"_a, "timeout"_a.none(),
      nb::call_guard<nb::gil_scoped_release>());

  nb::class_<Partition>(m, "Partition")
      .def_static("from_allocations", &Partition::from_allocations,
                  "allocations"_a, nb::call_guard<nb::gil_scoped_release>())
      .def("reorder", &Partition::reorder, "heuristic"_a,
           nb::call_guard<nb::gil_scoped_release>())
      .def("with_bound", &Partition::with_bound, "bound"_a,
           nb::call_guard<nb::gil_scoped_release>())
      .def_prop_ro("lower_bound", &Partition::lower_bound);

  nb::class_<Solution>(m, "Solution")
      .def_ro("allocations", &Solution::allocations)
      .def_ro("peak", &Solution::peak);

  m.def("greedy_pack_portfolio", &greedy_pack_portfolio, "partition"_a,
        "heuristics"_a, "timeout"_a.none(), "num_threads"_a,
        nb::call_guard<nb::gil_scoped_release>());

  // Parameter order: problem inputs, algorithm knobs, timeout, num_threads. The
  // five search switches bind flat; SearchOptions stays C++-internal.
  m.def(
      "try_solve_many",
      [](const std::vector<Partition>& partitions, int64_t best_bound,
         std::optional<int64_t> max_nodes, bool canonical, bool dominance,
         bool floor_inference, bool monotonic_floor, bool decompose,
         std::optional<double> timeout, int num_threads) {
        return try_solve_many(
            partitions, best_bound, max_nodes,
            {canonical, dominance, floor_inference, monotonic_floor, decompose},
            timeout, num_threads);
      },
      "partitions"_a, "best_bound"_a, "max_nodes"_a.none(), "canonical"_a,
      "dominance"_a, "floor_inference"_a, "monotonic_floor"_a, "decompose"_a,
      "timeout"_a.none(), "num_threads"_a,
      nb::call_guard<nb::gil_scoped_release>());
}
