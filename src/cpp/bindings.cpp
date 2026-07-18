//
// SPDX-License-Identifier: Apache-2.0
//

#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/unordered_map.h>
#include <nanobind/stl/unordered_set.h>
#include <nanobind/stl/variant.h>
#include <nanobind/stl/vector.h>

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
#include "analysis/conflicts.hpp"
#include "analysis/linearize.hpp"
#include "analysis/placement.hpp"
#include "primitives/allocation.hpp"
#include "primitives/allocation_kind.hpp"
#include "primitives/id_type.hpp"

namespace nb = nanobind;
using namespace nb::literals;
using namespace omnimalloc;

namespace {

// __str__/__repr__ via the type's operator<<
template <typename T>
std::string stream_str(const T& value) {
  std::ostringstream ss;
  ss << value;
  return ss.str();
}

// nanobind's default caster renders std::vector as list; the Python surface
// wants tuples so scalar/tuple time points stay consistent under == and hash.
nb::object time_to_python(const TimePoint& time) {
  if (const auto* scalar = std::get_if<int64_t>(&time)) {
    return nb::int_(*scalar);
  }
  return nb::tuple(nb::cast(std::get<std::vector<int64_t>>(time)));
}

}  // namespace

NB_MODULE(_cpp, m) {
  // AllocationKind enum
  nb::enum_<AllocationKind>(m, "AllocationKind")
      .value("WORKSPACE", AllocationKind::WORKSPACE)
      .value("CONSTANT", AllocationKind::CONSTANT)
      .value("INPUT", AllocationKind::INPUT)
      .value("OUTPUT", AllocationKind::OUTPUT)
      .def_prop_ro("is_io", [](AllocationKind kind) { return is_io(kind); })
      .def("__str__", &stream_str<AllocationKind>)
      .def("__repr__", &stream_str<AllocationKind>)
      .def("__hash__", std::hash<AllocationKind>{});

  // Allocation class
  nb::class_<Allocation>(m, "Allocation")
      .def(nb::init<IdType, int64_t, TimePoint, TimePoint,
                    std::optional<int64_t>, std::optional<AllocationKind>>(),
           "id"_a, "size"_a, "start"_a, "end"_a, "offset"_a = nb::none(),
           "kind"_a = nb::none())
      .def_prop_ro("id", &Allocation::id, nb::rv_policy::copy)
      .def_prop_ro("size", &Allocation::size)
      .def_prop_ro(
          "start",
          [](const Allocation& a) { return time_to_python(a.start_time()); },
          nb::for_getter(
              nb::sig("def start(self, /) -> int | tuple[int, ...]")))
      .def_prop_ro(
          "end",
          [](const Allocation& a) { return time_to_python(a.end_time()); },
          nb::for_getter(nb::sig("def end(self, /) -> int | tuple[int, ...]")))
      .def_prop_ro("dim", &Allocation::dim)
      .def_prop_ro("offset", &Allocation::offset, nb::rv_policy::copy)
      .def_prop_ro("kind", &Allocation::kind, nb::rv_policy::copy)
      .def_prop_ro("is_allocated", &Allocation::is_allocated)
      .def_prop_ro("duration", &Allocation::duration)
      .def_prop_ro("height", &Allocation::height, nb::rv_policy::copy)
      .def_prop_ro("area", &Allocation::area)
      .def("conflicts_with", &Allocation::conflicts_with, "other"_a)
      .def("overlaps_spatially", &Allocation::overlaps_spatially, "other"_a)
      .def("overlaps", &Allocation::overlaps, "other"_a)
      .def("with_offset", &Allocation::with_offset, "offset"_a,
           nb::rv_policy::move)
      .def("__str__", &stream_str<Allocation>)
      .def("__repr__", &stream_str<Allocation>)
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
                                  time_to_python(a.start_time()),
                                  time_to_python(a.end_time()),
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

  m.def("conflicts", &conflicts, "allocations"_a, "work_budget"_a.none(),
        nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);
  m.def("conflict_degrees", &conflict_degrees, "allocations"_a,
        "work_budget"_a.none(), nb::call_guard<nb::gil_scoped_release>(),
        nb::rv_policy::move);
  m.def("try_linearize", &try_linearize, "allocations"_a,
        "work_budget"_a.none(), nb::call_guard<nb::gil_scoped_release>(),
        nb::rv_policy::move);
  m.def("antichain_pressure", &antichain_pressure, "allocations"_a,
        "work_budget"_a.none(), nb::call_guard<nb::gil_scoped_release>());
  m.def("closure_pressure", &closure_pressure, "allocations"_a,
        "closure_cap"_a.none(), nb::call_guard<nb::gil_scoped_release>());
  m.def("antichain_pressure_per_allocation", &antichain_pressure_per_allocation,
        "allocations"_a, "work_budget"_a.none(),
        nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);
  m.def("closure_pressure_per_allocation", &closure_pressure_per_allocation,
        "allocations"_a, "closure_cap"_a.none(),
        nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);
  m.def("placement_pressure_per_allocation", &placement_pressure_per_allocation,
        "allocations"_a, "work_budget"_a.none(),
        nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);
  m.def("first_fit_place", &first_fit_place, "allocations"_a,
        nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);

  // FirstFitPlacer class: resident placer for the order-search allocators.
  // Standing invariant for every gil_scoped_release-guarded method in this
  // module: it must be const and stateless-or-synchronized, because
  // releasing the GIL admits concurrent same-object calls; every guarded
  // path is also Python-free (IdType/TimePoint are std variants).
  nb::class_<FirstFitPlacer>(m, "FirstFitPlacer")
      .def(nb::init<std::vector<Allocation>>(), "allocations"_a,
           nb::call_guard<nb::gil_scoped_release>())
      .def("peak", &FirstFitPlacer::peak, "order"_a,
           nb::call_guard<nb::gil_scoped_release>())
      .def("place", &FirstFitPlacer::place, "order"_a,
           nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move)
      .def_prop_ro("conflicts", &FirstFitPlacer::conflicts,
                   nb::rv_policy::copy);

  // Placement functions: data types and flat functions cross the
  // boundary, nothing else. The config structs stay C++-internal; each
  // lambda constructs one from the flat parameters.
  m.def("best_fit_place", &best_fit_place, "allocations"_a,
        nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);
  m.def("omni_place", &omni_place, "allocations"_a, "linearize_budget"_a.none(),
        nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);
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
      nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);
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
      nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);
  m.def(
      "telamalloc_place",
      [](const std::vector<Allocation>& allocations, uint64_t seed,
         int max_backtracks, std::optional<double> timeout) {
        return telamalloc_place(allocations, {seed, max_backtracks, timeout});
      },
      "allocations"_a, "seed"_a, "max_backtracks"_a, "timeout"_a.none(),
      nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);

  // Partition class
  nb::class_<Partition>(m, "Partition")
      .def_static("from_allocations", &Partition::from_allocations,
                  "allocations"_a, nb::call_guard<nb::gil_scoped_release>(),
                  nb::rv_policy::move)
      .def("reorder", &Partition::reorder, "heuristic"_a,
           nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move)
      .def("with_bound", &Partition::with_bound, "bound"_a,
           nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move)
      .def_prop_ro("lower_bound", &Partition::lower_bound);

  // Solution class: placed allocations and their peak
  nb::class_<Solution>(m, "Solution")
      .def_ro("allocations", &Solution::allocations)
      .def_ro("peak", &Solution::peak);

  m.def("greedy_pack_portfolio", &greedy_pack_portfolio, "partition"_a,
        "heuristics"_a, "timeout"_a.none(), "num_threads"_a,
        nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);

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
      nb::call_guard<nb::gil_scoped_release>(), nb::rv_policy::move);
}
