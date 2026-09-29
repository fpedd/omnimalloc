#pragma once
// PROFILING PROTOTYPE ONLY
#include <chrono>
#include <cstdio>
#include <cstdlib>

namespace omnimalloc {
struct PTimer {
  const char* label;
  std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
  explicit PTimer(const char* l) : label(l) {}
  ~PTimer() {
    static const bool on = std::getenv("OM_TIMING") != nullptr;
    if (on) {
      const double ms = std::chrono::duration<double, std::milli>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
      std::fprintf(stderr, "    [cpp] %-32s %8.2f ms\n", label, ms);
    }
  }
};
}  // namespace omnimalloc
#define PT_CAT2(a, b) a##b
#define PT_CAT(a, b) PT_CAT2(a, b)
#define PTIME(label) ::omnimalloc::PTimer PT_CAT(pt_, __LINE__)(label)
