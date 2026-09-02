cmake_minimum_required(VERSION 3.18...3.30)

# Version is sourced from setuptools-scm via scikit-build-core (git tags). This
# fallback only applies for standalone CMake calls, not using the build backend.
if(NOT DEFINED SKBUILD_PROJECT_VERSION)
  set(SKBUILD_PROJECT_VERSION "0.0.0")
endif()

project(
  OmniMalloc
  VERSION ${SKBUILD_PROJECT_VERSION}
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(ENABLE_CLANG_TIDY "Run clang-tidy with the compiler" OFF)
if(ENABLE_CLANG_TIDY)
  find_program(CLANG_TIDY_PROGRAM NAMES clang-tidy REQUIRED)
endif()

option(ENABLE_IWYU "Run include-what-you-use with the compiler" OFF)
if(ENABLE_IWYU)
  find_program(IWYU_PROGRAM NAMES include-what-you-use iwyu REQUIRED)
endif()

find_package(Python 3.10 REQUIRED COMPONENTS Interpreter Development.Module)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

execute_process(
  COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE
  OUTPUT_VARIABLE nanobind_ROOT)
find_package(nanobind CONFIG REQUIRED)

nanobind_add_module(
  _cpp
  NB_STATIC
  NOMINSIZE
  NB_SUPPRESS_WARNINGS
  src/cpp/allocators/best_fit.cpp
  src/cpp/allocators/first_fit.cpp
  src/cpp/allocators/local_search.cpp
  src/cpp/allocators/omni.cpp
  src/cpp/allocators/simulated_annealing.cpp
  src/cpp/allocators/supermalloc.cpp
  src/cpp/allocators/tabu_search.cpp
  src/cpp/allocators/telamalloc.cpp
  src/cpp/analysis/antichain.cpp
  src/cpp/analysis/closure.cpp
  src/cpp/analysis/collision.cpp
  src/cpp/analysis/conflicts.cpp
  src/cpp/analysis/linearize.cpp
  src/cpp/analysis/placement.cpp
  src/cpp/primitives/allocation.cpp
  src/cpp/bindings.cpp)

target_include_directories(_cpp PRIVATE src/cpp)
target_link_libraries(_cpp PRIVATE Threads::Threads)

# 64-bit atomics compile to libatomic calls on targets without native
# double-word atomics (32-bit ARM as Raspberry Pi OS builds it, among others).
# Without the link the module still links, since extension modules leave symbols
# undefined by design, but fails to load: undefined __atomic_*_8.
include(CheckCXXSourceCompiles)
set(INT64_ATOMICS_SOURCE
    "
#include <atomic>
#include <cstdint>
int main() {
  std::atomic<std::int64_t> value{0};
  std::int64_t expected = 0;
  value.fetch_add(1);
  value.compare_exchange_weak(expected, 2);
  return static_cast<int>(value.load());
}
")
check_cxx_source_compiles("${INT64_ATOMICS_SOURCE}" HAVE_NATIVE_INT64_ATOMICS)
if(NOT HAVE_NATIVE_INT64_ATOMICS)
  set(CMAKE_REQUIRED_LIBRARIES atomic)
  check_cxx_source_compiles("${INT64_ATOMICS_SOURCE}"
                            HAVE_LIBATOMIC_INT64_ATOMICS)
  unset(CMAKE_REQUIRED_LIBRARIES)
  if(NOT HAVE_LIBATOMIC_INT64_ATOMICS)
    message(FATAL_ERROR "64-bit atomics require libatomic, which was not found")
  endif()
  target_link_libraries(_cpp PRIVATE atomic)
endif()

option(WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)

if(MSVC)
  target_compile_options(_cpp PRIVATE /W4 $<$<BOOL:${WARNINGS_AS_ERRORS}>:/WX>)
else()
  target_compile_options(
    _cpp
    PRIVATE -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wno-sign-conversion
            -Wshadow
            -Wold-style-cast
            -Wcast-qual
            -Wcast-align
            $<$<BOOL:${WARNINGS_AS_ERRORS}>:-Werror>)
endif()

if(ENABLE_CLANG_TIDY)
  set_target_properties(_cpp PROPERTIES CXX_CLANG_TIDY "${CLANG_TIDY_PROGRAM}")
endif()
if(ENABLE_IWYU)
  set_target_properties(_cpp PROPERTIES CXX_INCLUDE_WHAT_YOU_USE
                                        "${IWYU_PROGRAM}")
endif()

option(ENABLE_STUB "Regenerate _cpp.pyi from the built module" ON)
if(ENABLE_STUB)
  nanobind_add_stub(
    _cpp_stub
    MODULE
    _cpp
    OUTPUT
    ${CMAKE_CURRENT_SOURCE_DIR}/src/python/omnimalloc/_cpp.pyi
    PYTHON_PATH
    $<TARGET_FILE_DIR:_cpp>
    DEPENDS
    _cpp)
endif()

install(TARGETS _cpp LIBRARY DESTINATION omnimalloc)
