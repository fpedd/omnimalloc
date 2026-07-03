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

execute_process(
  COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE
  OUTPUT_VARIABLE nanobind_ROOT)
find_package(nanobind CONFIG REQUIRED)

nanobind_add_module(_cpp NB_STATIC src/cpp/allocators/greedy.cpp
                    src/cpp/primitives/allocation.cpp src/cpp/bindings.cpp)

target_include_directories(_cpp PRIVATE src/cpp)

if(ENABLE_CLANG_TIDY)
  set_target_properties(_cpp PROPERTIES CXX_CLANG_TIDY "${CLANG_TIDY_PROGRAM}")
endif()
if(ENABLE_IWYU)
  set_target_properties(_cpp PROPERTIES CXX_INCLUDE_WHAT_YOU_USE
                                        "${IWYU_PROGRAM}")
endif()

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

install(TARGETS _cpp LIBRARY DESTINATION omnimalloc)
