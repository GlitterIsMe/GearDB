# Copyright 2017 The LevelDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

cmake_minimum_required(VERSION 3.9)
project(geardb VERSION 1.0 LANGUAGES C CXX)

# C standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_C_STANDARD)
    # This project can use C11, but will gracefully decay down to C89.
    set(CMAKE_C_STANDARD 11)
    set(CMAKE_C_STANDARD_REQUIRED OFF)
    set(CMAKE_C_EXTENSIONS OFF)
endif(NOT CMAKE_C_STANDARD)

# C++ standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_CXX_STANDARD)
    # This project requires C++11.
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

#option(GEARDB_BUILD_TESTS "Build GearDB's unit tests" ON)
option(GEARDB_BUILD_BENCHMARKS "Build GearDB's benchmarks" ON)


include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

include(CheckCXXSymbolExists)
# Using check_cxx_symbol_exists() instead of check_c_symbol_exists() because
# we're including the header from C++, and feature detection should use the same
# compiler language that the project will use later. Principles aside, some
# versions of do not expose fdatasync() in <unistd.h> in standard C mode
# (-std=c11), but do expose the function in standard C++ mode (-std=c++11).
check_cxx_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)
check_cxx_symbol_exists(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)

if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    # Disable C++ exceptions.
    string(REGEX REPLACE "/EH[a-z]+" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHs-c-")
    add_definitions(-D_HAS_EXCEPTIONS=0)

    # Disable RTTI.
    string(REGEX REPLACE "/GR" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /GR-")
else(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    # Enable strict prototype warnings for C code in clang and gcc.
    if(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wstrict-prototypes")
    endif(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")

    # Disable C++ exceptions.
    string(REGEX REPLACE "-fexceptions" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

    # Disable RTTI.
    string(REGEX REPLACE "-frtti" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")
endif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")

# Test whether -Wthread-safety is available. See
# https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety HAVE_CLANG_THREAD_SAFETY)

# Used by googletest.
check_cxx_compiler_flag(-Wno-missing-field-initializers
        LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)

include(CheckCXXSourceCompiles)

# Test whether C++17 __has_include is available.
check_cxx_source_compiles("
#if defined(__has_include) &&  __has_include(<string>)
#include <string>
#endif
int main() { std::string str; return 0; }
" HAVE_CXX17_HAS_INCLUDE)

set(GEARDB_PUBLIC_INCLUDE_DIR "include/leveldb")

include_directories(
        "${PROJECT_BINARY_DIR}/include"
        "${PROJECT_BINARY_DIR}/port"
        "."
)

if(BUILD_SHARED_LIBS)
    # Only export LEVELDB_EXPORT symbols from the shared library.
    add_compile_options(-fvisibility=hidden)
endif(BUILD_SHARED_LIBS)

add_definitions(-DMETRICS=1)
add_definitions(-DLEVELDB_PLATFORM_POSIX)
# Must be included before CMAKE_INSTALL_INCLUDEDIR is used.
include(GNUInstallDirs)

add_library(geardb "")
cmake_policy(SET CMP0076 OLD)
target_sources(geardb
        PRIVATE
        "db/autocompact_test.cc"
        "db/builder.cc"
        "db/builder.h"
        "db/c.cc"
        "db/db_impl.cc"
        "db/db_impl.h"
        "db/db_iter.cc"
        "db/db_iter.h"
        "db/dbformat.cc"
        "db/dbformat.h"
        "db/dumpfile.cc"
        "db/filename.cc"
        "db/filename.h"
        "db/log_format.h"
        "db/log_reader.cc"
        "db/log_reader.h"
        "db/log_writer.cc"
        "db/log_writer.h"
        "db/memtable.cc"
        "db/memtable.h"
        "db/repair.cc"
        "db/skiplist.h"
        "db/snapshot.h"
        "db/table_cache.cc"
        "db/table_cache.h"
        "db/version_edit.cc"
        "db/version_edit.h"
        "db/version_set.cc"
        "db/version_set.h"
        "db/write_batch_internal.h"
        "db/write_batch.cc"
        "port/atomic_pointer.h"
        "port/port.h"
        "port/port_posix.cc"
        "port/port_posix.h"
        "port/thread_annotations.h"
        "table/block_builder.cc"
        "table/block_builder.h"
        "table/block.cc"
        "table/block.h"
        "table/filter_block.cc"
        "table/filter_block.h"
        "table/format.cc"
        "table/format.h"
        "table/iterator_wrapper.h"
        "table/iterator.cc"
        "table/merger.cc"
        "table/merger.h"
        "table/table_builder.cc"
        "table/table.cc"
        "table/two_level_iterator.cc"
        "table/two_level_iterator.h"
        "util/arena.cc"
        "util/arena.h"
        "util/bloom.cc"
        "util/cache.cc"
        "util/coding.cc"
        "util/coding.h"
        "util/comparator.cc"
        "util/crc32c.cc"
        "util/crc32c.h"
        "util/env.cc"
        "util/env_posix.cc"
        "util/filter_policy.cc"
        "util/hash.cc"
        "util/hash.h"
        "util/histogram.cc"
        "util/histogram.h"
        "util/logging.cc"
        "util/logging.h"
        "util/posix_logger.h"
        "util/mutexlock.h"
        "util/options.cc"
        "util/random.h"
        "util/status.cc"
        "hm/BitMap.cc"
        "hm/BitMap.h"
        "hm/container.cc"
        "hm/container.h"
        "hm/get_manager.cc"
        "hm/get_manager.h"
        "hm/hm_manager.cc"
        "hm/hm_manager.h"
        "hm/hm_status.h"
        "hm/my_log.cc"
        "hm/my_log.h"
        )

#target_sources(geardb
#        PRIVATE
#        "helpers/memenv/memenv.cc"
#        "helpers/memenv/memenv.h"
#        )

target_include_directories(geardb
        PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        )

set_target_properties(geardb
        PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

if(HAVE_CRC32C)
    target_link_libraries(geardb crc32c)
endif(HAVE_CRC32C)
if(HAVE_SNAPPY)
    target_link_libraries(geardb snappy)
endif(HAVE_SNAPPY)
if(HAVE_TCMALLOC)
    target_link_libraries(geardb tcmalloc)
endif(HAVE_TCMALLOC)

# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(geardb Threads::Threads)

add_executable(leveldbutil
        "db/leveldbutil.cc"
        )
target_link_libraries(leveldbutil geardb)

if(GEARDB_BUILD_BENCHMARKS)
    function(geardb_benchmark bench_file)
        get_filename_component(bench_target_name "${bench_file}" NAME_WE)

        add_executable("${bench_target_name}" "")
        target_sources("${bench_target_name}"
                PRIVATE
                "util/histogram.cc"
                "util/histogram.h"
                "util/testutil.cc"
                "util/testutil.h"

                "${bench_file}"
                )
        target_link_libraries("${bench_target_name}" geardb)
    endfunction(geardb_benchmark)

    if(NOT BUILD_SHARED_LIBS)
        geardb_benchmark("db/db_bench.cc")
    endif(NOT BUILD_SHARED_LIBS)

    # check_library_exists is insufficient here because the library names have
    # different manglings when compiled with clang or gcc, at least when installed
    # with Homebrew on Mac.
endif(GEARDB_BUILD_BENCHMARKS)

