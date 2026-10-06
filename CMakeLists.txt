cmake_minimum_required(VERSION 3.20)
project(DeribitCollector VERSION 1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release)
endif()

option(DERIBIT_COLLECTOR_TESTS "Build unit tests" OFF)

include(FetchContent)

# =============================================================================
# 1. System dependencies
# =============================================================================

# OPENSSL (TLS for wss:// and https://)
# Linux / Raspberry Pi: apt install libssl-dev
# Windows: set OPENSSL_ROOT_DIR (e.g. conda or vcpkg install)
find_package(OpenSSL REQUIRED)

# ZSTD (hourly output compression)
# Linux / Raspberry Pi: apt install libzstd-dev
# Windows: set ZSTD_ROOT (defaults to OPENSSL_ROOT_DIR, e.g. conda)
find_path(ZSTD_INCLUDE_DIR zstd.h
    HINTS ${ZSTD_ROOT}/include ${OPENSSL_ROOT_DIR}/include
)
find_library(ZSTD_LIBRARY
    NAMES zstd zstd_static libzstd
    HINTS ${ZSTD_ROOT}/lib ${OPENSSL_ROOT_DIR}/lib
)
if(NOT ZSTD_INCLUDE_DIR OR NOT ZSTD_LIBRARY)
    message(FATAL_ERROR "zstd not found; install libzstd-dev or set ZSTD_ROOT")
endif()

# Expose only the zstd headers: the zstd include dir may also contain
# unrelated packages (e.g. conda ships gtest) that would shadow fetched ones.
foreach(header zstd.h zstd_errors.h zdict.h)
    if(EXISTS ${ZSTD_INCLUDE_DIR}/${header})
        configure_file(${ZSTD_INCLUDE_DIR}/${header} ${CMAKE_BINARY_DIR}/zstd_include/${header} COPYONLY)
    endif()
endforeach()

add_library(zstd_lib INTERFACE)
target_include_directories(zstd_lib SYSTEM INTERFACE ${CMAKE_BINARY_DIR}/zstd_include)
target_link_libraries(zstd_lib INTERFACE ${ZSTD_LIBRARY})

# THREADS
find_package(Threads REQUIRED)

# =============================================================================
# 2. Fetched dependencies
# =============================================================================

# NLOHMANN JSON (REST/WebSocket message parsing and output records)
FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.11.3
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(nlohmann_json)

# IXWEBSOCKET (wss:// market data and https:// REST client, TLS via OpenSSL)
set(USE_TLS ON CACHE BOOL "" FORCE)
set(USE_OPEN_SSL ON CACHE BOOL "" FORCE)
set(USE_ZLIB OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    ixwebsocket
    GIT_REPOSITORY https://github.com/machinezone/IXWebSocket.git
    GIT_TAG        v11.4.5
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(ixwebsocket)

# GOOGLE TEST (only with -DDERIBIT_COLLECTOR_TESTS=ON)
if(DERIBIT_COLLECTOR_TESTS)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.15.2
        GIT_SHALLOW    TRUE
    )
    FetchContent_MakeAvailable(googletest)
endif()

# =============================================================================
# 3. Collector core (header-only sources in src/)
# =============================================================================
add_library(collector_core INTERFACE)

target_include_directories(collector_core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(collector_core INTERFACE nlohmann_json::nlohmann_json zstd_lib Threads::Threads)

if(WIN32)
    target_compile_definitions(collector_core INTERFACE NOMINMAX WIN32_LEAN_AND_MEAN)
endif()

# =============================================================================
# 4. Collector executable
# =============================================================================
add_executable(deribit_collector src/main.cpp)

target_link_libraries(deribit_collector PRIVATE collector_core ixwebsocket OpenSSL::SSL OpenSSL::Crypto)

if(WIN32)
    target_link_libraries(deribit_collector PRIVATE ws2_32 crypt32)
endif()

# =============================================================================
# 5. Unit tests
# =============================================================================
if(DERIBIT_COLLECTOR_TESTS)
    enable_testing()

    add_executable(collector_tests
        tests/collector_tests.cpp
        tests/edge_case_tests.cpp
        tests/writer_tests.cpp
    )
    target_link_libraries(collector_tests PRIVATE collector_core ixwebsocket OpenSSL::SSL OpenSSL::Crypto GTest::gtest_main)
    target_include_directories(collector_tests PRIVATE tests)
    # OpenSSL's include dir may hold another gtest (e.g. conda); fetched headers must win.
    target_include_directories(collector_tests BEFORE PRIVATE ${googletest_SOURCE_DIR}/googletest/include)

    if(WIN32)
        target_link_libraries(collector_tests PRIVATE ws2_32 crypt32)
    endif()

    include(GoogleTest)
    gtest_discover_tests(collector_tests)
endif()
