cmake_minimum_required(VERSION 3.15)

project(ExternalSortUsingSeastar)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(SEASTAR_BUILD_DIR "" CACHE PATH "Path to the Seastar build directory (for example, SeaStar/build/release)")
if(SEASTAR_BUILD_DIR STREQUAL "")
  message(STATUS "SEASTAR_BUILD_DIR not passed to cmake - it will be downloaded from github.")
  # Pull seastar master from github if not available already
  include(FetchContent)
  FetchContent_Declare(
    seastar
    GIT_REPOSITORY https://github.com/scylladb/seastar.git
  )
  FetchContent_MakeAvailable(seastar)
else()
  # use the pre-built seastar
  message(STATUS "Using seastar from ${SEASTAR_BUILD_DIR}..")
  list(APPEND CMAKE_PREFIX_PATH "${SEASTAR_BUILD_DIR}")
  list(APPEND CMAKE_PREFIX_PATH "${SEASTAR_BUILD_DIR}/_cooking/installed")
  list(APPEND CMAKE_MODULE_PATH "${SEASTAR_BUILD_DIR}/../../cmake")
  find_package(Seastar REQUIRED)
  find_package(ucontext REQUIRED)
  find_package(Valgrind REQUIRED)
  find_package(SystemTap-SDT REQUIRED)
endif()

set(CMAKE_COMPILE_WARNING_AS_ERROR ON)
add_compile_options(-Wno-unknown-warning-option)

add_executable(external-sort main.cc external_sort.cc first_pass_service.cc second_pass_service.cc common.cc app_config.cc)

target_link_libraries(external-sort PRIVATE Seastar::seastar)
