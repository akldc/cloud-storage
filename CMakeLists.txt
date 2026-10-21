cmake_minimum_required(VERSION 3.16)

project(mymuduo VERSION 0.2.0 LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "mymuduo currently requires Linux (epoll/eventfd/timerfd)")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

find_package(Threads REQUIRED)
include(CMakePackageConfigHelpers)

option(MYMUDUO_BUILD_HTTP "Build the optional HTTP protocol library" ON)
option(MYMUDUO_BUILD_EXAMPLES "Build network examples" ON)
option(MYMUDUO_BUILD_TESTS "Build network regression tests" ON)
option(MYMUDUO_BUILD_LEGACY_FILE_MANAGER
       "Build the legacy MySQL-backed web file manager" OFF)

add_subdirectory(base)
add_subdirectory(net)

if(MYMUDUO_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(MYMUDUO_BUILD_TESTS)
    include(CTest)
    if(BUILD_TESTING)
        add_subdirectory(tests)
    endif()
endif()

if(MYMUDUO_BUILD_LEGACY_FILE_MANAGER)
    if(NOT MYMUDUO_BUILD_HTTP)
        message(FATAL_ERROR "The legacy file manager requires MYMUDUO_BUILD_HTTP=ON")
    endif()
    add_subdirectory(legacy/file_manager)
endif()

install(EXPORT mymuduoTargets
        NAMESPACE mymuduo::
        DESTINATION lib/cmake/mymuduo)

configure_package_config_file(
    cmake/mymuduoConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/mymuduoConfig.cmake
    INSTALL_DESTINATION lib/cmake/mymuduo)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/mymuduoConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/mymuduoConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/mymuduoConfigVersion.cmake
    DESTINATION lib/cmake/mymuduo)
