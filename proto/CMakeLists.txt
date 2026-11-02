find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET grpc++)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
if(NOT GRPC_CPP_PLUGIN_EXECUTABLE)
    message(FATAL_ERROR "Coordinator V1 requires grpc_cpp_plugin on PATH")
endif()

execute_process(COMMAND ${Protobuf_PROTOC_EXECUTABLE} --version
    OUTPUT_VARIABLE PROTOC_VERSION_OUTPUT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE PROTOC_VERSION_RESULT)
if(NOT PROTOC_VERSION_RESULT EQUAL 0 OR
   NOT PROTOC_VERSION_OUTPUT MATCHES "^libprotoc ([0-9]+\\.[0-9]+\\.[0-9]+)$")
    message(FATAL_ERROR "Coordinator V1 cannot determine protoc version: ${PROTOC_VERSION_OUTPUT}")
endif()
if(NOT CMAKE_MATCH_1 VERSION_EQUAL Protobuf_VERSION)
    message(FATAL_ERROR
        "Coordinator V1 protoc ${CMAKE_MATCH_1} differs from libprotobuf ${Protobuf_VERSION}")
endif()
message(STATUS "Coordinator V1: Protobuf ${Protobuf_VERSION}, gRPC++ ${GRPCPP_VERSION}, plugin ${GRPC_CPP_PLUGIN_EXECUTABLE}")

set(COORDINATOR_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
set(COORDINATOR_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/coordinator_v1.proto)
set(COORDINATOR_GENERATED_FILES
    ${COORDINATOR_GENERATED_DIR}/coordinator_v1.pb.cc
    ${COORDINATOR_GENERATED_DIR}/coordinator_v1.pb.h
    ${COORDINATOR_GENERATED_DIR}/coordinator_v1.grpc.pb.cc
    ${COORDINATOR_GENERATED_DIR}/coordinator_v1.grpc.pb.h)

add_custom_command(OUTPUT ${COORDINATOR_GENERATED_FILES}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${COORDINATOR_GENERATED_DIR}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
        --cpp_out=${COORDINATOR_GENERATED_DIR}
        --grpc_out=${COORDINATOR_GENERATED_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
        ${COORDINATOR_PROTO}
    DEPENDS ${COORDINATOR_PROTO}
    VERBATIM)

add_library(coordinator_v1_proto
    ${COORDINATOR_GENERATED_DIR}/coordinator_v1.pb.cc
    ${COORDINATOR_GENERATED_DIR}/coordinator_v1.grpc.pb.cc)
target_include_directories(coordinator_v1_proto PUBLIC ${COORDINATOR_GENERATED_DIR})
target_link_libraries(coordinator_v1_proto PUBLIC protobuf::libprotobuf PkgConfig::GRPCPP)
