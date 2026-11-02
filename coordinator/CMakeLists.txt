add_library(coordinator_control NodeRegistry.cc Placement.cc)
target_include_directories(coordinator_control PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(coordinator_control PUBLIC object_store::common)

add_library(coordinator_service CoordinatorService.cc)
target_include_directories(coordinator_service PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(coordinator_service PUBLIC coordinator_control coordinator_v1_proto)

add_library(coordinator_agent ../storage/node/CoordinatorAgent.cc)
target_include_directories(coordinator_agent PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(coordinator_agent PUBLIC coordinator_v1_proto object_store::common)
