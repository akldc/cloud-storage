find_package(CURL REQUIRED)
add_library(object_client_core ObjectClient.cc UploadSupport.cc HttpChunkTransfer.cc
                               ClientUtils.cc)
target_include_directories(object_client_core PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(object_client_core PUBLIC coordinator_v1_proto object_store::common CURL::libcurl)
