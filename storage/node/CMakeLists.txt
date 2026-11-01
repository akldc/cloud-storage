add_library(storage_node_service StorageNode.cc StorageNodeHttp.cc)
add_library(object_store::node ALIAS storage_node_service)
target_include_directories(storage_node_service PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include/mymuduo>)
target_compile_features(storage_node_service PUBLIC cxx_std_17)
target_link_libraries(storage_node_service PUBLIC
    object_store::chunk_store mymuduo::http)
