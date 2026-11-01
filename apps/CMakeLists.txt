add_executable(storage_node storage_node.cc)
target_link_libraries(storage_node PRIVATE object_store::node)
