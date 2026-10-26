add_library(chunk_store
    ChunkStore.cc
    ChunkWriter.cc
    ChunkReader.cc
)
add_library(object_store::chunk_store ALIAS chunk_store)

target_include_directories(chunk_store PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include/mymuduo>)
target_compile_features(chunk_store PUBLIC cxx_std_17)
target_link_libraries(chunk_store PUBLIC object_store::common)

