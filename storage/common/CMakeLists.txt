find_package(OpenSSL REQUIRED COMPONENTS Crypto)

add_library(object_store_common
    Checksum.cc
)
add_library(object_store::common ALIAS object_store_common)

target_include_directories(object_store_common PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include/mymuduo>)
target_compile_features(object_store_common PUBLIC cxx_std_17)
target_link_libraries(object_store_common PUBLIC OpenSSL::Crypto)

