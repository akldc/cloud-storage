set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


find_path(NLOHMANN_JSON_INCLUDE_DIR nlohmann/json.hpp REQUIRED)
find_library(MYSQLCLIENT_LIBRARY mysqlclient REQUIRED)

add_executable(http_upload http_upload.cc)
target_include_directories(http_upload PRIVATE ${NLOHMANN_JSON_INCLUDE_DIR})
target_link_libraries(http_upload PRIVATE mymuduo::http ${MYSQLCLIENT_LIBRARY} stdc++fs)

add_executable(route_test route_test.cc)
