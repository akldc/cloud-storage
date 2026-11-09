function(add_mymuduo_test name)
    add_executable(${name} ${name}.cc)
    target_link_libraries(${name} PRIVATE mymuduo::net)
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 15)
endfunction()

add_mymuduo_test(buffer_test)
add_mymuduo_test(thread_pool_test)
add_mymuduo_test(count_down_latch_test)
add_mymuduo_test(event_loop_thread_test)
add_mymuduo_test(tcp_server_test)

add_executable(chunk_store_test storage/chunk_store_test.cc)
target_link_libraries(chunk_store_test PRIVATE object_store::chunk_store)
target_link_options(chunk_store_test PRIVATE -Wl,--wrap=fsync)
add_test(NAME chunk_store_test COMMAND chunk_store_test)
set_tests_properties(chunk_store_test PROPERTIES TIMEOUT 30)

if(MYMUDUO_BUILD_COORDINATOR)
    if(MYMUDUO_BUILD_OBJECTS)
        add_executable(object_catalog_test object_catalog_test.cc)
        target_link_libraries(object_catalog_test PRIVATE coordinator_object_catalog coordinator_control)
        add_test(NAME object_catalog_test COMMAND object_catalog_test)
        set_tests_properties(object_catalog_test PROPERTIES TIMEOUT 30)
        add_executable(object_grpc_test object_grpc_test.cc)
        target_link_libraries(object_grpc_test PRIVATE coordinator_service)
        add_test(NAME object_grpc_test COMMAND object_grpc_test)
        set_tests_properties(object_grpc_test PROPERTIES TIMEOUT 30)
        add_executable(chunk_confirm_test chunk_confirm_test.cc)
        target_link_libraries(chunk_confirm_test PRIVATE coordinator_service)
        add_test(NAME chunk_confirm_test COMMAND chunk_confirm_test)
        set_tests_properties(chunk_confirm_test PROPERTIES TIMEOUT 30)
    endif()
    add_executable(coordinator_logic_test coordinator_logic_test.cc)
    target_link_libraries(coordinator_logic_test PRIVATE coordinator_control)
    add_test(NAME coordinator_logic_test COMMAND coordinator_logic_test)
    set_tests_properties(coordinator_logic_test PROPERTIES TIMEOUT 15)

    add_executable(coordinator_grpc_test coordinator_grpc_test.cc)
    target_link_libraries(coordinator_grpc_test PRIVATE coordinator_service)
    add_test(NAME coordinator_grpc_test COMMAND coordinator_grpc_test)
    set_tests_properties(coordinator_grpc_test PROPERTIES TIMEOUT 30)

    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    add_test(NAME coordinator_process_test
        COMMAND ${Python3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/integration/coordinator_process_test.py
                $<TARGET_FILE:coordinator>)
    set_tests_properties(coordinator_process_test PROPERTIES TIMEOUT 15)

    if(MYMUDUO_BUILD_HTTP)
        add_executable(coordinator_agent_test coordinator_agent_test.cc)
        target_link_libraries(coordinator_agent_test PRIVATE coordinator_agent coordinator_control)
        add_test(NAME coordinator_agent_test COMMAND coordinator_agent_test)
        set_tests_properties(coordinator_agent_test PROPERTIES TIMEOUT 15)

        add_executable(coordinator_probe coordinator_probe.cc)
        target_link_libraries(coordinator_probe PRIVATE coordinator_v1_proto)
        add_test(NAME storage_node_coordinator_test
            COMMAND ${Python3_EXECUTABLE}
                    ${CMAKE_CURRENT_SOURCE_DIR}/integration/storage_node_coordinator_test.py
                    $<TARGET_FILE:coordinator>
                    $<TARGET_FILE:storage_node>
                    $<TARGET_FILE:coordinator_probe>)
        set_tests_properties(storage_node_coordinator_test PROPERTIES TIMEOUT 40)

        add_test(NAME coordinator_multi_process_test
            COMMAND ${Python3_EXECUTABLE}
                    ${CMAKE_CURRENT_SOURCE_DIR}/integration/coordinator_multi_process_test.py
                    $<TARGET_FILE:coordinator>
                    $<TARGET_FILE:storage_node>
                    $<TARGET_FILE:coordinator_probe>)
        set_tests_properties(coordinator_multi_process_test PROPERTIES TIMEOUT 90)
        if(MYMUDUO_BUILD_OBJECTS)
            add_test(NAME object_small_file_test
                COMMAND ${Python3_EXECUTABLE}
                        ${CMAKE_CURRENT_SOURCE_DIR}/integration/object_small_file_test.py
                        $<TARGET_FILE:coordinator>
                        $<TARGET_FILE:storage_node>
                        $<TARGET_FILE:coordinator_probe>
                        $<TARGET_FILE:object_client>)
            set_tests_properties(object_small_file_test PROPERTIES TIMEOUT 60)
            add_test(NAME object_resume_test
                COMMAND ${Python3_EXECUTABLE}
                        ${CMAKE_CURRENT_SOURCE_DIR}/integration/object_resume_test.py
                        $<TARGET_FILE:coordinator>
                        $<TARGET_FILE:storage_node>
                        $<TARGET_FILE:coordinator_probe>
                        $<TARGET_FILE:object_client>)
            set_tests_properties(object_resume_test PROPERTIES TIMEOUT 180)
            add_test(NAME object_multi_node_test
                COMMAND ${Python3_EXECUTABLE}
                        ${CMAKE_CURRENT_SOURCE_DIR}/integration/object_multi_node_test.py
                        $<TARGET_FILE:coordinator>
                        $<TARGET_FILE:storage_node>
                        $<TARGET_FILE:coordinator_probe>
                        $<TARGET_FILE:object_client>)
            set_tests_properties(object_multi_node_test PROPERTIES TIMEOUT 180)
            add_test(NAME object_lost_reply_test
                COMMAND ${Python3_EXECUTABLE}
                        ${CMAKE_CURRENT_SOURCE_DIR}/integration/object_lost_reply_test.py
                        $<TARGET_FILE:coordinator>
                        $<TARGET_FILE:storage_node>
                        $<TARGET_FILE:coordinator_probe>
                        $<TARGET_FILE:object_client>)
            set_tests_properties(object_lost_reply_test PROPERTIES TIMEOUT 60)
        endif()
    endif()
endif()

if(MYMUDUO_BUILD_HTTP)
    add_executable(http_context_test http_context_test.cc)
    target_link_libraries(http_context_test PRIVATE mymuduo::http)
    add_test(NAME http_context_test COMMAND http_context_test)

    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    add_test(NAME storage_node_integration_test
        COMMAND ${Python3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/integration/storage_node_test.py
                $<TARGET_FILE:storage_node>)
    set_tests_properties(storage_node_integration_test PROPERTIES TIMEOUT 180)
endif()
