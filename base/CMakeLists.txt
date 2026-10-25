set(base_SRCS
    Timestamp.cc
    Date.cc
    TimeZone.cc
    Exception.cc
    CurrentThread.cc
    LogStream.cc
    Logging.cc
    CountDownLatch.cc
    ProcessInfo.cc
    FileUtil.cc
    LogFile.cc
    AsyncLogging.cc
    ThreadPool.cc
)

set(base_HEADERS
    Version.h
    Types.h
    StringPiece.h
    Timestamp.h
    Date.h
    TimeZone.h
    Exception.h
    CurrentThread.h
    LogStream.h
    Logging.h
    CountDownLatch.h
    ThreadPool.h
    WeakCallback.h
    ProcessInfo.h
    FileUtil.h
    LogFile.h
    AsyncLogging.h
    Endian.h
)

# 创建库并设置属性
add_library(mymuduo_base ${base_SRCS})
add_library(mymuduo::base ALIAS mymuduo_base)
set_target_properties(mymuduo_base PROPERTIES EXPORT_NAME base)
target_link_libraries(mymuduo_base PUBLIC Threads::Threads ${CMAKE_DL_LIBS})
target_include_directories(mymuduo_base PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include/mymuduo>)
target_compile_features(mymuduo_base PUBLIC cxx_std_17)

# 安装
install(TARGETS mymuduo_base EXPORT mymuduoTargets DESTINATION lib)
install(FILES ${base_HEADERS} DESTINATION include/mymuduo/base)
