set(net_SRCS
    Acceptor.cc
    Buffer.cc
    Channel.cc
    Connector.cc
    EventLoop.cc
    EventLoopThread.cc
    EventLoopThreadPool.cc
    InetAddress.cc
    Poller.cc
    poller/DefaultPoller.cc
    poller/PollPoller.cc
    poller/EPollPoller.cc
    Socket.cc
    SocketsOps.cc
    TcpConnection.cc
    TcpServer.cc
    TcpClient.cc
    Timer.cc
    TimerQueue.cc
)

set(net_HEADERS
    Acceptor.h
    Buffer.h
    Callbacks.h
    Channel.h
    Connector.h
    EventLoop.h
    EventLoopThread.h
    EventLoopThreadPool.h
    Endian.h
    InetAddress.h
    Poller.h
    Socket.h
    SocketsOps.h
    TcpConnection.h
    TcpServer.h
    TcpClient.h
    Timer.h
    TimerId.h
    TimerQueue.h
)

add_library(mymuduo_net ${net_SRCS})
add_library(mymuduo::net ALIAS mymuduo_net)
set_target_properties(mymuduo_net PROPERTIES EXPORT_NAME net)
target_link_libraries(mymuduo_net PUBLIC mymuduo::base)

# 设置包含目录
target_include_directories(mymuduo_net PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include/mymuduo>)
target_compile_features(mymuduo_net PUBLIC cxx_std_17)

# 安装
install(TARGETS mymuduo_net EXPORT mymuduoTargets DESTINATION lib)
install(FILES ${net_HEADERS} DESTINATION include/mymuduo/net)
install(FILES poller/PollPoller.h poller/EPollPoller.h
        DESTINATION include/mymuduo/net/poller)

if(MYMUDUO_BUILD_HTTP)
    add_library(mymuduo_http HttpServer.cc HttpContext.cc)
    add_library(mymuduo::http ALIAS mymuduo_http)
    set_target_properties(mymuduo_http PROPERTIES EXPORT_NAME http)
    target_link_libraries(mymuduo_http PUBLIC mymuduo::net)
    target_include_directories(mymuduo_http PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include/mymuduo>)
    target_compile_features(mymuduo_http PUBLIC cxx_std_17)
    install(TARGETS mymuduo_http EXPORT mymuduoTargets DESTINATION lib)
    install(FILES HttpServer.h HttpContext.h HttpRequest.h HttpResponse.h
            DESTINATION include/mymuduo/net)
endif()
