#include "storage/node/CoordinatorAgent.h"

#include "coordinator_v1.grpc.pb.h"
#include "storage/common/Types.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <iostream>
#include <optional>
#include <random>
#include <stdexcept>
#include <utility>

#include <grpcpp/grpcpp.h>

namespace object_storage {
namespace {

namespace rpc = ::object_storage::control::v1;
using Clock = std::chrono::steady_clock;

std::string NewInstanceId()
{
    std::random_device random;
    std::array<unsigned char, 16> bytes{};
    for (auto& byte : bytes) byte = static_cast<unsigned char>(random());
    bytes[6] = static_cast<unsigned char>((bytes[6] & 0x0f) | 0x40);
    bytes[8] = static_cast<unsigned char>((bytes[8] & 0x3f) | 0x80);
    constexpr char hex[] = "0123456789abcdef";
    std::string id;
    id.reserve(36);
    for (size_t i = 0; i < bytes.size(); ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) id.push_back('-');
        id.push_back(hex[bytes[i] >> 4]);
        id.push_back(hex[bytes[i] & 0x0f]);
    }
    return id;
}

// 统一调用 gRPC 方法，设置超时，并返回状态
template <typename Request, typename Reply, typename Method>
grpc::Status Call(rpc::Coordinator::Stub* stub, Method method,
                  const Request& request, Reply* reply, uint32_t deadlineMs)
{
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(deadlineMs));
    return (stub->*method)(&context, request, reply);
}

bool Retryable(grpc::StatusCode code)
{
    return code == grpc::UNAVAILABLE || code == grpc::DEADLINE_EXCEEDED ||
           code == grpc::RESOURCE_EXHAUSTED;
}

// 返回指数退避时间，带随机抖动
std::chrono::milliseconds Backoff(unsigned failures, std::mt19937* random)
{
    const unsigned shift = std::min(failures, 3u);
    const int base = std::min(4000, 500 << shift);
    std::uniform_int_distribution<int> jitter(0, base / 4);
    return std::chrono::milliseconds(base + jitter(*random));
}

}  // namespace

CoordinatorAgent::CoordinatorAgent(Options options)
    : options_(std::move(options)), instanceId_(NewInstanceId())
{
    if (options_.address.empty() || !IsCanonicalId(options_.nodeId) ||
        options_.advertisedHost.empty() || options_.advertisedHost.size() > 253 ||
        options_.advertisedPort == 0 || options_.rpcDeadlineMs == 0 ||
        options_.rpcDeadlineMs > coordinator::kMaxRpcDeadlineMs ||
        options_.conflictWaitMs < options_.rpcDeadlineMs ||
        options_.conflictWaitMs > coordinator::kMaxConflictWaitMs) {
        throw std::invalid_argument("invalid Coordinator agent options");
    }
}

CoordinatorAgent::~CoordinatorAgent() { Stop(); }

void CoordinatorAgent::Start()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (started_) return;
    started_ = true;
    worker_ = std::thread(&CoordinatorAgent::Run, this);
}

void CoordinatorAgent::Stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    stopped_.notify_all();  // 唤醒 Run() 中的 WaitFor()，让它尽快退出循环
    if (worker_.joinable()) worker_.join();
}

CoordinatorAgent::State CoordinatorAgent::state() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return state_;
}

std::string CoordinatorAgent::lastError() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return lastError_;
}

bool CoordinatorAgent::Stopping() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return stopping_;
}

// 等待到期或者被 Stop() 唤醒，返回 true 表示被唤醒
bool CoordinatorAgent::WaitFor(std::chrono::milliseconds delay)
{
    std::unique_lock<std::mutex> lock(mutex_);
    return stopped_.wait_for(lock, delay, [this] { return stopping_; });
}

void CoordinatorAgent::SetState(State state, std::string error)
{
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = state;
    lastError_ = std::move(error);
}

void CoordinatorAgent::Run()
{
    // 创建 gRPC channel 和 stub
    grpc::ChannelArguments channelArgs;
    channelArgs.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    auto channel = grpc::CreateCustomChannel(options_.address,
                                             grpc::InsecureChannelCredentials(), channelArgs);
    auto stub = rpc::Coordinator::NewStub(channel);

    // 构造注册和心跳请求
    rpc::RegisterNodeRequest registration;
    registration.set_node_id(options_.nodeId);
    registration.set_instance_id(instanceId_);
    registration.set_host(options_.advertisedHost);
    registration.set_port(options_.advertisedPort);
    rpc::HeartbeatRequest heartbeat;
    heartbeat.set_node_id(options_.nodeId);
    heartbeat.set_instance_id(instanceId_);

    std::mt19937 random(std::random_device{}());
    std::optional<Clock::time_point> conflictSince;
    bool registered = false;
    bool attemptedRegistration = false;
    uint32_t heartbeatMs = coordinator::kDefaultHeartbeatIntervalMs;
    unsigned failures = 0;
    while (!Stopping()) {
        if (!registered) {
            attemptedRegistration = true;
            rpc::RegisterNodeReply reply;
            const auto status = Call(stub.get(), &rpc::Coordinator::Stub::RegisterNode,
                                     registration, &reply, options_.rpcDeadlineMs);
            if (Stopping()) break;
            if (status.ok()) {  // 注册成功
                if (!coordinator::ValidTiming(reply.heartbeat_interval_ms(),
                                              reply.lease_timeout_ms(), options_.rpcDeadlineMs)) {
                    SetState(State::kTerminalError, "invalid heartbeat/lease values in reply");
                    std::cerr << "coordinator agent: invalid heartbeat/lease values in reply\n";
                    break;
                }
                heartbeatMs = reply.heartbeat_interval_ms();
                registered = true;
                failures = 0;
                conflictSince.reset();
                SetState(State::kRegistered);
                if (WaitFor(std::chrono::milliseconds(heartbeatMs))) break;  // 等待一个心跳周期
                continue;
            }
            if (status.error_code() == grpc::ALREADY_EXISTS) {
                const auto now = Clock::now();
                if (!conflictSince) conflictSince = now;
                if (now - *conflictSince >=
                    std::chrono::milliseconds(options_.conflictWaitMs)) {
                    SetState(State::kTerminalConflict, "registration conflict wait expired");
                    std::cerr << "coordinator agent: registration conflict wait expired\n";
                    break;
                }
                SetState(State::kRetrying, status.error_message());
                const auto remaining = std::chrono::milliseconds(options_.conflictWaitMs) -
                                       std::chrono::duration_cast<std::chrono::milliseconds>(
                                           now - *conflictSince);
                if (WaitFor(std::min(remaining, std::chrono::milliseconds(1000)))) break;
            } else if (Retryable(status.error_code())) {
                SetState(State::kRetrying, status.error_message());
                if (WaitFor(Backoff(failures++, &random))) break;
            } else {
                SetState(status.error_code() == grpc::FAILED_PRECONDITION  // 
                             ? State::kTerminalConflict : State::kTerminalError,
                         status.error_message());
                std::cerr << "coordinator agent: registration failed permanently: "
                          << status.error_message() << '\n';
                break;
            }
            continue;
        }

        // 已注册，发送心跳
        rpc::HeartbeatReply reply;
        const auto status = Call(stub.get(), &rpc::Coordinator::Stub::Heartbeat,
                                 heartbeat, &reply, options_.rpcDeadlineMs);
        if (Stopping()) break;
        if (status.ok()) {
            failures = 0;
            SetState(State::kRegistered);
            if (WaitFor(std::chrono::milliseconds(heartbeatMs))) break;
        } else if (status.error_code() == grpc::NOT_FOUND) {  // 注册丢失，必须重新注册
            registered = false;
            failures = 0;
            SetState(State::kRetrying, "registration lost; registering again");
        } else if (Retryable(status.error_code())) {
            SetState(State::kRetrying, status.error_message());
            if (WaitFor(Backoff(failures++, &random))) break;
        } else {
            SetState(status.error_code() == grpc::FAILED_PRECONDITION
                         ? State::kTerminalConflict : State::kTerminalError,
                     status.error_message());
            std::cerr << "coordinator agent: heartbeat failed permanently: "
                      << status.error_message() << '\n';
            break;
        }
    }

    if (attemptedRegistration && state() != State::kTerminalConflict) {
        rpc::UnregisterNodeRequest request;
        request.set_node_id(options_.nodeId);
        request.set_instance_id(instanceId_);
        rpc::UnregisterNodeReply reply;
        const auto status = Call(stub.get(), &rpc::Coordinator::Stub::UnregisterNode,
                                 request, &reply, options_.rpcDeadlineMs);
        if (!status.ok() && status.error_code() != grpc::NOT_FOUND) {
            std::cerr << "coordinator agent: unregister failed: "
                      << status.error_message() << '\n';
        }
    }
    if (Stopping() && state() != State::kTerminalConflict && state() != State::kTerminalError) {
        SetState(State::kStopped);
    }
}

}  // namespace object_storage
