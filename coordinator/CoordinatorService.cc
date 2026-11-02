#include "coordinator/CoordinatorService.h"

#include "coordinator/Placement.h"
#include "storage/common/Types.h"

namespace object_storage::coordinator {
namespace {

grpc::Status ToGrpc(const Status& status)
{
    switch (status.code) {
    case Code::kOk: return grpc::Status::OK;
    case Code::kInvalidArgument: return {grpc::StatusCode::INVALID_ARGUMENT, status.message};
    case Code::kNotFound: return {grpc::StatusCode::NOT_FOUND, status.message};
    case Code::kAlreadyExists: return {grpc::StatusCode::ALREADY_EXISTS, status.message};
    case Code::kFailedPrecondition:
        return {grpc::StatusCode::FAILED_PRECONDITION, status.message};
    case Code::kResourceExhausted:
        return {grpc::StatusCode::RESOURCE_EXHAUSTED, status.message};
    case Code::kInternal: return {grpc::StatusCode::INTERNAL, status.message};
    }
    return {grpc::StatusCode::INTERNAL, "unknown coordinator status"};
}

void FillNode(const NodeInfo& source, control::NodeInfo* target)
{
    target->set_node_id(source.nodeId);
    target->set_instance_id(source.instanceId);
    target->set_host(source.host);
    target->set_port(source.port);
    target->set_state(source.online ? control::NODE_ONLINE : control::NODE_OFFLINE);
    target->set_last_seen_unix_ms(source.lastSeenUnixMs);
}

grpc::Status InvalidId()
{
    return {grpc::StatusCode::INVALID_ARGUMENT, "ID must be a canonical lowercase UUID"};
}

}  // namespace

grpc::Status CoordinatorService::RegisterNode(grpc::ServerContext*,
                                              const control::RegisterNodeRequest* request,
                                              control::RegisterNodeReply* reply)
{
    if (!IsCanonicalId(request->node_id()) || !IsCanonicalId(request->instance_id())) {
        return InvalidId();
    }
    if (request->host().empty() || request->port() == 0 || request->port() > 65535) {
        return {grpc::StatusCode::INVALID_ARGUMENT, "invalid advertised host or port"};
    }
    auto result = registry_->Register(request->node_id(), request->instance_id(),
                                      request->host(), request->port(), Clock::now());
    if (!result.ok()) return ToGrpc(result);
    reply->set_heartbeat_interval_ms(heartbeatMs_);  // 向客户端发送心跳间隔和租约超时
    reply->set_lease_timeout_ms(leaseMs_);
    return grpc::Status::OK;
}

grpc::Status CoordinatorService::Heartbeat(grpc::ServerContext*,
                                           const control::HeartbeatRequest* request,
                                           control::HeartbeatReply*)
{
    if (!IsCanonicalId(request->node_id()) || !IsCanonicalId(request->instance_id())) {
        return InvalidId();
    }
    return ToGrpc(registry_->Heartbeat(request->node_id(), request->instance_id(), Clock::now()));  // 更新节点的最后心跳时间
}

grpc::Status CoordinatorService::UnregisterNode(grpc::ServerContext*,
                                                 const control::UnregisterNodeRequest* request,
                                                 control::UnregisterNodeReply*)
{
    if (!IsCanonicalId(request->node_id()) || !IsCanonicalId(request->instance_id())) {
        return InvalidId();
    }
    return ToGrpc(registry_->Unregister(request->node_id(), request->instance_id()));
}

grpc::Status CoordinatorService::GetNode(grpc::ServerContext*,
                                         const control::GetNodeRequest* request,
                                         control::NodeInfo* reply)
{
    if (!IsCanonicalId(request->node_id())) return InvalidId();
    auto result = registry_->GetNode(request->node_id(), Clock::now()); // 获取节点信息
    if (!result.ok()) return ToGrpc(result.status);
    FillNode(*result.value, reply);  // 将自定义的 NodeInfo 填充到 gRPC 响应的 NodeInfo 中
    return grpc::Status::OK;
}

grpc::Status CoordinatorService::ListNodes(grpc::ServerContext*,
                                           const control::ListNodesRequest*,
                                           control::ListNodesReply* reply)
{
    for (const auto& node : registry_->ListNodes(Clock::now())) {
        FillNode(node, reply->add_nodes());
    }
    return grpc::Status::OK;
}

grpc::Status CoordinatorService::GetPlacement(grpc::ServerContext*,
                                              const control::GetPlacementRequest* request,
                                              control::GetPlacementReply* reply)
{
    if (!IsCanonicalId(request->chunk_id()) || request->replica_count() == 0 ||
        request->replica_count() > kMaxPlacementReplicas) {
        return {grpc::StatusCode::INVALID_ARGUMENT, "invalid chunk ID or replica count"};
    }
    auto result = coordinator::GetPlacement(request->chunk_id(), request->replica_count(),
                                            registry_->OnlineNodes(Clock::now()));
    if (!result.ok()) return ToGrpc(result.status);
    for (const auto& node : *result.value) FillNode(node, reply->add_candidates());
    return grpc::Status::OK;
}

}  // namespace object_storage::coordinator
