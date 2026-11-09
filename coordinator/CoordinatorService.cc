#include "coordinator/CoordinatorService.h"

#include "coordinator/Placement.h"
#include "storage/common/Types.h"
#ifdef MYMUDUO_BUILD_OBJECTS
#include "coordinator/ChunkProbe.h"
#include "coordinator/ObjectCatalog.h"
#endif

#include <algorithm>
#include <chrono>

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
    case Code::kUnavailable: return {grpc::StatusCode::UNAVAILABLE, status.message};
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

#ifdef MYMUDUO_BUILD_OBJECTS
constexpr size_t kMaxObjectRpcBytes = 1024 * 1024;

void FillChunk(const ChunkPlan& source, control::PlannedChunk* target)
{
    target->set_index(source.input.index);
    target->set_chunk_id(source.chunkId);
    target->set_offset(source.input.offset);
    target->set_size(source.input.size);
    target->set_sha256(source.input.sha256);
    target->set_generation(1);
    target->set_target_node_id(source.targetNodeId);
    target->set_confirmed(!source.confirmedNodeId.empty());
    target->set_confirmed_node_id(source.confirmedNodeId);
}

template <typename Reply>
grpc::Status CheckReplySize(const Reply& reply)
{
    if (reply.ByteSizeLong() >= kMaxObjectRpcBytes) {
        return {grpc::StatusCode::RESOURCE_EXHAUSTED, "object RPC response exceeds 1 MiB"};
    }
    return grpc::Status::OK;
}

grpc::Status MissingCatalog()
{
    return {grpc::StatusCode::UNAVAILABLE, "object Catalog is not configured"};
}
#endif

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

#ifdef MYMUDUO_BUILD_OBJECTS
grpc::Status CoordinatorService::BeginUpload(grpc::ServerContext*,
                                              const control::BeginUploadRequest* request,
                                              control::BeginUploadReply* reply)
{
    if (!catalog_) return MissingCatalog();
    if (request->ByteSizeLong() >= kMaxObjectRpcBytes) {
        return {grpc::StatusCode::RESOURCE_EXHAUSTED, "object RPC request exceeds 1 MiB"};
    }
    BeginUploadInput input;
    input.requestId = request->request_id();
    input.totalSize = request->total_size();
    input.fileSha256 = request->file_sha256();
    input.chunks.reserve(request->chunks_size());
    for (const auto& chunk : request->chunks()) {
        input.chunks.push_back({chunk.index(), chunk.offset(), chunk.size(), chunk.sha256()});
    }
    auto result = catalog_->BeginUpload(input, registry_->OnlineNodes(Clock::now()));
    if (!result.ok()) return ToGrpc(result.status);
    reply->set_object_id(result.value->objectId);
    reply->set_upload_id(result.value->uploadId);
    reply->set_version(1);
    for (const auto& chunk : result.value->chunks) FillChunk(chunk, reply->add_chunks());
    return CheckReplySize(*reply);
}

grpc::Status CoordinatorService::GetUpload(grpc::ServerContext*,
                                            const control::GetUploadRequest* request,
                                            control::GetUploadReply* reply)
{
    if (!catalog_) return MissingCatalog();
    auto result = catalog_->GetUpload(request->request_id());
    if (!result.ok()) return ToGrpc(result.status);
    reply->set_object_id(result.value->objectId);
    reply->set_upload_id(result.value->uploadId);
    reply->set_version(1);
    reply->set_total_size(result.value->totalSize);
    reply->set_file_sha256(result.value->fileSha256);
    switch (result.value->state) {
    case UploadState::kPreparing: reply->set_state(control::UPLOAD_PREPARING); break;
    case UploadState::kCommitted: reply->set_state(control::UPLOAD_COMMITTED); break;
    case UploadState::kAborted: reply->set_state(control::UPLOAD_ABORTED); break;
    }
    for (const auto& chunk : result.value->chunks) FillChunk(chunk, reply->add_chunks());
    return CheckReplySize(*reply);
}

grpc::Status CoordinatorService::ConfirmChunk(grpc::ServerContext* context,
                                               const control::ConfirmChunkRequest* request,
                                               control::ConfirmChunkReply*)
{
    if (!catalog_) return MissingCatalog();
    auto upload = catalog_->GetUploadById(request->upload_id());
    if (!upload.ok()) return ToGrpc(upload.status);
    if (request->chunk_index() >= upload.value->chunks.size()) {
        return {grpc::StatusCode::INVALID_ARGUMENT, "chunk index out of range"};
    }
    const auto& chunk = upload.value->chunks[request->chunk_index()];
    if (!chunk.confirmedNodeId.empty()) {
        return chunk.confirmedNodeId == chunk.targetNodeId ? grpc::Status::OK :
               grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "chunk confirmed elsewhere");
    }
    if (upload.value->state != UploadState::kPreparing) {
        return {grpc::StatusCode::FAILED_PRECONDITION, "upload is not preparing"};
    }
    auto target = registry_->GetNode(chunk.targetNodeId, Clock::now());
    if (!target.ok() || !target.value->online) {
        return {grpc::StatusCode::UNAVAILABLE, "target Storage Node is offline or unknown"};
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        context->deadline() - std::chrono::system_clock::now());
    if (remaining.count() <= 0) {
        return {grpc::StatusCode::DEADLINE_EXCEEDED, "ConfirmChunk deadline expired"};
    }
    const auto timeout = std::min(remaining, std::chrono::milliseconds(probeTimeoutMs_));
    auto probed = ProbeChunk(*target.value, *upload.value, chunk, timeout);
    if (!probed.ok()) return ToGrpc(probed);
    auto current = registry_->GetNode(chunk.targetNodeId, Clock::now());
    if (!current.ok() || !current.value->online ||
        current.value->instanceId != target.value->instanceId) {
        return {grpc::StatusCode::UNAVAILABLE, "target Storage Node changed during HEAD"};
    }
    return ToGrpc(catalog_->ConfirmLocation(request->upload_id(), request->chunk_index(),
                                             chunk.targetNodeId));
}

grpc::Status CoordinatorService::CommitUpload(grpc::ServerContext*,
                                               const control::CommitUploadRequest* request,
                                               control::CommitUploadReply* reply)
{
    if (!catalog_) return MissingCatalog();
    auto result = catalog_->CommitUpload(request->upload_id());
    if (!result.ok()) return ToGrpc(result.status);
    reply->set_object_id(*result.value);
    return grpc::Status::OK;
}

grpc::Status CoordinatorService::GetObject(grpc::ServerContext*,
                                            const control::GetObjectRequest* request,
                                            control::GetObjectReply* reply)
{
    if (!catalog_) return MissingCatalog();
    auto result = catalog_->GetObject(request->object_id());
    if (!result.ok()) return ToGrpc(result.status);
    reply->set_object_id(result.value->objectId);
    reply->set_version(1);
    reply->set_total_size(result.value->totalSize);
    reply->set_file_sha256(result.value->fileSha256);
    for (const auto& chunk : result.value->chunks) FillChunk(chunk, reply->add_chunks());
    return CheckReplySize(*reply);
}
#endif

}  // namespace object_storage::coordinator
