#ifndef OBJECT_STORAGE_COORDINATOR_COORDINATORSERVICE_H
#define OBJECT_STORAGE_COORDINATOR_COORDINATORSERVICE_H

#include "coordinator/NodeRegistry.h"
#include "coordinator/Config.h"
#include "coordinator_v1.grpc.pb.h"

#include <cstdint>

namespace object_storage::coordinator {

namespace control = object_storage::control::v1;
class ObjectCatalog;

class CoordinatorService final : public control::Coordinator::Service {
public:
    CoordinatorService(NodeRegistry* registry, uint32_t heartbeatMs, uint32_t leaseMs
#ifdef MYMUDUO_BUILD_OBJECTS
                       , ObjectCatalog* catalog = nullptr,
                       uint32_t probeTimeoutMs = kDefaultRpcDeadlineMs
#endif
                       )
        : registry_(registry), heartbeatMs_(heartbeatMs), leaseMs_(leaseMs)
#ifdef MYMUDUO_BUILD_OBJECTS
        , catalog_(catalog), probeTimeoutMs_(probeTimeoutMs)
#endif
    {}

    grpc::Status RegisterNode(grpc::ServerContext*, const control::RegisterNodeRequest*,
                              control::RegisterNodeReply*) override;
    grpc::Status Heartbeat(grpc::ServerContext*, const control::HeartbeatRequest*,
                           control::HeartbeatReply*) override;
    grpc::Status UnregisterNode(grpc::ServerContext*, const control::UnregisterNodeRequest*,
                                control::UnregisterNodeReply*) override;
    grpc::Status GetNode(grpc::ServerContext*, const control::GetNodeRequest*,
                         control::NodeInfo*) override;
    grpc::Status ListNodes(grpc::ServerContext*, const control::ListNodesRequest*,
                           control::ListNodesReply*) override;
    grpc::Status GetPlacement(grpc::ServerContext*, const control::GetPlacementRequest*,
                              control::GetPlacementReply*) override;
#ifdef MYMUDUO_BUILD_OBJECTS
    grpc::Status BeginUpload(grpc::ServerContext*, const control::BeginUploadRequest*,
                             control::BeginUploadReply*) override;
    grpc::Status GetUpload(grpc::ServerContext*, const control::GetUploadRequest*,
                           control::GetUploadReply*) override;
    grpc::Status ConfirmChunk(grpc::ServerContext*, const control::ConfirmChunkRequest*,
                              control::ConfirmChunkReply*) override;
    grpc::Status CommitUpload(grpc::ServerContext*, const control::CommitUploadRequest*,
                              control::CommitUploadReply*) override;
    grpc::Status GetObject(grpc::ServerContext*, const control::GetObjectRequest*,
                           control::GetObjectReply*) override;
#endif

private:
    NodeRegistry* registry_; // Server and its handlers stop before Registry destruction.
    uint32_t heartbeatMs_;  // 注册成功后告诉 StorageNode 应当使用什么心跳间隔和租约超时。
    uint32_t leaseMs_;
#ifdef MYMUDUO_BUILD_OBJECTS
    ObjectCatalog* catalog_; // Owned by main; destroyed after gRPC Server::Wait().
    uint32_t probeTimeoutMs_;
#endif
};

}  // namespace object_storage::coordinator

#endif
