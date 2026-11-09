#ifndef OBJECT_STORAGE_CLIENT_OBJECTCLIENT_H
#define OBJECT_STORAGE_CLIENT_OBJECTCLIENT_H

#include "coordinator_v1.grpc.pb.h"

#include <memory>
#include <string>

namespace object_storage {

class ObjectClient {
public:
    explicit ObjectClient(const std::string& coordinatorAddress);

    bool Upload(const std::string& source, std::string* objectId, std::string* error,
                const std::string& stateOverride = {});
    bool Download(const std::string& objectId, const std::string& output, std::string* error);

private:
    using Stub = control::v1::Coordinator::Stub;
    bool GetNode(const std::string& nodeId, control::v1::NodeInfo* node, std::string* error);

    std::unique_ptr<Stub> stub_;
};

}  // namespace object_storage

#endif
