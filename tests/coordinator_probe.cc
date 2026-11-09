#include "coordinator_v1.grpc.pb.h"

#include <charconv>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>

#include <grpcpp/grpcpp.h>

namespace {

namespace rpc = object_storage::control::v1;

const char* StatusName(grpc::StatusCode code)
{
    switch (code) {
    case grpc::INVALID_ARGUMENT: return "INVALID_ARGUMENT";
    case grpc::NOT_FOUND: return "NOT_FOUND";
    case grpc::ALREADY_EXISTS: return "ALREADY_EXISTS";
    case grpc::FAILED_PRECONDITION: return "FAILED_PRECONDITION";
    case grpc::RESOURCE_EXHAUSTED: return "RESOURCE_EXHAUSTED";
    case grpc::UNAVAILABLE: return "UNAVAILABLE";
    case grpc::DEADLINE_EXCEEDED: return "DEADLINE_EXCEEDED";
    default: return "OTHER";
    }
}

void PrintStatus(const grpc::Status& status)
{
    std::cout << (status.ok() ? "OK" : "ERROR ") << (status.ok() ? "" : StatusName(status.error_code()))
              << '\n';
}

void PrintNode(const rpc::NodeInfo& node)
{
    std::cout << node.node_id() << ' ' << node.instance_id() << ' '
              << node.host() << ' ' << node.port() << ' '
              << (node.state() == rpc::NODE_ONLINE ? "ONLINE" : "OFFLINE") << '\n';
}

bool ParseUint(const char* input, uint32_t* result)
{
    const std::string value(input);
    if (value.empty()) return false;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), *result);
    return parsed.ec == std::errc{} && parsed.ptr == value.data() + value.size();
}

bool ParseUint64(const char* input, uint64_t* result)
{
    const std::string value(input);
    if (value.empty()) return false;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), *result);
    return parsed.ec == std::errc{} && parsed.ptr == value.data() + value.size();
}

void SetDeadline(grpc::ClientContext* context)
{
    context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
}

}  // namespace

int main(int argc, char* argv[])
{
    if (argc < 3) return 2;
    grpc::ChannelArguments arguments;
    arguments.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    auto channel = grpc::CreateCustomChannel(argv[1], grpc::InsecureChannelCredentials(),
                                              arguments);
    auto stub = rpc::Coordinator::NewStub(channel);
    const std::string command(argv[2]);

    if ((argc == 3 && command != "list") || (argc == 4 && command == "get")) {
        const bool legacy = argc == 3;
        rpc::GetNodeRequest request;
        request.set_node_id(argv[legacy ? 2 : 3]);
        rpc::NodeInfo reply;
        grpc::ClientContext context;
        SetDeadline(&context);
        const auto status = stub->GetNode(&context, request, &reply);
        if (legacy) {
            if (status.error_code() == grpc::NOT_FOUND) return 3;
            if (!status.ok()) return 1;
            std::cout << (reply.state() == rpc::NODE_ONLINE ? "ONLINE" : "OFFLINE")
                      << ' ' << reply.instance_id() << ' ' << reply.host()
                      << ' ' << reply.port() << '\n';
        } else if (status.ok()) {
            PrintNode(reply);
        } else {
            PrintStatus(status);
        }
        return 0;
    }
    if (argc == 3 && command == "list") {
        rpc::ListNodesRequest request;
        rpc::ListNodesReply reply;
        grpc::ClientContext context;
        SetDeadline(&context);
        const auto status = stub->ListNodes(&context, request, &reply);
        if (!status.ok()) PrintStatus(status);
        else for (const auto& node : reply.nodes()) PrintNode(node);
        return 0;
    }
    if (argc == 5 && command == "placement") {
        uint32_t count = 0;
        if (!ParseUint(argv[4], &count)) return 2;
        rpc::GetPlacementRequest request;
        request.set_chunk_id(argv[3]);
        request.set_replica_count(count);
        rpc::GetPlacementReply reply;
        grpc::ClientContext context;
        SetDeadline(&context);
        const auto status = stub->GetPlacement(&context, request, &reply);
        if (!status.ok()) PrintStatus(status);
        else for (const auto& node : reply.candidates()) PrintNode(node);
        return 0;
    }
    if (argc == 7 && command == "register") {
        uint32_t port = 0;
        if (!ParseUint(argv[6], &port)) return 2;
        rpc::RegisterNodeRequest request;
        request.set_node_id(argv[3]);
        request.set_instance_id(argv[4]);
        request.set_host(argv[5]);
        request.set_port(port);
        rpc::RegisterNodeReply reply;
        grpc::ClientContext context;
        SetDeadline(&context);
        PrintStatus(stub->RegisterNode(&context, request, &reply));
        return 0;
    }
    if (argc == 5 && (command == "heartbeat" || command == "unregister")) {
        grpc::ClientContext context;
        SetDeadline(&context);
        if (command == "heartbeat") {
            rpc::HeartbeatRequest request;
            request.set_node_id(argv[3]);
            request.set_instance_id(argv[4]);
            rpc::HeartbeatReply reply;
            PrintStatus(stub->Heartbeat(&context, request, &reply));
        } else {
            rpc::UnregisterNodeRequest request;
            request.set_node_id(argv[3]);
            request.set_instance_id(argv[4]);
            rpc::UnregisterNodeReply reply;
            PrintStatus(stub->UnregisterNode(&context, request, &reply));
        }
        return 0;
    }
    if (argc == 6 && command == "begin-single") {
        uint64_t size = 0;
        if (!ParseUint64(argv[4], &size) || size == 0) return 2;
        rpc::BeginUploadRequest request;
        request.set_request_id(argv[3]);
        request.set_total_size(size);
        request.set_file_sha256(argv[5]);
        auto* chunk = request.add_chunks();
        chunk->set_index(0);
        chunk->set_offset(0);
        chunk->set_size(size);
        chunk->set_sha256(argv[5]);
        rpc::BeginUploadReply reply;
        grpc::ClientContext context;
        SetDeadline(&context);
        PrintStatus(stub->BeginUpload(&context, request, &reply));
        return 0;
    }
    if (argc == 4 && command == "get-upload") {
        rpc::GetUploadRequest request;
        request.set_request_id(argv[3]);
        rpc::GetUploadReply reply;
        grpc::ClientContext context;
        SetDeadline(&context);
        const auto status = stub->GetUpload(&context, request, &reply);
        if (!status.ok()) PrintStatus(status);
        else std::cout << "OK " << reply.object_id() << ' ' << reply.upload_id()
                       << ' ' << reply.state() << '\n';
        return 0;
    }
    if (argc == 5 && command == "confirm") {
        uint32_t index = 0;
        if (!ParseUint(argv[4], &index)) return 2;
        rpc::ConfirmChunkRequest request;
        request.set_upload_id(argv[3]);
        request.set_chunk_index(index);
        rpc::ConfirmChunkReply reply;
        grpc::ClientContext context;
        SetDeadline(&context);
        PrintStatus(stub->ConfirmChunk(&context, request, &reply));
        return 0;
    }
    if (argc == 4 && command == "commit") {
        rpc::CommitUploadRequest request;
        request.set_upload_id(argv[3]);
        rpc::CommitUploadReply reply;
        grpc::ClientContext context;
        SetDeadline(&context);
        PrintStatus(stub->CommitUpload(&context, request, &reply));
        return 0;
    }
    if (argc == 4 && command == "get-object") {
        rpc::GetObjectRequest request;
        request.set_object_id(argv[3]);
        rpc::GetObjectReply reply;
        grpc::ClientContext context;
        SetDeadline(&context);
        PrintStatus(stub->GetObject(&context, request, &reply));
        return 0;
    }
    return 2;
}
