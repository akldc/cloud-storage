#include "coordinator/CoordinatorService.h"
#include "coordinator/ObjectCatalog.h"

#include <grpcpp/grpcpp.h>

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>

using namespace object_storage::coordinator;
namespace proto = object_storage::control::v1;

namespace {
const std::string requestId = "11111111-1111-4111-8111-111111111111";
const std::string nodeId = "22222222-2222-4222-8222-222222222222";
const std::string instanceId = "33333333-3333-4333-8333-333333333333";
const std::string sha(64, 'a');

void Deadline(grpc::ClientContext* context)
{
    context->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
}

void Run(const std::string& db)
{
    NodeRegistry registry;
    ObjectCatalog catalog(db);
    CoordinatorService service(&registry, 2000, 10000, &catalog);
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(1024 * 1024);
    builder.SetMaxSendMessageSize(1024 * 1024);
    builder.RegisterService(&service);
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    auto server = builder.BuildAndStart();
    assert(server && port != 0);
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    auto stub = proto::Coordinator::NewStub(grpc::CreateCustomChannel(
        "127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials(), args));

    proto::RegisterNodeRequest registration;
    registration.set_node_id(nodeId);
    registration.set_instance_id(instanceId);
    registration.set_host("127.0.0.1");
    registration.set_port(18080);
    proto::RegisterNodeReply registered;
    grpc::ClientContext registerContext;
    Deadline(&registerContext);
    assert(stub->RegisterNode(&registerContext, registration, &registered).ok());

    proto::BeginUploadRequest begin;
    begin.set_request_id(requestId);
    begin.set_total_size(1);
    begin.set_file_sha256(sha);
    auto* chunk = begin.add_chunks();
    chunk->set_index(0);
    chunk->set_offset(0);
    chunk->set_size(1);
    chunk->set_sha256(sha);
    proto::BeginUploadReply plan;
    grpc::ClientContext beginContext;
    Deadline(&beginContext);
    assert(stub->BeginUpload(&beginContext, begin, &plan).ok());
    assert(plan.version() == 1 && plan.chunks_size() == 1);
    assert(plan.chunks(0).target_node_id() == nodeId);

    proto::GetObjectRequest getObject;
    getObject.set_object_id(plan.object_id());
    proto::GetObjectReply object;
    grpc::ClientContext hiddenContext;
    Deadline(&hiddenContext);
    assert(stub->GetObject(&hiddenContext, getObject, &object).error_code() == grpc::NOT_FOUND);

    proto::CommitUploadRequest commit;
    commit.set_upload_id(plan.upload_id());
    proto::CommitUploadReply committed;
    grpc::ClientContext earlyContext;
    Deadline(&earlyContext);
    assert(stub->CommitUpload(&earlyContext, commit, &committed).error_code() ==
           grpc::FAILED_PRECONDITION);

    proto::GetUploadRequest getUpload;
    getUpload.set_request_id(requestId);
    proto::GetUploadReply upload;
    grpc::ClientContext getContext;
    Deadline(&getContext);
    assert(stub->GetUpload(&getContext, getUpload, &upload).ok());
    assert(upload.state() == proto::UPLOAD_PREPARING && !upload.chunks(0).confirmed());
    assert(upload.chunks(0).chunk_id() == plan.chunks(0).chunk_id());

    assert(catalog.ConfirmLocation(plan.upload_id(), 0, nodeId).ok());
    grpc::ClientContext commitContext;
    Deadline(&commitContext);
    assert(stub->CommitUpload(&commitContext, commit, &committed).ok());
    assert(committed.object_id() == plan.object_id());
    grpc::ClientContext visibleContext;
    Deadline(&visibleContext);
    assert(stub->GetObject(&visibleContext, getObject, &object).ok());
    assert(object.chunks(0).confirmed_node_id() == nodeId);
    grpc::ClientContext retryContext;
    Deadline(&retryContext);
    proto::CommitUploadReply retry;
    assert(stub->CommitUpload(&retryContext, commit, &retry).ok());
    assert(retry.object_id() == committed.object_id());

    server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(3));
    server->Wait();
}
}

int main()
{
    char path[] = "/tmp/object-grpc-test-XXXXXX";
    char* dir = ::mkdtemp(path);
    assert(dir);
    const std::string directory(dir);
    Run(directory + "/objects.sqlite");
    std::filesystem::remove_all(directory);
}
