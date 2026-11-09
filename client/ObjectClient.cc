#include "client/ObjectClientInternal.h"

#include "storage/common/Types.h"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fcntl.h>
#include <sys/syscall.h>
#include <thread>

namespace object_storage {

using namespace detail;

ObjectClient::ObjectClient(const std::string& coordinatorAddress)
{
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    stub_ = proto::Coordinator::NewStub(grpc::CreateCustomChannel(
        coordinatorAddress, grpc::InsecureChannelCredentials(), args));
}

bool ObjectClient::GetNode(const std::string& nodeId, proto::NodeInfo* node, std::string* error)
{
    proto::GetNodeRequest request;
    request.set_node_id(nodeId);
    grpc::ClientContext context;
    Deadline(&context);
    auto status = stub_->GetNode(&context, request, node);
    if (!status.ok()) { *error = RpcText(status); return false; }
    if (node->node_id() != nodeId || node->state() != proto::NODE_ONLINE || node->host().empty() ||
        node->port() == 0 || node->port() > 65535) {
        *error = "confirmed Storage Node is offline or has no endpoint";
        return false;
    }
    return true;
}

bool ObjectClient::Upload(const std::string& source, std::string* objectId, std::string* error,
                          const std::string& stateOverride)
{
    // 打开源文件，获取文件信息，检查文件类型和大小
    Fd file(::open(source.c_str(), O_RDONLY | O_CLOEXEC));
    if (file.value < 0) { *error = ErrnoText("open source"); return false; }
    struct stat info{};
    if (::fstat(file.value, &info) != 0) { *error = ErrnoText("stat source"); return false; }
    if (!S_ISREG(info.st_mode) || info.st_size < 0 ||
        static_cast<uint64_t>(info.st_size) > kMaxSize) {
        *error = "upload requires a regular file of at most 64 GiB";
        return false;
    }
    // 获取源文件大小，准备上传状态文件和锁文件
    const uint64_t size = static_cast<uint64_t>(info.st_size);
    std::string statePath;
    std::string requestId;
    Fd stateLock;
    if (!Sidecar(source, stateOverride, size, &statePath, &requestId, &stateLock, error)) return false;

    // 构建上传清单，计算源文件的 SHA-256 和每个 Chunk 的 SHA-256
    proto::BeginUploadRequest manifest;
    manifest.set_request_id(requestId);
    if (!ScanFile(file.value, size, &manifest, error)) return false;
    if (!SourceUnchanged(file.value, info)) {
        *error = "source file changed during manifest scan";
        return false;
    }

    // 创建 / 恢复 上传任务
    proto::GetUploadReply upload;
    grpc::Status status;
    if (!FetchUpload(stub_.get(), requestId, &upload, &status)) {
        if (status.error_code() != grpc::NOT_FOUND) { *error = RpcText(status); return false; }
        bool found = false;
        for (int attempt = 0; attempt < 3 && !found; ++attempt) {
            proto::BeginUploadReply begun;
            grpc::ClientContext context;
            Deadline(&context);
            auto beginStatus = stub_->BeginUpload(&context, manifest, &begun);  // 向 Coordinator 发起上传请求，返回上传任务信息
            if (FetchUpload(stub_.get(), requestId, &upload, &status)) {
                found = true;
            } else if (status.error_code() != grpc::NOT_FOUND) {
                *error = RpcText(status);
                return false;
            } else if (beginStatus.error_code() != grpc::UNAVAILABLE &&
                       beginStatus.error_code() != grpc::DEADLINE_EXCEEDED) {
                *error = RpcText(beginStatus);
                return false;
            }
        }
        if (!found) { *error = "BeginUpload result remains unknown; request_id=" + requestId; return false; }
    }
    if (!ValidateUpload(manifest, upload, error)) return false;
    if (upload.state() == proto::UPLOAD_ABORTED) { *error = "upload was aborted"; return false; }
    if (upload.state() == proto::UPLOAD_COMMITTED) {
        return FinishUpload(statePath, upload.object_id(), objectId, error);
    }

    // 上传每个 Chunk，直到所有 Chunk 都被确认
    for (int i = 0; i < upload.chunks_size(); ++i) {
        const auto& chunk = upload.chunks(i);
        if (chunk.confirmed()) continue;
        proto::NodeInfo node;
        if (!GetNode(chunk.target_node_id(), &node, error)) return false;
        bool confirmed = false;
        bool uncertainCommit = false;
        for (int attempt = 0; attempt < 8 && !confirmed; ++attempt) {
            error->clear();
            long headCode = 0;
            uint64_t durable = 0;
            bool permanentFailure = false;
            if (!HeadChunk(node, upload, chunk, &headCode, &durable, &permanentFailure, error)) {
                if (permanentFailure) return false;
            } else if (headCode == 200) {
                proto::ConfirmChunkRequest request;
                request.set_upload_id(upload.upload_id());
                request.set_chunk_index(chunk.index());
                proto::ConfirmChunkReply reply;
                grpc::ClientContext context;
                Deadline(&context);
                auto confirmation = stub_->ConfirmChunk(&context, request, &reply);  // 向 Coordinator 确认 Chunk 已经上传成功
                if (confirmation.ok()) { confirmed = true; break; }
                proto::GetUploadReply current;
                grpc::Status queried;
                if (FetchUpload(stub_.get(), requestId, &current, &queried) &&
                    current.chunks_size() == upload.chunks_size() && current.chunks(i).confirmed()) {
                    confirmed = true;
                    break;
                }
                *error = RpcText(confirmation);
            } else if ((headCode == 308 && durable < chunk.size()) ||
                       (headCode == 404 && !uncertainCommit)) {
                PutResult put;
                const uint64_t offset = headCode == 308 ? durable : 0;
                if (!PutChunk(node, upload, chunk, file.value, offset, &put, error)) {
                    uncertainCommit = uncertainCommit || put.uncertain;
                    if (put.sourceFailed) return false;
                } else {
                    uncertainCommit = uncertainCommit || put.uncertain;
                    if (put.code != 200 && put.code != 201 && put.code != 202 &&
                        put.code != 409 && put.code != 503) {
                        *error = "HTTP PUT returned " + std::to_string(put.code);
                        return false;
                    }
                }
            } else if (headCode == 404 && uncertainCommit) {
                *error = "Chunk commit state is unknown after HTTP 503; refusing a blind retry";
                return false;
            } else if (headCode != 308 && headCode != 409 && headCode != 503) {
                *error = "HTTP HEAD returned " + std::to_string(headCode);
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
        }
        if (!confirmed) {
            if (error->empty()) *error = "Chunk upload did not converge after bounded retries";
            *error += "; request_id=" + requestId;
            return false;
        }
    }

    // 提交上传任务，完成上传
    if (!SourceUnchanged(file.value, info)) {
        *error = "source file changed during upload; request_id=" + requestId;
        return false;
    }
    proto::CommitUploadRequest commit;
    commit.set_upload_id(upload.upload_id());
    for (int attempt = 0; attempt < 3; ++attempt) {
        proto::CommitUploadReply reply;
        grpc::ClientContext context;
        Deadline(&context);
        auto committed = stub_->CommitUpload(&context, commit, &reply);  // 向 Coordinator 提交上传任务，返回提交结果
        if (committed.ok()) {
            if (reply.object_id() != upload.object_id()) {
                *error = "Coordinator committed a different Object ID";
                return false;
            }
            return FinishUpload(statePath, reply.object_id(), objectId, error);
        }
        proto::GetUploadReply current;
        grpc::Status queried;
        if (FetchUpload(stub_.get(), requestId, &current, &queried) &&
            current.state() == proto::UPLOAD_COMMITTED && current.object_id() == upload.object_id()) {
            return FinishUpload(statePath, current.object_id(), objectId, error);
        }
        if (queried.ok() && current.state() != proto::UPLOAD_PREPARING) {
            *error = "upload left PREPARING without committing";
            return false;
        }
        *error = RpcText(committed);
    }
    *error += "; commit result uncertain; request_id=" + requestId;
    return false;
}

bool ObjectClient::Download(const std::string& objectId, const std::string& output,
                            std::string* error)
{
    if (!IsCanonicalId(objectId) || output.empty()) {
        *error = "invalid Object ID or output path";
        return false;
    }
    // 向 Coordinator 查询指定 Object 的元数据，获取其清单信息，包括总大小、SHA-256 校验和以及每个 Chunk 的信息
    proto::GetObjectRequest request;
    request.set_object_id(objectId);
    proto::GetObjectReply object;
    grpc::ClientContext context;
    Deadline(&context);
    auto status = stub_->GetObject(&context, request, &object);
    if (!status.ok()) { *error = RpcText(status); return false; }

    // 验证返回的 Object 清单是否合法，包括 Object ID、版本号、总大小、Chunk 数量以及 SHA-256 校验和等
    const uint64_t expectedChunks = object.total_size() == 0 ? 0 :
        1 + (object.total_size() - 1) / kChunkSize;
    if (object.object_id() != objectId || object.version() != 1 ||
        object.total_size() > kMaxSize || expectedChunks > kMaxChunks ||
        !ValidateChecksum({ChecksumAlgorithm::kSha256, object.file_sha256()}).ok() ||
        object.chunks_size() != static_cast<int>(expectedChunks) ||
        (object.total_size() == 0 && object.file_sha256() !=
         "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")) {
        *error = "invalid committed object manifest";
        return false;
    }
    for (int i = 0; i < object.chunks_size(); ++i) {
        const auto& chunk = object.chunks(i);
        const uint64_t offset = static_cast<uint64_t>(i) * kChunkSize;
        if (chunk.index() != static_cast<uint32_t>(i) || chunk.offset() != offset ||
            chunk.size() != std::min(kChunkSize, object.total_size() - offset) ||
            chunk.generation() != 1 || !IsCanonicalId(chunk.chunk_id()) ||
            !IsCanonicalId(chunk.confirmed_node_id()) || !chunk.confirmed() ||
            !ValidateChecksum({ChecksumAlgorithm::kSha256, chunk.sha256()}).ok()) {
            *error = "invalid committed Chunk manifest";
            return false;
        }
    }

    // 创建临时文件，下载每个 Chunk 并写入临时文件，同时计算整个文件的 SHA-256 校验和
    const std::filesystem::path outputPath(output);
    const std::filesystem::path directory = outputPath.parent_path().empty() ? "." :
                                             outputPath.parent_path();
    Fd parent(::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (parent.value < 0) { *error = ErrnoText("open output directory"); return false; }
    std::string temporary = (directory / ".object-download-XXXXXX").string();
    Fd file(::mkostemp(temporary.data(), O_CLOEXEC));
    if (file.value < 0) { *error = ErrnoText("create temporary output"); return false; }
    bool published = false;
    Digest fileDigest(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    bool success = false;
    if (!fileDigest || EVP_DigestInit_ex(fileDigest.get(), EVP_sha256(), nullptr) != 1) {
        *error = "cannot initialize file SHA-256";
    } else {
        // 下载每个 Chunk，写入临时文件，并计算整个文件的 SHA-256 校验和
        bool downloaded = true;
        for (const auto& chunk : object.chunks()) {
            proto::NodeInfo node;
            if (!GetNode(chunk.confirmed_node_id(), &node, error) ||
                !GetChunk(node, chunk, file.value, fileDigest.get(), error)) {
                downloaded = false;
                break;
            }
        }
        if (downloaded) {
        std::string actual;
        if (!FinalDigest(fileDigest.get(), &actual) || actual != object.file_sha256()) {
            *error = "downloaded Object SHA-256 mismatch";
        } else if (::fsync(file.value) != 0) {  // 确保临时文件持久化
            *error = ErrnoText("fsync temporary output");
        } else if (::syscall(SYS_renameat2, AT_FDCWD, temporary.c_str(),  // 使用 renameat2 原子操作将临时文件重命名为最终的输出文件，确保不会覆盖已有文件
                             AT_FDCWD, output.c_str(), RENAME_NOREPLACE) != 0) {
            *error = ErrnoText("publish output without replacement");
        } else {
            published = true;
            if (::fsync(parent.value) != 0) {
                *error = ErrnoText("fsync output directory; published file durability is uncertain");
            } else {
                success = true;
            }
        }
        }
    }
    if (!published) ::unlink(temporary.c_str());
    return success;
}

}  // namespace object_storage
