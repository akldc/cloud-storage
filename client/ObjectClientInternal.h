#ifndef OBJECT_STORAGE_CLIENT_OBJECTCLIENTINTERNAL_H
#define OBJECT_STORAGE_CLIENT_OBJECTCLIENTINTERNAL_H

#include "client/ObjectClient.h"

#include <openssl/evp.h>

#include <cstdint>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace object_storage {
namespace detail {

namespace proto = control::v1;
constexpr uint64_t kChunkSize = 64ULL * 1024 * 1024;  // 单个chunk大小
constexpr uint32_t kMaxChunks = 1024;
constexpr uint64_t kMaxSize = kChunkSize * kMaxChunks;
using Digest = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;

// RAII 封装 fd
struct Fd {
    int value{-1};
    explicit Fd(int fd = -1);
    ~Fd();
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
};

struct PutResult {
    long code{0};
    bool uncertain{false};
    bool sourceFailed{false};
};

std::string ErrnoText(const char* action);
std::string RpcText(const grpc::Status& status);
void Deadline(grpc::ClientContext* context);
bool FinalDigest(EVP_MD_CTX* context, std::string* hex);
bool NewId(std::string* id, std::string* error);

bool ScanFile(int fd, uint64_t size, proto::BeginUploadRequest* manifest, std::string* error);
bool SourceUnchanged(int fd, const struct stat& original);
bool Sidecar(const std::string& source, const std::string& stateOverride, uint64_t size,
             std::string* statePath, std::string* requestId, Fd* lock, std::string* error);
bool FinishUpload(const std::string& statePath, const std::string& committedId,
                  std::string* objectId, std::string* error);
bool FetchUpload(proto::Coordinator::Stub* stub, const std::string& requestId,
                 proto::GetUploadReply* upload, grpc::Status* status);
bool ValidateUpload(const proto::BeginUploadRequest& manifest,
                    const proto::GetUploadReply& upload, std::string* error);

bool PutChunk(const proto::NodeInfo& node, const proto::GetUploadReply& plan,
              const proto::PlannedChunk& chunk, int fd, uint64_t resumeOffset,
              PutResult* put, std::string* error);
bool HeadChunk(const proto::NodeInfo& node, const proto::GetUploadReply& upload,
               const proto::PlannedChunk& chunk, long* code, uint64_t* durable,
               bool* permanentFailure, std::string* error);
bool GetChunk(const proto::NodeInfo& node, const proto::PlannedChunk& chunk,
              int fd, EVP_MD_CTX* fileDigest, std::string* error);

}  // namespace detail
}  // namespace object_storage

#endif
