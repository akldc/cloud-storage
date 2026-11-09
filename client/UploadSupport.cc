#include "client/ObjectClientInternal.h"

#include "storage/common/Types.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <filesystem>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/syscall.h>

namespace object_storage {
namespace detail {

/// @brief 读取源文件，计算整个文件以及每个 Chunk 的 SHA-256，构建上传清单
bool ScanFile(int fd, uint64_t size, proto::BeginUploadRequest* manifest,
              std::string* error)
{
    Digest file(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    Digest chunk(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!file || !chunk || EVP_DigestInit_ex(file.get(), EVP_sha256(), nullptr) != 1 ||
        EVP_DigestInit_ex(chunk.get(), EVP_sha256(), nullptr) != 1) {
        *error = "cannot initialize upload SHA-256";
        return false;
    }
    std::array<char, 64 * 1024> buffer{};  // 64KB 缓冲区
    uint64_t offset = 0, currentSize = 0;
    while (offset < size) {
        const size_t wanted = static_cast<size_t>(std::min<uint64_t>(buffer.size(), size - offset));
        const ssize_t count = ::pread(fd, buffer.data(), wanted, static_cast<off_t>(offset)); // pread 从指定 offset 读取，不改变文件当前偏移
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            *error = count == 0 ? "source ended during manifest scan" : ErrnoText("read source");
            return false;
        }
        if (EVP_DigestUpdate(file.get(), buffer.data(), static_cast<size_t>(count)) != 1 ||
            EVP_DigestUpdate(chunk.get(), buffer.data(), static_cast<size_t>(count)) != 1) {
            *error = "cannot hash source file";
            return false;
        }
        offset += static_cast<uint64_t>(count);
        currentSize += static_cast<uint64_t>(count);
        if (currentSize == kChunkSize || offset == size) { // 当达到一个完整chunk或者文件末尾时，计算当前 chunk 的 SHA-256，并将其添加到 manifest 中
            std::string sha;
            if (!FinalDigest(chunk.get(), &sha)) { *error = "cannot finish Chunk SHA-256"; return false; }
            auto* item = manifest->add_chunks();
            item->set_index(static_cast<uint32_t>(manifest->chunks_size() - 1));
            item->set_offset(offset - currentSize);
            item->set_size(currentSize);
            item->set_sha256(sha);
            currentSize = 0;
            if (offset < size && EVP_DigestInit_ex(chunk.get(), EVP_sha256(), nullptr) != 1) {
                *error = "cannot reset Chunk SHA-256";
                return false;
            }
        }
    }
    std::string fileSha;
    if (!FinalDigest(file.get(), &fileSha)) { *error = "cannot finish file SHA-256"; return false; }
    manifest->set_total_size(size);
    manifest->set_file_sha256(fileSha);
    return true;
}

// 判断源文件是否发生变更。扫描文件结束后，正式commit前
bool SourceUnchanged(int fd, const struct stat& original)
{
    struct stat current{};
    return ::fstat(fd, &current) == 0 && current.st_dev == original.st_dev &&
           current.st_ino == original.st_ino && current.st_size == original.st_size &&
           current.st_mtim.tv_sec == original.st_mtim.tv_sec &&
           current.st_mtim.tv_nsec == original.st_mtim.tv_nsec &&
           current.st_ctim.tv_sec == original.st_ctim.tv_sec &&
           current.st_ctim.tv_nsec == original.st_ctim.tv_nsec;
}

// 根据 requestId 查询之前的上传任务，用于断点续传
bool FetchUpload(proto::Coordinator::Stub* stub, const std::string& requestId,
                 proto::GetUploadReply* upload, grpc::Status* status)
{
    proto::GetUploadRequest request;
    request.set_request_id(requestId);
    grpc::ClientContext context;
    Deadline(&context);
    *status = stub->GetUpload(&context, request, upload);
    return status->ok();
}

// 验证返回的上传任务信息是否与本地源文件的 manifest 一致
bool ValidateUpload(const proto::BeginUploadRequest& manifest,
                    const proto::GetUploadReply& upload, std::string* error)
{
    if (!IsCanonicalId(upload.object_id()) || !IsCanonicalId(upload.upload_id()) ||
        upload.version() != 1 || upload.total_size() != manifest.total_size() ||
        upload.file_sha256() != manifest.file_sha256() ||
        upload.chunks_size() != manifest.chunks_size() ||
        (upload.state() != proto::UPLOAD_PREPARING &&
         upload.state() != proto::UPLOAD_COMMITTED && upload.state() != proto::UPLOAD_ABORTED)) {
        *error = "saved upload differs from source file or has invalid metadata";
        return false;
    }
    for (int i = 0; i < manifest.chunks_size(); ++i) {
        const auto& expected = manifest.chunks(i);
        const auto& saved = upload.chunks(i);
        if (saved.index() != expected.index() || saved.offset() != expected.offset() ||
            saved.size() != expected.size() || saved.sha256() != expected.sha256() ||
            saved.generation() != 1 || !IsCanonicalId(saved.chunk_id()) ||
            !IsCanonicalId(saved.target_node_id()) ||
            (saved.confirmed() != !saved.confirmed_node_id().empty()) ||
            (saved.confirmed() && saved.confirmed_node_id() != saved.target_node_id())) {
            *error = "saved Chunk plan differs from source manifest";
            return false;
        }
    }
    return true;
}

bool WriteAll(int fd, const std::string& data)
{
    size_t done = 0;
    while (done < data.size()) {
        const ssize_t count = ::write(fd, data.data() + done, data.size() - done);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        done += static_cast<size_t>(count);
    }
    return true;
}

// 上传 断点恢复的关键
bool Sidecar(const std::string& source, const std::string& stateOverride, uint64_t size,
             std::string* statePath,
             std::string* requestId, Fd* lock, std::string* error)
{
    std::error_code pathError;
    const auto canonical = std::filesystem::canonical(source, pathError);
    if (pathError) { *error = "cannot resolve source path: " + pathError.message(); return false; }
    if (stateOverride.empty()) *statePath = canonical.string() + ".object-upload";
    else {
        const auto absolute = std::filesystem::absolute(stateOverride, pathError);
        if (pathError) { *error = "invalid state path: " + pathError.message(); return false; }
        *statePath = absolute.lexically_normal().string();
    }

    // 尝试获取上传状态锁，防止多个客户端同时操作同一个上传状态文件
    lock->value = ::open((*statePath + ".lock").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (lock->value < 0) { *error = ErrnoText("open upload state lock"); return false; }
    if (::flock(lock->value, LOCK_EX | LOCK_NB) != 0) {
        *error = errno == EWOULDBLOCK ? "another client holds the upload state lock" :
                 ErrnoText("lock upload state");
        return false;
    }
    Fd existing(::open(statePath->c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (existing.value >= 0) {
        struct stat info{};
        if (::fstat(existing.value, &info) != 0 || !S_ISREG(info.st_mode) ||
            info.st_size <= 0 || info.st_size > 16 * 1024) {
            *error = "invalid upload state file";
            return false;
        }
        std::string data(static_cast<size_t>(info.st_size), '\0');
        size_t done = 0;
        while (done < data.size()) {
            const ssize_t count = ::read(existing.value, data.data() + done, data.size() - done);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) { *error = "cannot read complete upload state"; return false; }
            done += static_cast<size_t>(count);
        }
        const size_t first = data.find('\n');
        const size_t second = data.find('\n', first + 1);
        const size_t third = data.find('\n', second + 1);
        if (first == std::string::npos || second == std::string::npos ||
            third == std::string::npos || data.substr(0, first) != "object-upload-v1") {
            *error = "invalid upload state format";
            return false;
        }
        uint64_t savedSize = 0;
        const std::string sizeText = data.substr(first + 1, second - first - 1);
        const auto parsed = std::from_chars(sizeText.data(), sizeText.data() + sizeText.size(), savedSize);
        *requestId = data.substr(second + 1, third - second - 1);
        if (parsed.ec != std::errc{} || parsed.ptr != sizeText.data() + sizeText.size() ||
            savedSize != size || !IsCanonicalId(*requestId) ||
            data.substr(third + 1) != canonical.string()) {
            *error = "source path or size differs from upload state";
            return false;
        }
        return true;
    }
    if (errno != ENOENT) { *error = ErrnoText("open upload state"); return false; }
    if (!NewId(requestId, error)) return false;

    // 写入上传状态文件的内容格式
    // 保存requestId和源文件的绝对路径
    const std::string contents = "object-upload-v1\n" + std::to_string(size) + "\n" +
                                 *requestId + "\n" + canonical.string();
    std::string temp = *statePath + ".tmp.XXXXXX";
    Fd output(::mkostemp(temp.data(), O_CLOEXEC));
    if (output.value < 0) { *error = ErrnoText("create upload state"); return false; }
    bool published = false;
    const auto parent = std::filesystem::path(*statePath).parent_path();
    Fd directory(::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (directory.value < 0) { *error = ErrnoText("open source directory"); }
    else if (!WriteAll(output.value, contents) || ::fsync(output.value) != 0) {  // 确保上传状态文件内容写入磁盘
        *error = ErrnoText("sync upload state");
    } else if (::syscall(SYS_renameat2, AT_FDCWD, temp.c_str(), AT_FDCWD,  // 使用 renameat2 原子操作将临时文件重命名为最终的上传状态文件，确保不会覆盖已有文件
                         statePath->c_str(), RENAME_NOREPLACE) != 0) {
        *error = ErrnoText("publish upload state");
    } else {
        published = true;
        if (::fsync(directory.value) != 0) *error = ErrnoText("sync source directory");  // 同步父目录
        else return true;
    }
    if (!published) ::unlink(temp.c_str());
    return false;
}

bool RemoveSidecar(const std::string& statePath, std::string* error)
{
    const auto directory = std::filesystem::path(statePath).parent_path();
    Fd parent(::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (parent.value < 0) { *error = ErrnoText("open upload state directory"); return false; }
    if (::unlink(statePath.c_str()) != 0 && errno != ENOENT) {
        *error = ErrnoText("remove upload state");
        return false;
    }
    if (::fsync(parent.value) != 0) { *error = ErrnoText("sync upload state removal"); return false; }
    return true;
}

// 上传完成后，删除上传状态文件和锁文件
bool FinishUpload(const std::string& statePath, const std::string& committedId,
                  std::string* objectId, std::string* error)
{
    *objectId = committedId;
    if (RemoveSidecar(statePath, error)) return true;
    *error += "; object committed as " + committedId;
    return false;
}

}  // namespace detail
}  // namespace object_storage
