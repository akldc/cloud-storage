#include "storage/chunk_store/ChunkStore.h"

#include <cassert>
#include <filesystem>
#include <fcntl.h>
#include <string>
#include <type_traits>
#include <unistd.h>

using namespace object_storage;
namespace fs = std::filesystem;

namespace {
static_assert(!std::is_polymorphic_v<ChunkWriter>);
static_assert(!std::is_polymorphic_v<ChunkReader>);

const std::string kChunk = "01234567-89ab-cdef-0123-456789abcdef";
const std::string kObject = "11111111-2222-3333-4444-555555555555";
const std::string kUpload = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

class TempDir {
public:
    TempDir()
    {
        char pattern[] = "/tmp/chunk-store-test.XXXXXX";
        char* path = ::mkdtemp(pattern);
        assert(path);
        path_ = path;
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path_, ec); }
    const std::string& path() const { return path_; }
private:
    std::string path_;
};

BeginChunkWrite MakeRequest(const std::string& contents)
{
    auto checksum = Sha256Bytes(contents.data(), contents.size());
    assert(checksum.ok());
    BeginChunkWrite request;
    request.uploadId = kUpload;
    request.descriptor = {kChunk, kObject, 7, 2, 1024, contents.size(),
                          checksum.value(), 1, 1};
    return request;
}

void TestWriteReadRepeatAndDelete()
{
    TempDir root;
    ChunkStore store(root.path());
    assert(store.Stat({kChunk, 1}).status().code() == ErrorCode::kConflict);
    assert(store.Initialize().ok());
    auto initializedAgain = store.Initialize();
    assert(initializedAgain.ok() && initializedAgain.value().healthyChunks == 0);
    const std::string contents = "persistent chunk contents";
    auto request = MakeRequest(contents);
    auto result = store.BeginWrite(request);
    assert(result.ok());
    auto writer = std::move(result).value();
    auto wrongOffset = writer->Append(1, contents.data(), 4);
    assert(wrongOffset.status.code() == ErrorCode::kOffsetMismatch);
    assert(wrongOffset.expectedOffset == 0 && wrongOffset.durableOffset == 0);
    assert(writer->Append(0, contents.data(), 8).ok());
    assert(writer->Checkpoint().ok() && writer->durableBytes() == 8);
    auto repeated = writer->Append(0, contents.data(), 8);
    assert(repeated.status.code() == ErrorCode::kOffsetMismatch);
    assert(repeated.expectedOffset == 8 && repeated.durableOffset == 8);
    assert(store.BeginWrite(request).status().code() == ErrorCode::kConflict);
    assert(writer->Append(8, contents.data() + 8, contents.size() - 8).ok());
    assert(writer->Commit().ok());
    writer.reset();

    assert(store.BeginWrite(request).status().code() == ErrorCode::kAlreadyCommitted);
    auto readerResult = store.OpenRead({kChunk, 1}, {3, 8});
    assert(readerResult.ok());
    auto reader = std::move(readerResult).value();
    assert(reader->range().offset == 3 && reader->length() == 8);
    char buffer[16]{};
    auto read = reader->ReadAt(0, buffer, sizeof buffer);
    assert(read.ok() && std::string(buffer, read.value()) == contents.substr(3, 8));

    auto emptyResult = store.OpenRead({kChunk, 1}, {contents.size(), 0});
    assert(emptyResult.ok());
    auto empty = std::move(emptyResult).value();
    assert(empty->length() == 0);
    assert(empty->ReadAt(0, nullptr, 0).value() == 0);

    assert(store.Delete({kChunk, 2}).code() == ErrorCode::kStaleGeneration);
    assert(store.Delete({kChunk, 1}).ok());
    assert(store.Delete({kChunk, 1}).ok());
    assert(store.Stat({kChunk, 1}).status().code() == ErrorCode::kNotFound);
}

void TestChecksumFailureIsHidden()
{
    TempDir root;
    ChunkStore store(root.path());
    assert(store.Initialize().ok());
    auto result = store.BeginWrite(MakeRequest("expected"));
    assert(result.ok());
    auto writer = std::move(result).value();
    const std::string wrong = "differnt";
    assert(writer->Append(0, wrong.data(), wrong.size()).ok());
    assert(writer->Commit().status().code() == ErrorCode::kChecksumMismatch);
    assert(writer->Abort().ok());
    writer.reset();
    assert(store.Stat({kChunk, 1}).status().code() == ErrorCode::kNotFound);
}

void TestRestartRecovery()
{
    TempDir root;
    const std::string contents = "checkpoint-and-resume";
    {
        ChunkStore store(root.path());
        assert(store.Initialize().ok());
        auto result = store.BeginWrite(MakeRequest(contents));
        assert(result.ok());
        auto writer = std::move(result).value();
        assert(writer->Append(0, contents.data(), 10).ok());
        assert(writer->Checkpoint().ok());
        assert(writer->Append(10, contents.data() + 10, 3).ok());
    }
    {
        ChunkStore store(root.path());
        auto recovery = store.Initialize();
        assert(recovery.ok() && recovery.value().resumableWrites == 1);
        assert(store.Stat({kChunk, 1}).status().code() == ErrorCode::kNotFound);
        auto result = store.ResumeWrite({kUpload, {kChunk, 1}});
        assert(result.ok());
        auto writer = std::move(result).value();
        assert(writer->durableBytes() == 10);
        assert(writer->Append(10, contents.data() + 10, contents.size() - 10).ok());
        assert(writer->Commit().ok());
    }
    ChunkStore restarted(root.path());
    auto recovery = restarted.Initialize();
    assert(recovery.ok() && recovery.value().healthyChunks == 1);
}

void TestVerifyDetectsCorruption()
{
    TempDir root;
    ChunkStore store(root.path());
    assert(store.Initialize().ok());
    const std::string contents = "verify-me";
    auto result = store.BeginWrite(MakeRequest(contents));
    assert(result.ok());
    auto writer = std::move(result).value();
    assert(writer->Append(0, contents.data(), contents.size()).ok());
    assert(writer->Commit().ok());
    writer.reset();
    auto healthy = store.Verify({kChunk, 1});
    assert(healthy.ok() && healthy.value().checksumMatches);
    fs::path data = fs::path(root.path()) / "chunks" / "01" / "23" / kChunk / "data";
    int fd = ::open(data.c_str(), O_WRONLY | O_CLOEXEC);
    assert(fd >= 0 && ::pwrite(fd, "X", 1, 0) == 1);
    ::close(fd);
    auto corrupted = store.Verify({kChunk, 1});
    assert(corrupted.ok() && !corrupted.value().checksumMatches);
}

void TestRecoveryPublishesInterruptedCommit()
{
    TempDir root;
    const std::string contents = "publish-after-restart";
    {
        ChunkStore store(root.path());
        assert(store.Initialize().ok());
        auto result = store.BeginWrite(MakeRequest(contents));
        assert(result.ok());
        auto writer = std::move(result).value();
        assert(writer->Append(0, contents.data(), contents.size()).ok());
        assert(writer->Commit().ok());
    }
    const fs::path final = fs::path(root.path()) / "chunks" / "01" / "23" / kChunk;
    const fs::path interrupted = fs::path(root.path()) / "staging" /
                                 (kChunk + "." + kUpload + ".1.tmp");
    fs::rename(final, interrupted);  // State after staging is complete, before publish.

    ChunkStore restarted(root.path());
    auto recovery = restarted.Initialize();
    assert(recovery.ok() && recovery.value().completedCommits == 1);
    assert(restarted.Stat({kChunk, 1}).ok());
}
}  // namespace

int main()
{
    TestWriteReadRepeatAndDelete();
    TestChecksumFailureIsHidden();
    TestRestartRecovery();
    TestVerifyDetectsCorruption();
    TestRecoveryPublishesInterruptedCommit();
}
