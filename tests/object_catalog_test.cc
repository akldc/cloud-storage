#include "coordinator/ObjectCatalog.h"

#include <sqlite3.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>
#include <unistd.h>

using namespace object_storage::coordinator;

namespace {
const std::string emptySha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const std::string dataSha(64, 'a');
const std::string nodeId = "11111111-1111-4111-8111-111111111111";

std::string Id(unsigned value)
{
    char result[37];
    std::snprintf(result, sizeof result, "%08x-0000-4000-8000-000000000000", value);
    return result;
}

BeginUploadInput Manifest(unsigned request, uint64_t size)
{
    BeginUploadInput input;
    input.requestId = Id(request);
    input.totalSize = size;
    input.fileSha256 = size ? dataSha : emptySha;
    for (uint64_t offset = 0; offset < size; offset += kObjectChunkSize) {
        input.chunks.push_back({static_cast<uint32_t>(input.chunks.size()), offset,
                                std::min(kObjectChunkSize, size - offset), dataSha});
    }
    return input;
}

void ExpectCode(Status status, Code code) { assert(status.code == code); }

void Run(const std::string& path)
{
    const NodeInfo node{nodeId, Id(900), "localhost", 8000, true, 0};
    const std::vector<NodeInfo> online{node};
    std::string committedId;
    std::string pendingId;
    std::string pendingUpload;
    std::string preparingId;
    {
        ObjectCatalog catalog(path);
        bool rejectedSecond = false;
        try { ObjectCatalog second(path); } catch (const std::runtime_error&) { rejectedSecond = true; }
        assert(rejectedSecond);

        auto empty = catalog.BeginUpload(Manifest(1, 0), {});
        assert(empty.ok() && empty.value->chunks.empty());
        assert(catalog.GetObject(empty.value->objectId).status.code == Code::kNotFound);
        auto committed = catalog.CommitUpload(empty.value->uploadId);
        assert(committed.ok() && *committed.value == empty.value->objectId);
        committedId = *committed.value;
        assert(catalog.GetUpload(Id(1)).value->state == UploadState::kCommitted);
        assert(catalog.GetObject(committedId).ok());
        assert(catalog.CommitUpload(empty.value->uploadId).value == committed.value);

        auto shortLast = catalog.BeginUpload(Manifest(2, kObjectChunkSize + 3), online);
        assert(shortLast.ok() && shortLast.value->chunks.size() == 2);
        assert(shortLast.value->chunks[1].input.size == 3);
        pendingId = shortLast.value->objectId;
        pendingUpload = shortLast.value->uploadId;
        auto repeated = catalog.BeginUpload(Manifest(2, kObjectChunkSize + 3), {});
        assert(repeated.ok() && repeated.value->objectId == pendingId);
        assert(repeated.value->chunks[0].chunkId == shortLast.value->chunks[0].chunkId);
        auto changed = Manifest(2, kObjectChunkSize + 3);
        changed.chunks[1].sha256 = std::string(64, 'b');
        ExpectCode(catalog.BeginUpload(changed, online).status, Code::kAlreadyExists);
        ExpectCode(catalog.CommitUpload(pendingUpload).status, Code::kFailedPrecondition);
        ExpectCode(catalog.GetObject(pendingId).status, Code::kNotFound);
        ExpectCode(catalog.ConfirmLocation(pendingUpload, 0, Id(901)), Code::kFailedPrecondition);
        ExpectCode(catalog.ConfirmLocation(pendingUpload, 0, nodeId), Code::kOk);
        ExpectCode(catalog.ConfirmLocation(pendingUpload, 0, nodeId), Code::kOk);
        ExpectCode(catalog.CommitUpload(pendingUpload).status, Code::kFailedPrecondition);
        ExpectCode(catalog.ConfirmLocation(pendingUpload, 1, nodeId), Code::kOk);
        assert(catalog.CommitUpload(pendingUpload).ok());
        assert(catalog.GetObject(pendingId).ok());

        auto exact = catalog.BeginUpload(Manifest(3, 2 * kObjectChunkSize), online);
        assert(exact.ok() && exact.value->chunks.size() == 2);
        assert(exact.value->chunks.back().input.size == kObjectChunkSize);
        preparingId = exact.value->objectId;

        auto bad = Manifest(4, kObjectChunkSize + 1);
        bad.chunks[0].size--;
        ExpectCode(catalog.BeginUpload(bad, online).status, Code::kInvalidArgument);
        bad = Manifest(4, kObjectChunkSize + 1);
        bad.chunks.pop_back();
        ExpectCode(catalog.BeginUpload(bad, online).status, Code::kInvalidArgument);
        bad = Manifest(4, kObjectChunkSize + 1);
        bad.chunks[1].offset++;
        ExpectCode(catalog.BeginUpload(bad, online).status, Code::kInvalidArgument);
        bad = Manifest(4, 1);
        bad.chunks[0].size = 0;
        ExpectCode(catalog.BeginUpload(bad, online).status, Code::kInvalidArgument);
        bad = Manifest(4, kMaxObjectSize + 1);
        ExpectCode(catalog.BeginUpload(bad, online).status, Code::kInvalidArgument);
        bad = Manifest(4, 0);
        bad.fileSha256 = dataSha;
        ExpectCode(catalog.BeginUpload(bad, online).status, Code::kInvalidArgument);

        auto limit = catalog.BeginUpload(Manifest(5, kMaxObjectSize), online);
        assert(limit.ok() && limit.value->chunks.size() == kMaxObjectChunks);
        assert(limit.value->chunks.back().input.offset == (kMaxObjectChunks - 1) * kObjectChunkSize);
        auto noTarget = catalog.BeginUpload(Manifest(6, 1), {});
        assert(!noTarget.ok());
        ExpectCode(catalog.GetUpload(Id(6)).status, Code::kNotFound);

        auto pendingEmpty = catalog.BeginUpload(Manifest(7, 0), {});
        assert(pendingEmpty.ok());
        sqlite3* blocker = nullptr;
        assert(sqlite3_open(path.c_str(), &blocker) == SQLITE_OK);
        assert(sqlite3_exec(blocker, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr) == SQLITE_OK);
        ExpectCode(catalog.CommitUpload(pendingEmpty.value->uploadId).status, Code::kUnavailable);
        ExpectCode(catalog.GetObject(pendingEmpty.value->objectId).status, Code::kNotFound);
        assert(catalog.GetUpload(Id(7)).value->state == UploadState::kPreparing);
        ExpectCode(catalog.BeginUpload(Manifest(8, 0), {}).status, Code::kUnavailable);
        ExpectCode(catalog.GetUpload(Id(8)).status, Code::kNotFound);
        assert(sqlite3_exec(blocker, "ROLLBACK", nullptr, nullptr, nullptr) == SQLITE_OK);
        assert(sqlite3_close(blocker) == SQLITE_OK);
        assert(catalog.CommitUpload(pendingEmpty.value->uploadId).ok());
        assert(catalog.GetObject(pendingEmpty.value->objectId).ok());
        assert(catalog.BeginUpload(Manifest(8, 0), {}).ok());
    }
    {
        ObjectCatalog reopened(path);
        assert(reopened.GetObject(committedId).ok());
        assert(reopened.GetObject(pendingId).ok());
        assert(reopened.GetUpload(Id(2)).value->state == UploadState::kCommitted);
        assert(reopened.GetUpload(Id(3)).value->state == UploadState::kPreparing);
        ExpectCode(reopened.GetObject(preparingId).status, Code::kNotFound);
        auto plan = reopened.BeginUpload(Manifest(2, kObjectChunkSize + 3), {});
        assert(plan.ok() && plan.value->objectId == pendingId);
    }
}
}

int main()
{
    char path[] = "/tmp/object-catalog-test-XXXXXX";
    char* dir = ::mkdtemp(path);
    assert(dir);
    const std::string directory(dir);
    Run(directory + "/objects.sqlite");
    std::filesystem::remove_all(directory);
}
