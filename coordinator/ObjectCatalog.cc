#include "coordinator/ObjectCatalog.h"

#include "coordinator/Placement.h"
#include "storage/common/Types.h"

#include <sqlite3.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <stdexcept>
#include <sys/file.h>
#include <sys/random.h>
#include <unistd.h>
#include <utility>

namespace object_storage::coordinator {
namespace {

using Statement = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;

Statement Prepare(sqlite3* db, const char* sql)
{
    sqlite3_stmt* raw = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &raw, nullptr) != SQLITE_OK) {
        return Statement(nullptr, sqlite3_finalize);
    }
    return Statement(raw, sqlite3_finalize);
}

void BindText(sqlite3_stmt* statement, int index, const std::string& value)
{
    sqlite3_bind_text(statement, index, value.c_str(), static_cast<int>(value.size()),
                      SQLITE_TRANSIENT);
}

std::string Text(sqlite3_stmt* statement, int column)
{
    const auto* value = sqlite3_column_text(statement, column);
    return value ? reinterpret_cast<const char*>(value) : "";
}

int64_t UnixMillis()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

Result<std::string> RandomId()
{
    std::array<unsigned char, 16> bytes{};
    size_t read = 0;
    while (read < bytes.size()) {
        const ssize_t count = ::getrandom(bytes.data() + read, bytes.size() - read, 0);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return Status{Code::kUnavailable, "cannot generate object UUID"};
        read += static_cast<size_t>(count);
    }
    bytes[6] = static_cast<unsigned char>((bytes[6] & 0x0f) | 0x40);
    bytes[8] = static_cast<unsigned char>((bytes[8] & 0x3f) | 0x80);
    constexpr char hex[] = "0123456789abcdef";
    std::string id;
    id.reserve(36);
    for (size_t i = 0; i < bytes.size(); ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) id.push_back('-');
        id.push_back(hex[bytes[i] >> 4]);
        id.push_back(hex[bytes[i] & 0x0f]);
    }
    return id;
}

bool ValidSha256(const std::string& value)
{
    return ValidateChecksum({ChecksumAlgorithm::kSha256, value}).ok();
}

const char* StateName(UploadState state)
{
    switch (state) {
    case UploadState::kPreparing: return "PREPARING";
    case UploadState::kCommitted: return "COMMITTED";
    case UploadState::kAborted: return "ABORTED";
    }
    return "UNKNOWN";
}

}  // namespace

ObjectCatalog::ObjectCatalog(std::string dbPath) : dbPath_(std::move(dbPath))
{
    if (dbPath_.empty()) throw std::invalid_argument("object DB path is empty");
    lockFd_ = ::open((dbPath_ + ".lock").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (lockFd_ < 0) throw std::runtime_error(std::string("cannot open DB lock: ") + std::strerror(errno));
    try {
        if (::flock(lockFd_, LOCK_EX | LOCK_NB) != 0) {
            throw std::runtime_error("object DB is already owned by another Coordinator");
        }
        const int opened = sqlite3_open_v2(dbPath_.c_str(), &db_,
                                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
        if (opened != SQLITE_OK) throw std::runtime_error(SqlError(opened).message);
        sqlite3_busy_timeout(db_, 1000);
        for (const char* pragma : {"PRAGMA foreign_keys=ON", "PRAGMA journal_mode=WAL",
                                   "PRAGMA synchronous=FULL"}) {
            auto status = Exec(pragma);
            if (!status.ok()) throw std::runtime_error(status.message);
        }
        {
            auto journal = Prepare(db_, "PRAGMA journal_mode");
            if (!journal || sqlite3_step(journal.get()) != SQLITE_ROW ||
                Text(journal.get(), 0) != "wal") {
                throw std::runtime_error("object DB requires SQLite WAL mode");
            }
        }
        int schemaVersion = 0;
        {
            auto version = Prepare(db_, "PRAGMA user_version");
            if (!version || sqlite3_step(version.get()) != SQLITE_ROW) {
                throw std::runtime_error("cannot read object DB schema version");
            }
            schemaVersion = sqlite3_column_int(version.get(), 0);
        }
        if (schemaVersion != 0 && schemaVersion != 1) {
            throw std::runtime_error("unsupported object DB schema version");
        }
        if (schemaVersion == 0) {
            auto status = Exec("BEGIN IMMEDIATE");
            if (!status.ok()) throw std::runtime_error(status.message);
            for (const char* statement : {
                     "CREATE TABLE objects(object_id TEXT PRIMARY KEY,"
                     "request_id TEXT UNIQUE NOT NULL,upload_id TEXT UNIQUE NOT NULL,"
                     "version INTEGER NOT NULL CHECK(version=1),state TEXT NOT NULL "
                     "CHECK(state IN ('PREPARING','COMMITTED','ABORTED')),"
                     "total_size INTEGER NOT NULL,file_sha256 TEXT NOT NULL,"
                     "created_ms INTEGER NOT NULL,committed_ms INTEGER)",
                     "CREATE TABLE chunks(chunk_id TEXT PRIMARY KEY,object_id TEXT NOT NULL "
                     "REFERENCES objects(object_id),chunk_index INTEGER NOT NULL,"
                     "logical_offset INTEGER NOT NULL,size INTEGER NOT NULL,sha256 TEXT NOT NULL,"
                     "generation INTEGER NOT NULL CHECK(generation=1),target_node_id TEXT NOT NULL,"
                     "UNIQUE(object_id,chunk_index))",
                     "CREATE TABLE chunk_locations(chunk_id TEXT PRIMARY KEY REFERENCES chunks(chunk_id),"
                     "node_id TEXT NOT NULL,confirmed_ms INTEGER NOT NULL)",
                     "PRAGMA user_version=1"}) {
                status = Exec(statement);
                if (!status.ok()) {
                    Exec("ROLLBACK");
                    throw std::runtime_error(status.message);
                }
            }
            status = Exec("COMMIT");
            if (!status.ok()) {
                Exec("ROLLBACK");
                throw std::runtime_error(status.message);
            }
        }
    } catch (...) {
        if (db_) sqlite3_close(db_);
        db_ = nullptr;
        ::close(lockFd_);
        lockFd_ = -1;
        throw;
    }
}

ObjectCatalog::~ObjectCatalog()
{
    if (db_) sqlite3_close(db_);
    if (lockFd_ >= 0) ::close(lockFd_);
}

Status ObjectCatalog::SqlError(int code) const
{
    const int primary = code & 0xff;
    Code mapped = Code::kInternal;
    if (primary == SQLITE_BUSY || primary == SQLITE_LOCKED || primary == SQLITE_IOERR ||
        primary == SQLITE_FULL || primary == SQLITE_CANTOPEN) mapped = Code::kUnavailable;
    if (primary == SQLITE_CONSTRAINT) mapped = Code::kAlreadyExists;
    return {mapped, "SQLite " + std::to_string(code) + ": " +
                        (db_ ? sqlite3_errmsg(db_) : sqlite3_errstr(code))};
}

Status ObjectCatalog::Exec(const char* sql)
{
    const int code = sqlite3_exec(db_, sql, nullptr, nullptr, nullptr);
    return code == SQLITE_OK ? Status{} : SqlError(code);
}

Status ObjectCatalog::Validate(const BeginUploadInput& input)
{
    if (!IsCanonicalId(input.requestId) || !ValidSha256(input.fileSha256) ||
        input.totalSize > kMaxObjectSize) {
        return {Code::kInvalidArgument, "invalid request ID, object checksum, or total size"};
    }
    const uint64_t expectedCount = input.totalSize == 0 ? 0 :
        1 + (input.totalSize - 1) / kObjectChunkSize;
    if (input.chunks.size() != expectedCount) {
        return {Code::kInvalidArgument, "manifest has an incorrect chunk count"};
    }
    for (size_t i = 0; i < input.chunks.size(); ++i) {
        const auto& chunk = input.chunks[i];
        const uint64_t expectedSize = std::min<uint64_t>(
            kObjectChunkSize, input.totalSize - i * kObjectChunkSize);
        if (chunk.index != i || chunk.offset != i * kObjectChunkSize ||
            chunk.size != expectedSize || !ValidSha256(chunk.sha256)) {
            return {Code::kInvalidArgument, "manifest chunks must be contiguous fixed-size blocks"};
        }
    }
    if (input.totalSize == 0 &&
        input.fileSha256 != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") {
        return {Code::kInvalidArgument, "empty object checksum mismatch"};
    }
    return {};
}

bool ObjectCatalog::SameManifest(const UploadRecord& saved, const BeginUploadInput& input)
{
    if (saved.totalSize != input.totalSize || saved.fileSha256 != input.fileSha256 ||
        saved.chunks.size() != input.chunks.size()) return false;
    for (size_t i = 0; i < input.chunks.size(); ++i) {
        const auto& a = saved.chunks[i].input;
        const auto& b = input.chunks[i];
        if (a.index != b.index || a.offset != b.offset || a.size != b.size ||
            a.sha256 != b.sha256) return false;
    }
    return true;
}

Result<UploadRecord> ObjectCatalog::Load(const char* column, const std::string& id)
{
    const std::string sql = std::string("SELECT object_id,request_id,upload_id,state,total_size,") +
                            "file_sha256 FROM objects WHERE " + column + "=?1";
    auto object = Prepare(db_, sql.c_str());
    if (!object) return SqlError(sqlite3_errcode(db_));
    BindText(object.get(), 1, id);
    const int found = sqlite3_step(object.get());
    if (found == SQLITE_DONE) return Status{Code::kNotFound, "upload or object not found"};
    if (found != SQLITE_ROW) return SqlError(found);
    UploadRecord record;
    record.objectId = Text(object.get(), 0);
    record.requestId = Text(object.get(), 1);
    record.uploadId = Text(object.get(), 2);
    const std::string state = Text(object.get(), 3);
    if (state == "PREPARING") record.state = UploadState::kPreparing;
    else if (state == "COMMITTED") record.state = UploadState::kCommitted;
    else if (state == "ABORTED") record.state = UploadState::kAborted;
    else return Status{Code::kInternal, "invalid stored object state"};
    record.totalSize = static_cast<uint64_t>(sqlite3_column_int64(object.get(), 4));
    record.fileSha256 = Text(object.get(), 5);
    auto chunks = Prepare(db_,
        "SELECT c.chunk_id,c.chunk_index,c.logical_offset,c.size,c.sha256,c.generation,"
        "c.target_node_id,l.node_id FROM chunks c LEFT JOIN chunk_locations l "
        "ON l.chunk_id=c.chunk_id WHERE c.object_id=?1 ORDER BY c.chunk_index");
    if (!chunks) return SqlError(sqlite3_errcode(db_));
    BindText(chunks.get(), 1, record.objectId);
    int step = SQLITE_ROW;
    while ((step = sqlite3_step(chunks.get())) == SQLITE_ROW) {
        ChunkPlan chunk;
        chunk.chunkId = Text(chunks.get(), 0);
        chunk.input.index = static_cast<uint32_t>(sqlite3_column_int(chunks.get(), 1));
        chunk.input.offset = static_cast<uint64_t>(sqlite3_column_int64(chunks.get(), 2));
        chunk.input.size = static_cast<uint64_t>(sqlite3_column_int64(chunks.get(), 3));
        chunk.input.sha256 = Text(chunks.get(), 4);
        if (sqlite3_column_int(chunks.get(), 5) != 1) {
            return Status{Code::kInternal, "invalid stored chunk generation"};
        }
        chunk.targetNodeId = Text(chunks.get(), 6);
        chunk.confirmedNodeId = Text(chunks.get(), 7);
        record.chunks.push_back(std::move(chunk));
    }
    if (step != SQLITE_DONE) return SqlError(step);
    return record;
}

Result<UploadRecord> ObjectCatalog::BeginUpload(const BeginUploadInput& input,
                                                 const std::vector<NodeInfo>& onlineNodes)
{
    const auto validated = Validate(input);
    if (!validated.ok()) return validated;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto existing = Load("request_id", input.requestId);
        if (existing.ok()) {
            if (!SameManifest(*existing.value, input)) {
                return Status{Code::kAlreadyExists, "request ID used with a different manifest"};
            }
            return existing;
        }
        if (existing.status.code != Code::kNotFound) return existing.status;
    }

    auto objectId = RandomId();
    auto uploadId = RandomId();
    if (!objectId.ok()) return objectId.status;
    if (!uploadId.ok()) return uploadId.status;
    UploadRecord record;
    record.requestId = input.requestId;
    record.objectId = *objectId.value;
    record.uploadId = *uploadId.value;
    record.totalSize = input.totalSize;
    record.fileSha256 = input.fileSha256;
    record.chunks.reserve(input.chunks.size());
    for (const auto& chunk : input.chunks) {
        auto id = RandomId();
        if (!id.ok()) return id.status;
        auto target = GetPlacement(*id.value, 1, onlineNodes);
        if (!target.ok()) return target.status;
        record.chunks.push_back({chunk, *id.value, target.value->front().nodeId, {}});
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto status = Exec("BEGIN IMMEDIATE");
    if (!status.ok()) return status;
    auto existing = Load("request_id", input.requestId);
    if (existing.ok()) {
        Exec("ROLLBACK");
        if (!SameManifest(*existing.value, input)) {
            return Status{Code::kAlreadyExists, "request ID used with a different manifest"};
        }
        return existing;
    }
    if (existing.status.code != Code::kNotFound) {
        Exec("ROLLBACK");
        return existing.status;
    }
    auto object = Prepare(db_, "INSERT INTO objects(object_id,request_id,upload_id,version,"
                               "state,total_size,file_sha256,created_ms) VALUES(?1,?2,?3,1,"
                               "'PREPARING',?4,?5,?6)");
    if (!object) {
        status = SqlError(sqlite3_errcode(db_));
    } else {
        BindText(object.get(), 1, record.objectId);
        BindText(object.get(), 2, record.requestId);
        BindText(object.get(), 3, record.uploadId);
        sqlite3_bind_int64(object.get(), 4, static_cast<sqlite3_int64>(record.totalSize));
        BindText(object.get(), 5, record.fileSha256);
        sqlite3_bind_int64(object.get(), 6, UnixMillis());
        const int step = sqlite3_step(object.get());
        if (step != SQLITE_DONE) status = SqlError(step);
    }
    if (status.ok()) {
        auto chunks = Prepare(db_, "INSERT INTO chunks(chunk_id,object_id,chunk_index,"
                                   "logical_offset,size,sha256,generation,target_node_id) "
                                   "VALUES(?1,?2,?3,?4,?5,?6,1,?7)");
        if (!chunks) status = SqlError(sqlite3_errcode(db_));
        else for (const auto& chunk : record.chunks) {
            sqlite3_reset(chunks.get());
            sqlite3_clear_bindings(chunks.get());
            BindText(chunks.get(), 1, chunk.chunkId);
            BindText(chunks.get(), 2, record.objectId);
            sqlite3_bind_int(chunks.get(), 3, static_cast<int>(chunk.input.index));
            sqlite3_bind_int64(chunks.get(), 4, static_cast<sqlite3_int64>(chunk.input.offset));
            sqlite3_bind_int64(chunks.get(), 5, static_cast<sqlite3_int64>(chunk.input.size));
            BindText(chunks.get(), 6, chunk.input.sha256);
            BindText(chunks.get(), 7, chunk.targetNodeId);
            const int step = sqlite3_step(chunks.get());
            if (step != SQLITE_DONE) {
                status = SqlError(step);
                break;
            }
        }
    }
    if (!status.ok()) {
        Exec("ROLLBACK");
        return status;
    }
    status = Exec("COMMIT");
    if (!status.ok()) {
        Exec("ROLLBACK");
        return status;
    }
    return record;
}

Result<UploadRecord> ObjectCatalog::GetUpload(const std::string& requestId)
{
    if (!IsCanonicalId(requestId)) return Status{Code::kInvalidArgument, "invalid request ID"};
    std::lock_guard<std::mutex> lock(mutex_);
    return Load("request_id", requestId);
}

Result<UploadRecord> ObjectCatalog::GetUploadById(const std::string& uploadId)
{
    if (!IsCanonicalId(uploadId)) return Status{Code::kInvalidArgument, "invalid upload ID"};
    std::lock_guard<std::mutex> lock(mutex_);
    return Load("upload_id", uploadId);
}

Result<UploadRecord> ObjectCatalog::GetObject(const std::string& objectId)
{
    if (!IsCanonicalId(objectId)) return Status{Code::kInvalidArgument, "invalid object ID"};
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = Load("object_id", objectId);
    if (!found.ok()) return found;
    if (found.value->state != UploadState::kCommitted) {
        return Status{Code::kNotFound, "object is not committed"};
    }
    return found;
}

Status ObjectCatalog::ConfirmLocation(const std::string& uploadId, uint32_t chunkIndex,
                                      const std::string& nodeId)
{
    if (!IsCanonicalId(uploadId) || !IsCanonicalId(nodeId)) {
        return {Code::kInvalidArgument, "invalid upload or node ID"};
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto status = Exec("BEGIN IMMEDIATE");
    if (!status.ok()) return status;
    auto found = Load("upload_id", uploadId);
    if (!found.ok()) {
        Exec("ROLLBACK");
        return found.status;
    }
    if (chunkIndex >= found.value->chunks.size()) {
        Exec("ROLLBACK");
        return {Code::kInvalidArgument, "chunk index out of range"};
    }
    const auto& chunk = found.value->chunks[chunkIndex];
    if (!chunk.confirmedNodeId.empty()) {
        Exec("ROLLBACK");
        return chunk.confirmedNodeId == nodeId
                   ? Status{} : Status{Code::kAlreadyExists, "chunk confirmed elsewhere"};
    }
    if (found.value->state != UploadState::kPreparing || chunk.targetNodeId != nodeId) {
        Exec("ROLLBACK");
        return {Code::kFailedPrecondition, "upload state or target node differs"};
    }
    auto insert = Prepare(db_, "INSERT INTO chunk_locations(chunk_id,node_id,confirmed_ms) "
                               "VALUES(?1,?2,?3)");
    if (!insert) status = SqlError(sqlite3_errcode(db_));
    else {
        BindText(insert.get(), 1, chunk.chunkId);
        BindText(insert.get(), 2, nodeId);
        sqlite3_bind_int64(insert.get(), 3, UnixMillis());
        const int step = sqlite3_step(insert.get());
        if (step != SQLITE_DONE) status = SqlError(step);
    }
    if (!status.ok()) {
        Exec("ROLLBACK");
        return status;
    }
    status = Exec("COMMIT");
    if (!status.ok()) Exec("ROLLBACK");
    return status;
}

Result<std::string> ObjectCatalog::CommitUpload(const std::string& uploadId)
{
    if (!IsCanonicalId(uploadId)) return Status{Code::kInvalidArgument, "invalid upload ID"};
    std::lock_guard<std::mutex> lock(mutex_);
    auto status = Exec("BEGIN IMMEDIATE");
    if (!status.ok()) return status;
    auto found = Load("upload_id", uploadId);
    if (!found.ok()) {
        Exec("ROLLBACK");
        return found.status;
    }
    if (found.value->state == UploadState::kCommitted) {
        Exec("ROLLBACK");
        return found.value->objectId;
    }
    if (found.value->state != UploadState::kPreparing) {
        Exec("ROLLBACK");
        return Status{Code::kFailedPrecondition, "upload is not preparing"};
    }
    BeginUploadInput manifest;
    manifest.requestId = found.value->requestId;
    manifest.totalSize = found.value->totalSize;
    manifest.fileSha256 = found.value->fileSha256;
    for (const auto& chunk : found.value->chunks) manifest.chunks.push_back(chunk.input);
    if (!Validate(manifest).ok()) {
        Exec("ROLLBACK");
        return Status{Code::kFailedPrecondition, "stored chunk manifest is incomplete"};
    }
    for (const auto& chunk : found.value->chunks) {
        if (chunk.confirmedNodeId.empty() || chunk.confirmedNodeId != chunk.targetNodeId) {
            Exec("ROLLBACK");
            return Status{Code::kFailedPrecondition, "not all chunks are confirmed"};
        }
    }
    auto update = Prepare(db_, "UPDATE objects SET state='COMMITTED',committed_ms=?1 "
                               "WHERE upload_id=?2 AND state='PREPARING'");
    if (!update) status = SqlError(sqlite3_errcode(db_));
    else {
        sqlite3_bind_int64(update.get(), 1, UnixMillis());
        BindText(update.get(), 2, uploadId);
        const int step = sqlite3_step(update.get());
        if (step != SQLITE_DONE || sqlite3_changes(db_) != 1) {
            status = step != SQLITE_DONE ? SqlError(step) :
                     Status{Code::kInternal, "object commit updated no row"};
        }
    }
    if (!status.ok()) {
        Exec("ROLLBACK");
        return status;
    }
    status = Exec("COMMIT");
    if (!status.ok()) {
        Exec("ROLLBACK");
        return status;
    }
    return found.value->objectId;
}

}  // namespace object_storage::coordinator
