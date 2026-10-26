#ifndef OBJECT_STORAGE_COMMON_TYPES_H
#define OBJECT_STORAGE_COMMON_TYPES_H

#include "storage/common/Status.h"

#include <cstdint>
#include <string>

namespace object_storage {

enum class ChecksumAlgorithm {
    kSha256,
};

struct Checksum {
    ChecksumAlgorithm algorithm{ChecksumAlgorithm::kSha256};
    std::string value;  // Lowercase hexadecimal.
};

struct ChunkKey {
    std::string chunkId;
    uint64_t generation{1};
};

struct ChunkDescriptor {
    std::string chunkId;
    std::string objectId;
    uint64_t objectVersion{0};
    uint32_t index{0};
    uint64_t logicalOffset{0};
    uint64_t size{0};
    Checksum checksum;
    uint64_t generation{1};
    uint32_t formatVersion{1};
};

bool operator==(const Checksum& lhs, const Checksum& rhs);
bool operator==(const ChunkDescriptor& lhs, const ChunkDescriptor& rhs);
bool IsCanonicalId(const std::string& id);
Status ValidateChecksum(const Checksum& checksum);
Result<Checksum> Sha256File(int fd, uint64_t length);
Result<Checksum> Sha256Bytes(const void* data, size_t length);

}  // namespace object_storage

#endif
