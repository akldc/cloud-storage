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

// ChunkKey：定位一个特定版本的 Chunk
// ChunkDescriptor：描述这个 Chunk 的完整属性

struct ChunkKey {
    std::string chunkId;
    uint64_t generation{1};
};

struct ChunkDescriptor {
    std::string chunkId;
    std::string objectId;   // 所属对象id
    uint64_t objectVersion{0};
    uint32_t index{0};      // 分块序号
    uint64_t logicalOffset{0}; // 在原始对象中的偏移
    uint64_t size{0};
    Checksum checksum;
    uint64_t generation{1}; // chunk的版本号，上传失败后重新上传会增加该值, 识别过期操作，防止旧请求干扰新状态
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
