#ifndef OBJECT_STORAGE_CHUNK_STORE_CHUNKREADER_H
#define OBJECT_STORAGE_CHUNK_STORE_CHUNKREADER_H

#include "storage/common/Types.h"

#include <cstddef>
#include <cstdint>

namespace object_storage {

class ChunkStore;

struct ByteRange {
    uint64_t offset{0};
    uint64_t length{0};  // Exact length; zero represents a valid empty range.
};

// A reader must not outlive the ChunkStore that created it. ReadAt offsets are
// relative to range().offset and reads are bounded by range().length.
class ChunkReader {
public:
    ~ChunkReader();
    ChunkReader(const ChunkReader&) = delete;
    ChunkReader& operator=(const ChunkReader&) = delete;

    Result<size_t> ReadAt(uint64_t relativeOffset, void* output, size_t length);
    const ChunkDescriptor& descriptor() const;
    const ByteRange& range() const;
    uint64_t length() const;

private:
    friend class ChunkStore;
    ChunkReader(int fd, ChunkDescriptor descriptor, ByteRange range);

    int fd_;
    ChunkDescriptor descriptor_;
    ByteRange range_;
};

}  // namespace object_storage

#endif
