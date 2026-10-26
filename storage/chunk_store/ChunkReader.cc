#include "storage/chunk_store/ChunkReader.h"

#include "storage/chunk_store/ChunkStoreInternal.h"

#include <algorithm>
#include <cerrno>
#include <unistd.h>

namespace object_storage {

ChunkReader::ChunkReader(int fd, ChunkDescriptor descriptor, ByteRange range)
    : fd_(fd), descriptor_(std::move(descriptor)), range_(range)
{
}

ChunkReader::~ChunkReader()
{
    if (fd_ >= 0) ::close(fd_);
}

Result<size_t> ChunkReader::ReadAt(uint64_t relativeOffset,
                                  void* output,
                                  size_t length)
{
    if (length > 0 && output == nullptr) {
        return Status(ErrorCode::kInvalidArgument, "read output is null");
    }
    if (relativeOffset > range_.length) {
        return Status(ErrorCode::kInvalidArgument,
                      "read offset is outside selected range");
    }

    // 对外 offset 始终相对于打开的 Range；真正访问文件时再叠加范围起点。
    // 读取长度会被范围尾部截断，绝不会越过调用方选择的窗口。
    const size_t wanted = static_cast<size_t>(
        std::min<uint64_t>(length, range_.length - relativeOffset));
    ssize_t n;
    do {
        n = ::pread(fd_, output, wanted,
                    static_cast<off_t>(range_.offset + relativeOffset));
    } while (n < 0 && errno == EINTR);
    if (n < 0) return detail::IoError("read chunk");
    return static_cast<size_t>(n);
}

const ChunkDescriptor& ChunkReader::descriptor() const
{
    return descriptor_;
}

const ByteRange& ChunkReader::range() const
{
    return range_;
}

uint64_t ChunkReader::length() const
{
    return range_.length;
}

}  // namespace object_storage
