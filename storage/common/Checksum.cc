#include "storage/common/Types.h"

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <memory>
#include <unistd.h>

namespace object_storage {
namespace {

std::string ToHex(const unsigned char* data, size_t length)
{
    static const char digits[] = "0123456789abcdef";
    std::string result(length * 2, '0');
    for (size_t i = 0; i < length; ++i) {
        result[2 * i] = digits[data[i] >> 4];
        result[2 * i + 1] = digits[data[i] & 0x0f];
    }
    return result;
}

Result<Checksum> Finish(EVP_MD_CTX* context)
{
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int length = 0;
    if (EVP_DigestFinal_ex(context, digest.data(), &length) != 1) {
        return Status(ErrorCode::kIoError, "EVP_DigestFinal_ex failed");
    }
    return Checksum{ChecksumAlgorithm::kSha256, ToHex(digest.data(), length)};
}

}  // namespace

bool operator==(const Checksum& lhs, const Checksum& rhs)
{
    return lhs.algorithm == rhs.algorithm && lhs.value == rhs.value;
}

bool operator==(const ChunkDescriptor& lhs, const ChunkDescriptor& rhs)
{
    return lhs.chunkId == rhs.chunkId && lhs.objectId == rhs.objectId &&
           lhs.objectVersion == rhs.objectVersion && lhs.index == rhs.index &&
           lhs.logicalOffset == rhs.logicalOffset && lhs.size == rhs.size &&
           lhs.checksum == rhs.checksum && lhs.generation == rhs.generation &&
           lhs.formatVersion == rhs.formatVersion;
}

bool IsCanonicalId(const std::string& id)
{
    if (id.size() != 36) return false;
    for (size_t i = 0; i < id.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (id[i] != '-') return false;
        } else if (!((id[i] >= '0' && id[i] <= '9') ||
                     (id[i] >= 'a' && id[i] <= 'f'))) {
            return false;
        }
    }
    return true;
}

Status ValidateChecksum(const Checksum& checksum)
{
    if (checksum.algorithm != ChecksumAlgorithm::kSha256 || checksum.value.size() != 64) {
        return {ErrorCode::kInvalidArgument, "SHA-256 checksum must contain 64 hex characters"};
    }
    for (char c : checksum.value) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            return {ErrorCode::kInvalidArgument, "checksum is not lowercase hexadecimal"};
        }
    }
    return Status::Ok();
}

Result<Checksum> Sha256File(int fd, uint64_t length)
{
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context(
        EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!context || EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1) {
        return Status(ErrorCode::kIoError, "cannot initialize SHA-256");
    }
    std::array<unsigned char, 64 * 1024> buffer{};
    uint64_t offset = 0;
    while (offset < length) {
        const size_t wanted = static_cast<size_t>(std::min<uint64_t>(buffer.size(), length - offset));
        const ssize_t n = ::pread(fd, buffer.data(), wanted, static_cast<off_t>(offset));
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            return Status(ErrorCode::kIoError,
                          n == 0 ? "unexpected end of file" : std::strerror(errno));
        }
        if (EVP_DigestUpdate(context.get(), buffer.data(), static_cast<size_t>(n)) != 1) {
            return Status(ErrorCode::kIoError, "EVP_DigestUpdate failed");
        }
        offset += static_cast<uint64_t>(n);
    }
    return Finish(context.get());
}

Result<Checksum> Sha256Bytes(const void* data, size_t length)
{
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context(
        EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!context || EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1 ||
        EVP_DigestUpdate(context.get(), data, length) != 1) {
        return Status(ErrorCode::kIoError, "cannot calculate SHA-256");
    }
    return Finish(context.get());
}

}  // namespace object_storage
