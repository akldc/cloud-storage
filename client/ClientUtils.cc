#include "client/ObjectClientInternal.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <sys/random.h>

namespace object_storage {
namespace detail {

Fd::Fd(int fd) : value(fd) {}
Fd::~Fd() { if (value >= 0) ::close(value); }

std::string ErrnoText(const char* action)
{
    return std::string(action) + ": " + std::strerror(errno);
}

std::string RpcText(const grpc::Status& status)
{
    return "gRPC " + std::to_string(status.error_code()) + ": " + status.error_message();
}

void Deadline(grpc::ClientContext* context)
{
    context->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
}

std::string Hex(const unsigned char* bytes, size_t count)
{
    constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(count * 2);
    for (size_t i = 0; i < count; ++i) {
        result.push_back(digits[bytes[i] >> 4]);
        result.push_back(digits[bytes[i] & 0x0f]);
    }
    return result;
}

bool FinalDigest(EVP_MD_CTX* context, std::string* hex)
{
    std::array<unsigned char, EVP_MAX_MD_SIZE> bytes{};
    unsigned count = 0;
    if (EVP_DigestFinal_ex(context, bytes.data(), &count) != 1) return false;
    *hex = Hex(bytes.data(), count);
    return true;
}

// 
bool NewId(std::string* id, std::string* error)
{
    std::array<unsigned char, 16> bytes{};
    size_t got = 0;
    while (got < bytes.size()) {
        const ssize_t count = ::getrandom(bytes.data() + got, bytes.size() - got, 0);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) { *error = ErrnoText("getrandom"); return false; }
        got += static_cast<size_t>(count);
    }
    bytes[6] = static_cast<unsigned char>((bytes[6] & 0x0f) | 0x40);
    bytes[8] = static_cast<unsigned char>((bytes[8] & 0x3f) | 0x80);
    constexpr char hex[] = "0123456789abcdef";
    id->clear();
    id->reserve(36);
    for (size_t i = 0; i < bytes.size(); ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) id->push_back('-');
        id->push_back(hex[bytes[i] >> 4]);
        id->push_back(hex[bytes[i] & 0x0f]);
    }
    return true;
}

}  // namespace detail
}  // namespace object_storage
