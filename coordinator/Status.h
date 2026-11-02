#ifndef OBJECT_STORAGE_COORDINATOR_STATUS_H
#define OBJECT_STORAGE_COORDINATOR_STATUS_H

#include <optional>
#include <string>
#include <utility>

namespace object_storage::coordinator {

enum class Code {
    kOk,
    kInvalidArgument,
    kNotFound,
    kAlreadyExists,
    kFailedPrecondition,
    kResourceExhausted,
    kInternal,
};

struct Status {
    Code code{Code::kOk};
    std::string message;
    bool ok() const { return code == Code::kOk; }
};

template <typename T>
struct Result {
    Status status;
    std::optional<T> value;

    Result(Status error) : status(std::move(error)) {}
    Result(T result) : value(std::move(result)) {}
    bool ok() const { return status.ok(); }
};

}  // namespace object_storage::coordinator

#endif
