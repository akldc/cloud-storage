#ifndef OBJECT_STORAGE_COMMON_STATUS_H
#define OBJECT_STORAGE_COMMON_STATUS_H

#include <optional>
#include <string>
#include <utility>

namespace object_storage {

enum class ErrorCode {
    kOk = 0,
    kNotFound,
    kAlreadyCommitted,
    kConflict,
    kOffsetMismatch,
    kChecksumMismatch,
    kStaleGeneration,
    kNoSpace,
    kCorrupt,
    kIoError,
    kInvalidArgument,
};

class Status {
public:
    Status() = default;
    Status(ErrorCode code, std::string message)
        : code_(code), message_(std::move(message)) {}

    static Status Ok() { return {}; }
    bool ok() const { return code_ == ErrorCode::kOk; }
    ErrorCode code() const { return code_; }
    const std::string& message() const { return message_; }

private:
    ErrorCode code_{ErrorCode::kOk};
    std::string message_;
};

template <typename T>
class Result {
public:
    Result(Status status) : status_(std::move(status)) {}
    Result(T value) : status_(), value_(std::move(value)) {}

    bool ok() const { return status_.ok(); }
    const Status& status() const { return status_; }
    T& value() & { return *value_; }
    const T& value() const& { return *value_; }
    T&& value() && { return std::move(*value_); }

private:
    Status status_;
    std::optional<T> value_;
};

}  // namespace object_storage

#endif
