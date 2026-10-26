#include "Channel.h"
#include "EventLoop.h"
#include <sstream>
#include <poll.h>
#include "base/Logging.h"

using namespace mymuduo;
using namespace mymuduo::net;

const int Channel::kNoneEvent = 0;
const int Channel::kReadEvent = POLLIN | POLLPRI;
const int Channel::kWriteEvent = POLLOUT;

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop),
      fd_(fd),
      events_(0),
      revents_(0),
      index_(-1),
      eventHandling_(false),
      addedToLoop_(false),
      logHup_(true),
      tied_(false) {
}

Channel::~Channel() {
    assert(!eventHandling_);
    assert(!addedToLoop_);
    if (loop_->isInLoopThread()) {
        assert(!loop_->hasChannel(this));
    }
}

void Channel::update() {
    addedToLoop_ = true;
    loop_->updateChannel(this);  // 通知 EventLoop 更新注册信息。
}

void Channel::remove() {
    assert(isNoneEvent());
    addedToLoop_ = false;
    loop_->removeChannel(this);
}

void Channel::tie(const std::shared_ptr<void>& obj) {
    tie_ = obj;
    tied_ = true;
}

void Channel::handleEvent(Timestamp receiveTime) {
    if (tied_) {
        std::shared_ptr<void> guard = tie_.lock();
        if (guard) {
            handleEventWithGuard(receiveTime);
        }
    } else {
        handleEventWithGuard(receiveTime);
    }
}

/*
POLLHUP：表示挂断相关事件，但代码只有在同时没有 POLLIN 时才调用关闭回调。如果同时存在可读事件，仍可能需要读取剩余数据。
POLLERR：表示发生错误相关事件，代码会尝试调用错误回调。
POLLNVAL：通常表示 fd 无效，例如文件描述符没有被正确打开或已不再有效。
POLLRDHUP：表示对端关闭了连接的写方向，是 Linux 常用的半关闭检测标志。
*/

void Channel::handleEventWithGuard(Timestamp receiveTime) {
    eventHandling_ = true;
    LOG_TRACE << reventsToString();

    if ((revents_ & POLLHUP) && !(revents_ & POLLIN)) {
        if (logHup_) {
            LOG_WARN << "fd = " << fd_ << " Channel::handle_event() POLLHUP";
        }
        if (closeCallback_) closeCallback_();
    }

    if (revents_ & POLLNVAL) {
        LOG_WARN << "fd = " << fd_ << " Channel::handle_event() POLLNVAL";
    }

    if (revents_ & (POLLERR | POLLNVAL)) {
        if (errorCallback_) errorCallback_();
    }

    if (revents_ & (POLLIN | POLLPRI | POLLRDHUP)) {
        if (readCallback_) readCallback_(receiveTime);
    }

    if (revents_ & POLLOUT) {
        if (writeCallback_) writeCallback_();
    }

    eventHandling_ = false;
}

string Channel::reventsToString() const {
    return eventsToString(fd_, revents_);
}

string Channel::eventsToString() const {
    return eventsToString(fd_, events_);
}

string Channel::eventsToString(int fd, int ev) {
    std::ostringstream oss;
    oss << fd << ": ";
    if (ev & POLLIN)
        oss << "IN ";
    if (ev & POLLPRI)
        oss << "PRI ";
    if (ev & POLLOUT)
        oss << "OUT ";
    if (ev & POLLHUP)
        oss << "HUP ";
    if (ev & POLLRDHUP)
        oss << "RDHUP ";
    if (ev & POLLERR)
        oss << "ERR ";
    if (ev & POLLNVAL)
        oss << "NVAL ";

    return oss.str();
} 