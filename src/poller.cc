/**
 * @file poller.cc
 * @author Sigma711 (sigma711 at foxmail dot com)
 * @brief io_uring-based I/O multiplexing implementation.
 * @date 2024-xx-xx
 *
 * @copyright Copyright (c) 2021 Sigma711
 *
 */

#include "poller.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <string>

#include "eventer.h"
#include "logger.h"

namespace taotu {
namespace {
constexpr uint32_t kDefaultEntries = 32768;
constexpr uint32_t kMinEntries = 1024;
constexpr size_t kDefaultSubmitBatch = 1;
constexpr size_t kMaxSubmitBatch = 256;
constexpr size_t kDefaultOpPoolLimit = 1U << 16;
constexpr size_t kMaxOpPoolLimit = 1U << 20;
constexpr size_t kDefaultBorrowedBufLimit = Poller::kBufCount / 2;

uint32_t GetIoUringEntries() {
  const char* env = ::getenv("TAOTU_IORING_ENTRIES");
  if (!env || *env == '\0') {
    return kDefaultEntries;
  }
  char* end = nullptr;
  uint64_t val = ::strtoull(env, &end, 10);
  if (end == env || val == 0) {
    return kDefaultEntries;
  }
  if (val > kDefaultEntries) {
    return kDefaultEntries;
  }
  if (val < kMinEntries) {
    return kMinEntries;
  }
  return static_cast<uint32_t>(val);
}

size_t GetSubmitBatch() {
  const char* env = ::getenv("TAOTU_IORING_SUBMIT_BATCH");
  if (!env || *env == '\0') {
    return kDefaultSubmitBatch;
  }
  char* end = nullptr;
  uint64_t val = ::strtoull(env, &end, 10);
  if (end == env || val == 0) {
    return kDefaultSubmitBatch;
  }
  if (val > kMaxSubmitBatch) {
    return kMaxSubmitBatch;
  }
  return static_cast<size_t>(val);
}

size_t GetOpPoolLimit() {
  const char* env = ::getenv("TAOTU_IORING_OP_POOL_LIMIT");
  if (!env || *env == '\0') {
    return kDefaultOpPoolLimit;
  }
  char* end = nullptr;
  uint64_t val = ::strtoull(env, &end, 10);
  if (end == env) {
    return kDefaultOpPoolLimit;
  }
  if (val > kMaxOpPoolLimit) {
    return kMaxOpPoolLimit;
  }
  return static_cast<size_t>(val);
}

size_t GetBorrowedBufLimit() {
  const char* env = ::getenv("TAOTU_IORING_BORROWED_BUFFER_LIMIT");
  if (!env || *env == '\0') {
    return kDefaultBorrowedBufLimit;
  }
  char* end = nullptr;
  uint64_t val = ::strtoull(env, &end, 10);
  if (end == env) {
    return kDefaultBorrowedBufLimit;
  }
  if (val > Poller::kBufCount) {
    return Poller::kBufCount;
  }
  return static_cast<size_t>(val);
}
}  // namespace

Poller::Poller() {
  ::memset(static_cast<void*>(&ring_), 0, sizeof(ring_));
  struct io_uring_params params {};
  bool want_sqpoll = false;
  const char* enable_sqpoll = ::getenv("TAOTU_ENABLE_SQPOLL");
  if (enable_sqpoll && *enable_sqpoll != '\0' && *enable_sqpoll != '0') {
    want_sqpoll = true;
  }
  const char* disable_sqpoll = ::getenv("TAOTU_DISABLE_SQPOLL");
  if (disable_sqpoll && *disable_sqpoll != '\0' && *disable_sqpoll != '0') {
    want_sqpoll = false;
  }
  const bool requested_sqpoll = want_sqpoll;
  if (requested_sqpoll) {
    params.flags = IORING_SETUP_SQPOLL;
  }
  uint32_t entries = GetIoUringEntries();
  int ret = -ENOMEM;
  while (entries >= kMinEntries) {
    ret = ::io_uring_queue_init_params(entries, &ring_, &params);
    if (ret == -ENOMEM && entries > kMinEntries) {
      entries /= 2;
      continue;
    }
    break;
  }
  if (requested_sqpoll && (ret == -EPERM || ret == -EINVAL)) {
    LOG_WARN("io_uring SQPOLL unavailable, fallback to default: %s",
             ::strerror(-ret));
    ::memset(static_cast<void*>(&ring_), 0, sizeof(ring_));
    ::memset(static_cast<void*>(&params), 0, sizeof(params));
    entries = GetIoUringEntries();
    ret = -ENOMEM;
    while (entries >= kMinEntries) {
      ret = ::io_uring_queue_init_params(entries, &ring_, &params);
      if (ret == -ENOMEM && entries > kMinEntries) {
        entries /= 2;
        continue;
      }
      break;
    }
  } else if (requested_sqpoll) {
    use_sqpoll_ = true;
  }
  if (ret < 0) {
    LOG_ERROR("io_uring_queue_init failed: %s", ::strerror(-ret));
    ::exit(EXIT_FAILURE);
  }
  if (use_sqpoll_) {
    LOG_DEBUG("io_uring initialized with SQPOLL.");
  }
  submit_batch_ = GetSubmitBatch();
  op_pool_limit_ = GetOpPoolLimit();
  leased_buffer_limit_ = GetBorrowedBufLimit();
  op_pool_.reserve(std::min(op_pool_limit_, static_cast<size_t>(2048)));
  struct io_uring_probe* probe = ::io_uring_get_probe_ring(&ring_);
  if (probe) {
    if (!::io_uring_opcode_supported(probe, IORING_OP_ACCEPT)) {
      use_multishot_accept_ = false;
      LOG_WARN("io_uring_accept not supported; multishot accept disabled.");
    }
    ::io_uring_free_probe(probe);
  }
  RegisterBuffers();
}

Poller::~Poller() {
  for (auto* op : op_pool_) {
    delete op;
  }
  op_pool_.clear();
  UnregisterBuffers();
  ::io_uring_queue_exit(&ring_);
}

uint64_t Poller::EncodeOp(IoUringOp* op) {
  return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(op));
}

Poller::IoUringOp* Poller::DecodeOp(uint64_t token) {
  if (token == 0) {
    return nullptr;
  }
  return reinterpret_cast<IoUringOp*>(static_cast<uintptr_t>(token));
}

Poller::IoUringOp* Poller::AcquireOp(OpType type, Eventer* eventer, void* ctx,
                                     int fd, CompletionFn completion,
                                     ContextDeleter context_deleter) {
  IoUringOp* op = nullptr;
  if (!op_pool_.empty()) {
    op = op_pool_.back();
    op_pool_.pop_back();
  } else {
    op = new IoUringOp;
  }
  op->type = type;
  op->eventer = eventer;
  op->context = ctx;
  op->fd = fd;
  op->completion = completion;
  op->context_deleter = context_deleter;
  op->skip_buf_release = false;
  op->uses_provided_buffer = false;
  op->provided_buf_count = 0;
  op->provided_iovs[0].iov_base = nullptr;
  op->provided_iovs[0].iov_len = 0;
  op->state.store(IoUringOp::State::kInflight, std::memory_order_relaxed);
  return op;
}

void Poller::RecycleOp(IoUringOp* op) {
  if (!op) {
    return;
  }
  op->type = OpType::kNone;
  op->eventer = nullptr;
  op->context = nullptr;
  op->fd = -1;
  op->completion = nullptr;
  op->context_deleter = nullptr;
  op->skip_buf_release = false;
  op->uses_provided_buffer = false;
  op->provided_buf_count = 0;
  op->provided_iovs[0].iov_base = nullptr;
  op->provided_iovs[0].iov_len = 0;
  op->state.store(IoUringOp::State::kInit, std::memory_order_relaxed);
  if (op_pool_.size() < op_pool_limit_) {
    op_pool_.push_back(op);
  } else {
    delete op;
  }
}

void Poller::AddEventer(Eventer* eventer) {
  eventer->poll_mask_ = eventer->Events();
  eventer->poll_armed_ = false;
  eventer->poll_token_ = 0;
  SubmitPoll(eventer);
  SubmitPending(true);
}

void Poller::ModifyEventer(Eventer* eventer) {
  eventer->poll_mask_ = eventer->Events();
  if (eventer->poll_mask_ == 0) {
    CancelPoll(eventer);
  } else {
    SubmitPoll(eventer);
  }
  SubmitPending(true);
}

void Poller::RemoveEventer(Eventer* eventer) {
  CancelPoll(eventer);
  eventer->poll_mask_ = 0;
  eventer->poll_armed_ = false;
  eventer->poll_token_ = 0;
}

uint64_t Poller::SubmitRead(Eventer* eventer, struct iovec* iov, int iovcnt,
                            CompletionFn completion, void* ctx, uint64_t key,
                            ContextDeleter context_deleter) {
  (void)key;
  struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe) {
    SubmitPending(true);
    sqe = ::io_uring_get_sqe(&ring_);
  }
  if (!sqe) {
    LOG_ERROR("io_uring_get_sqe failed when submit read fd(%d)", eventer->Fd());
    return 0;
  }
  auto* op = AcquireOp(OpType::kRead, eventer, ctx, eventer->Fd(), completion,
                       context_deleter);
  uint64_t token = EncodeOp(op);
  ::io_uring_prep_readv(sqe, eventer->Fd(), iov, iovcnt, 0);
  ::io_uring_sqe_set_data64(sqe, token);
  SubmitPending();
  return token;
}

uint64_t Poller::SubmitReadMultishot(Eventer* eventer, int buf_group,
                                     CompletionFn completion, void* ctx,
                                     uint64_t key,
                                     ContextDeleter context_deleter) {
#ifdef IORING_RECV_MULTISHOT
  (void)key;
  struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe) {
    SubmitPending(true);
    sqe = ::io_uring_get_sqe(&ring_);
  }
  if (!sqe) {
    LOG_ERROR("io_uring_get_sqe failed when submit recv-multishot fd(%d)",
              eventer->Fd());
    return 0;
  }
  auto* op = AcquireOp(OpType::kRead, eventer, ctx, eventer->Fd(), completion,
                       context_deleter);
  uint64_t token = EncodeOp(op);
  ::io_uring_prep_recv(sqe, eventer->Fd(), nullptr, 0, 0);
  sqe->ioprio |= IORING_RECV_MULTISHOT;
  ::io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT);
  sqe->buf_group = static_cast<__u16>(buf_group);
  ::io_uring_sqe_set_data64(sqe, token);
  SubmitPending();
  return token;
#else
  (void)eventer;
  (void)buf_group;
  (void)completion;
  (void)ctx;
  (void)key;
  LOG_WARN("recv-multishot not supported, skip submit.");
  return 0;
#endif
}

uint64_t Poller::SubmitWrite(Eventer* eventer, struct iovec* iov, int iovcnt,
                             CompletionFn completion, void* ctx, uint64_t key,
                             ContextDeleter context_deleter) {
  (void)key;
  struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe) {
    SubmitPending(true);
    sqe = ::io_uring_get_sqe(&ring_);
  }
  if (!sqe) {
    LOG_ERROR("io_uring_get_sqe failed when submit write fd(%d)",
              eventer->Fd());
    return 0;
  }
  auto* op = AcquireOp(OpType::kWrite, eventer, ctx, eventer->Fd(), completion,
                       context_deleter);
  uint64_t token = EncodeOp(op);
  ::io_uring_prep_writev(sqe, eventer->Fd(), iov, iovcnt, 0);
  ::io_uring_sqe_set_data64(sqe, token);
  SubmitPending();
  return token;
}

uint64_t Poller::SubmitWriteProvidedBuffer(Eventer* eventer, uint16_t buf_id,
                                           size_t offset, size_t len,
                                           CompletionFn completion, void* ctx,
                                           uint64_t key,
                                           ContextDeleter context_deleter) {
#ifdef IORING_RECV_MULTISHOT
  (void)key;
  if (!buffers_registered_) {
    return 0;
  }
  if (!buffers_) {
    return 0;
  }
  if (buf_id >= kBufCount) {
    LOG_WARN("SubmitWriteProvidedBuffer: buffer id out of range: %u", buf_id);
    return 0;
  }
  if (len == 0 || offset >= kBufSize || (offset + len) > kBufSize) {
    LOG_WARN("SubmitWriteProvidedBuffer: invalid range (id=%u off=%zu len=%zu)",
             buf_id, offset, len);
    return 0;
  }
  struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe) {
    SubmitPending(true);
    sqe = ::io_uring_get_sqe(&ring_);
  }
  if (!sqe) {
    LOG_ERROR("io_uring_get_sqe failed when submit provided write fd(%d)",
              eventer->Fd());
    return 0;
  }
  auto* op = AcquireOp(OpType::kWrite, eventer, ctx, eventer->Fd(), completion,
                       context_deleter);
  op->uses_provided_buffer = true;
  op->provided_buf_count = 1;
  op->provided_buf_ids[0] = buf_id;
  op->provided_iovs[0].iov_base = static_cast<void*>(
      buffers_.get() + (static_cast<size_t>(buf_id) * kBufSize) +
      static_cast<size_t>(offset));
  op->provided_iovs[0].iov_len = len;
  uint64_t token = EncodeOp(op);
  ::io_uring_prep_writev(sqe, eventer->Fd(), op->provided_iovs.data(), 1, 0);
  ::io_uring_sqe_set_data64(sqe, token);
  SubmitPending();
  return token;
#else
  (void)eventer;
  (void)buf_id;
  (void)offset;
  (void)len;
  (void)completion;
  (void)ctx;
  (void)key;
  (void)context_deleter;
  return 0;
#endif
}

uint64_t Poller::SubmitAccept(int fd, struct sockaddr* addr, socklen_t* addrlen,
                              void* ctx, CompletionFn completion, uint64_t key,
                              bool multishot, ContextDeleter context_deleter) {
  (void)key;
  struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe) {
    SubmitPending(true);
    sqe = ::io_uring_get_sqe(&ring_);
  }
  if (!sqe) {
    LOG_ERROR("io_uring_get_sqe failed when submit accept fd(%d)", fd);
    return 0;
  }
  auto* op =
      AcquireOp(OpType::kAccept, nullptr, ctx, fd, completion, context_deleter);
  uint64_t token = EncodeOp(op);
  if (multishot && use_multishot_accept_) {
#ifdef IORING_ACCEPT_MULTISHOT
    ::io_uring_prep_multishot_accept(sqe, fd, addr, addrlen,
                                     SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
    ::io_uring_prep_accept(sqe, fd, addr, addrlen,
                           SOCK_NONBLOCK | SOCK_CLOEXEC);
#endif
  } else {
    ::io_uring_prep_accept(sqe, fd, addr, addrlen,
                           SOCK_NONBLOCK | SOCK_CLOEXEC);
  }
  ::io_uring_sqe_set_data64(sqe, token);
  SubmitPending();
  return token;
}

bool Poller::CancelOp(uint64_t user_data_key) {
  if (user_data_key == 0) {
    return false;
  }
  auto* op = DecodeOp(user_data_key);
  if (!op) {
    return false;
  }
  // Mark canceled: keep op until its CQE arrives, so the kernel won't
  // touch freed context/iov memory.
  op->state.store(IoUringOp::State::kCanceled, std::memory_order_relaxed);
  op->eventer = nullptr;
  op->completion = nullptr;
  struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe) {
    SubmitPending(true);
    sqe = ::io_uring_get_sqe(&ring_);
  }
  if (!sqe) {
    LOG_ERROR("io_uring_get_sqe failed when cancel op");
    return true;
  }
  ::io_uring_prep_cancel64(sqe, user_data_key, 0);
  ::io_uring_sqe_set_data64(sqe, 0);  // Cancellation CQE needs no handling.
  SubmitPending();
  return true;
}
TimePoint Poller::Poll(int timeout, EventerList* active_eventers) {
  SubmitPending(true);
  struct __kernel_timespec ts {};
  struct __kernel_timespec* tsp = nullptr;
  if (timeout >= 0) {
    ts.tv_sec = timeout / 1000;
    ts.tv_nsec = (timeout % 1000) * 1000000;
    tsp = &ts;
  }

  struct io_uring_cqe* cqe = nullptr;
  int ret = ::io_uring_wait_cqe_timeout(&ring_, &cqe, tsp);
  if (ret == -ETIME) {
    SubmitPending(true);
    return TimePoint::FromMicroseconds(TimePoint::FNowRaw());
  }
  if (ret < 0) {
    LOG_ERROR("io_uring_wait_cqe_timeout failed: %s", ::strerror(-ret));
    SubmitPending(true);
    return TimePoint::FromMicroseconds(TimePoint::FNowRaw());
  }

  int64_t now_us = TimePoint::FNowRaw();
  const int64_t start_us = now_us;
  TimePoint::NowCacheGuard now_cache(now_us);
  HandleCqe(cqe, active_eventers);
  ::io_uring_cqe_seen(&ring_, cqe);

  // Continue draining all completed CQEs.
  const size_t limit = cqe_batch_limit_;
  const int64_t budget_us = cqe_time_budget_us_;
  size_t since_last_clock_check = 0;
  size_t handled = 1;
  while (limit == 0 || handled < limit) {
    // Avoid querying time for every CQE. We only refresh cached "now"
    // periodically, which keeps timer precision within one CQE batch chunk.
    if (budget_us > 0 && (++since_last_clock_check & 31U) == 0U) {
      now_us = TimePoint::FNowRaw();
      now_cache.Update(now_us);
      if ((now_us - start_us) >= budget_us) {
        break;
      }
    }
    ret = ::io_uring_peek_cqe(&ring_, &cqe);
    if (ret == -EAGAIN) {
      break;
    } else if (ret < 0) {
      LOG_ERROR("io_uring_peek_cqe failed: %s", ::strerror(-ret));
      break;
    }
    HandleCqe(cqe, active_eventers);
    ::io_uring_cqe_seen(&ring_, cqe);
    ++handled;
  }

  now_us = TimePoint::FNowRaw();
  now_cache.Update(now_us);
  SubmitPending(true);
  return TimePoint::FromMicroseconds(now_us);
}

uint64_t Poller::SubmitWriteProvidedBuffers(
    Eventer* eventer, const uint16_t* buf_ids, const size_t* offsets,
    const size_t* lens, size_t count, CompletionFn completion, void* ctx,
    uint64_t key, ContextDeleter context_deleter) {
#ifdef IORING_RECV_MULTISHOT
  (void)key;
  if (!buffers_registered_) {
    return 0;
  }
  if (!buffers_) {
    return 0;
  }
  if (!buf_ids || !offsets || !lens || count == 0) {
    return 0;
  }
  if (count > IoUringOp::kProvidedIovMax) {
    LOG_WARN("SubmitWriteProvidedBuffers: too many iovecs: %zu", count);
    return 0;
  }
  for (size_t i = 0; i < count; ++i) {
    const uint16_t bid = buf_ids[i];
    const size_t off = offsets[i];
    const size_t len = lens[i];
    if (bid >= kBufCount) {
      LOG_WARN("SubmitWriteProvidedBuffers: buffer id out of range: %u", bid);
      return 0;
    }
    if (len == 0 || off >= kBufSize || (off + len) > kBufSize) {
      LOG_WARN(
          "SubmitWriteProvidedBuffers: invalid range (id=%u off=%zu len=%zu)",
          bid, off, len);
      return 0;
    }
  }
  struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe) {
    SubmitPending(true);
    sqe = ::io_uring_get_sqe(&ring_);
  }
  if (!sqe) {
    LOG_ERROR("io_uring_get_sqe failed when submit provided write fd(%d)",
              eventer->Fd());
    return 0;
  }
  auto* op = AcquireOp(OpType::kWrite, eventer, ctx, eventer->Fd(), completion,
                       context_deleter);
  op->uses_provided_buffer = true;
  op->provided_buf_count = static_cast<uint8_t>(count);
  for (size_t i = 0; i < count; ++i) {
    const uint16_t bid = buf_ids[i];
    const size_t off = offsets[i];
    const size_t len = lens[i];
    op->provided_buf_ids[i] = bid;
    op->provided_iovs[i].iov_base = static_cast<void*>(
        buffers_.get() + (static_cast<size_t>(bid) * kBufSize) +
        static_cast<size_t>(off));
    op->provided_iovs[i].iov_len = len;
  }
  uint64_t token = EncodeOp(op);
  ::io_uring_prep_writev(sqe, eventer->Fd(), op->provided_iovs.data(),
                         static_cast<int>(count), 0);
  ::io_uring_sqe_set_data64(sqe, token);
  SubmitPending();
  return token;
#else
  (void)eventer;
  (void)buf_ids;
  (void)offsets;
  (void)lens;
  (void)count;
  (void)completion;
  (void)ctx;
  (void)key;
  (void)context_deleter;
  return 0;
#endif
}

void Poller::HandleCqe(struct io_uring_cqe* cqe, EventerList* active_eventers) {
  uint64_t token = cqe->user_data;
  if (token == 0) {
    ReleaseBufferFromCqe(cqe);
    return;  // cancellation or ignored CQE
  }
  IoUringOp* op = DecodeOp(token);
  if (!op) {
    ReleaseBufferFromCqe(cqe);
    return;
  }
  LOG_DEBUG("CQE type(%d) res(%d) user_data(%llu) completion(%p)",
            static_cast<int>(op->type), cqe->res,
            static_cast<unsigned long long>(token),
            reinterpret_cast<void*>(op->completion));
  bool keep_op = (cqe->flags & IORING_CQE_F_MORE) != 0;
  if (op->completion) {
    op->skip_buf_release = false;
    if ((op->type == OpType::kRead || op->type == OpType::kWrite) &&
        op->eventer == nullptr) {
      CleanupOpContext(op);
      ReleaseBufferFromCqe(cqe);
      op->state.store(IoUringOp::State::kDone, std::memory_order_relaxed);
      if (!keep_op) {
        RecycleOp(op);
      }
      return;
    }
    LOG_DEBUG("Call completion for type(%d)", static_cast<int>(op->type));
    op->completion(cqe, op);
    if (!op->skip_buf_release) {
      ReleaseBufferFromCqe(cqe);
    }
    if (!keep_op) {
      op->state.store(IoUringOp::State::kDone, std::memory_order_relaxed);
      RecycleOp(op);
    }
    return;
  }
  if (op->context != nullptr) {
    CleanupOpContext(op);
  }
  switch (op->type) {
    case OpType::kPoll: {
      auto* eventer = op->eventer;
      if (eventer == nullptr) {
        break;  // Eventer was removed, ignore this CQE
      }
      eventer->poll_armed_ = false;
      eventer->poll_token_ = 0;
      if (cqe->res >= 0) {
        eventer->ReceiveEvents(static_cast<uint32_t>(cqe->res));
        active_eventers->push_back(eventer);
      } else {
        LOG_ERROR("io_uring poll on fd(%d) failed: %s", eventer->Fd(),
                  ::strerror(-cqe->res));
      }
      SubmitPoll(eventer);
      break;
    }
    case OpType::kRead: {
      auto* eventer = op->eventer;
      if (eventer == nullptr) {
        ReleaseBufferFromCqe(cqe);
        break;  // Eventer was removed, ignore
      }
      Eventer::ReadResult rr{.bytes = cqe->res,
                             .err = cqe->res < 0 ? -cqe->res : 0};
      ReleaseBufferFromCqe(cqe);
      eventer->OnReadDone(rr, TimePoint{});
      break;
    }
    case OpType::kWrite: {
      auto* eventer = op->eventer;
      if (eventer == nullptr) {
        break;  // Eventer was removed, ignore
      }
      Eventer::WriteResult wr{.bytes = cqe->res,
                              .err = cqe->res < 0 ? -cqe->res : 0};
      eventer->OnWriteDone(wr);
      break;
    }
    case OpType::kAccept: {
      auto* eventer = op->eventer;
      if (eventer) {
        eventer->OnAcceptDone(static_cast<int>(cqe->res), nullptr, 0);
      }
      break;
    }
    case OpType::kTimeout:
    case OpType::kNone:
      break;
  }
  if (op->uses_provided_buffer && op->type == OpType::kWrite) {
    for (size_t i = 0; i < op->provided_buf_count; ++i) {
      ReturnBuffer(op->provided_buf_ids[i]);
    }
  }
  op->state.store(IoUringOp::State::kDone, std::memory_order_relaxed);
  if (!keep_op) {
    RecycleOp(op);
  }
}

void Poller::CleanupOpContext(IoUringOp* op) {
  if (!op || op->context == nullptr || op->context_deleter == nullptr) {
    return;
  }
  op->context_deleter(op->context);
  op->context = nullptr;
}

void Poller::SubmitPoll(Eventer* eventer) {
  if (eventer->poll_mask_ == 0 || eventer->poll_armed_) {
    return;
  }
  struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe) {
    SubmitPending(true);
    sqe = ::io_uring_get_sqe(&ring_);
  }
  if (!sqe) {
    LOG_ERROR("io_uring_get_sqe failed when arming fd(%d)", eventer->Fd());
    return;
  }
  auto* op = AcquireOp(OpType::kPoll, eventer, nullptr, eventer->Fd(), nullptr,
                       nullptr);
  uint64_t token = EncodeOp(op);
  ::io_uring_prep_poll_add(sqe, eventer->Fd(),
                           static_cast<unsigned>(eventer->poll_mask_));
  ::io_uring_sqe_set_data64(sqe, token);
  eventer->poll_armed_ = true;
  eventer->poll_token_ = token;
}

void Poller::CancelPoll(Eventer* eventer) {
  if (!eventer->poll_armed_ || eventer->poll_token_ == 0) {
    return;
  }
  (void)CancelOp(eventer->poll_token_);
  eventer->poll_armed_ = false;
  eventer->poll_token_ = 0;
}

void Poller::SubmitPending(bool force) {
  const unsigned ready = ::io_uring_sq_ready(&ring_);
  if (ready == 0) {
    return;
  }
  if (!force && ready < submit_batch_) {
    return;
  }
  int ret = ::io_uring_submit(&ring_);
  if (ret < 0) {
    LOG_ERROR("io_uring_submit failed: %s", ::strerror(-ret));
  }
}

void Poller::RegisterBuffers() {
#ifdef IORING_RECV_MULTISHOT
  const char* disable_multishot = ::getenv("TAOTU_DISABLE_RECV_MULTISHOT");
  if (disable_multishot && *disable_multishot != '\0' &&
      *disable_multishot != '0') {
    LOG_DEBUG("recv-multishot disabled by TAOTU_DISABLE_RECV_MULTISHOT.");
    return;
  }
  if (!buffers_) {
    buffers_.reset(new char[kBufCount * kBufSize]);
  }
  struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe) {
    LOG_WARN("io_uring_get_sqe failed when registering buffers, skip.");
    return;
  }
  struct io_uring_recvmsg_out out;  // dummy to silence potential warnings
  (void)out;
  ::io_uring_prep_provide_buffers(sqe, buffers_.get(), kBufSize, kBufCount,
                                  kBufferGroupId, 0);
  ::io_uring_sqe_set_data64(sqe, 0);  // ignored CQE
  int ret = ::io_uring_submit(&ring_);
  if (ret < 0) {
    LOG_WARN("io_uring_submit provide_buffers failed: %s", ::strerror(-ret));
    return;
  }
  struct io_uring_cqe* cqe = nullptr;
  ret = ::io_uring_wait_cqe(&ring_, &cqe);
  if (ret == 0 && cqe->res >= 0) {
    buffers_registered_ = true;
  } else {
    LOG_WARN("provide_buffers CQE failed: %s", ::strerror(-ret));
  }
  ::io_uring_cqe_seen(&ring_, cqe);
#endif
}

void Poller::UnregisterBuffers() {
#ifdef IORING_RECV_MULTISHOT
  if (!buffers_registered_) {
    return;
  }
  struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe) {
    return;
  }
  ::io_uring_prep_remove_buffers(sqe, kBufCount, kBufferGroupId);
  ::io_uring_sqe_set_data64(sqe, 0);
  SubmitPending(true);
  buffers_registered_ = false;
#endif
}

void Poller::ReleaseBufferFromCqe(struct io_uring_cqe* cqe) {
#ifdef IORING_RECV_MULTISHOT
  if (!buffers_registered_) {
    return;
  }
  if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
    return;
  }
  uint16_t bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
  if (bid >= kBufCount) {
    LOG_WARN("buffer id out of range: %u", bid);
    return;
  }
  ReturnBuffer(bid);
#endif
}

void Poller::ReturnBuffer(uint16_t bid) {
#ifdef IORING_RECV_MULTISHOT
  if (!buffers_registered_) {
    return;
  }
  if (!buffers_) {
    return;
  }
  if (bid >= kBufCount) {
    LOG_WARN("buffer id out of range: %u", bid);
    return;
  }
  if (leased_buffers_[bid]) {
    leased_buffers_[bid] = 0;
    if (leased_buffer_count_ > 0) {
      --leased_buffer_count_;
    }
  }
  struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe) {
    SubmitPending(true);
    sqe = ::io_uring_get_sqe(&ring_);
    if (!sqe) {
      LOG_WARN("io_uring_get_sqe failed when release buffer");
      return;
    }
  }
  ::io_uring_prep_provide_buffers(
      sqe, buffers_.get() + (static_cast<size_t>(bid) * kBufSize), kBufSize, 1,
      kBufferGroupId, bid);
  ::io_uring_sqe_set_data64(sqe, 0);
#endif
}

char* Poller::GetBuffer(uint16_t id) {
  if (!buffers_registered_ || !buffers_ || id >= kBufCount) {
    return nullptr;
  }
  return buffers_.get() + (static_cast<size_t>(id) * kBufSize);
}

bool Poller::TryLeaseBuffer(uint16_t id) {
#ifdef IORING_RECV_MULTISHOT
  if (!buffers_registered_) {
    return false;
  }
  if (id >= kBufCount) {
    return false;
  }
  if (leased_buffer_limit_ == 0) {
    return false;
  }
  if (leased_buffers_[id]) {
    return false;
  }
  if (leased_buffer_count_ >= leased_buffer_limit_) {
    return false;
  }
  leased_buffers_[id] = 1;
  ++leased_buffer_count_;
  return true;
#else
  (void)id;
  return false;
#endif
}

}  // namespace taotu
