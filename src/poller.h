/**
 * @file poller.h
 * @author Sigma711 (sigma711 at foxmail dot com)
 * @brief io_uring-based I/O multiplexing wrapper.
 * @date 2024-xx-xx
 *
 * Copyright (c) 2021 Sigma711
 *
 */

#ifndef TAOTU_SRC_POLLER_H_
#define TAOTU_SRC_POLLER_H_

#include <liburing.h>
#include <sys/uio.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include "non_copyable_movable.h"
#include "spin_lock.h"
#include "time_point.h"

#ifndef __linux__
#error "io_uring backend requires Linux."
#endif

namespace taotu {

class Eventer;

class Poller : NonCopyableMovable {
 public:
  typedef std::vector<Eventer*> EventerList;

  enum class OpType { kPoll, kRead, kWrite, kAccept, kTimeout, kNone };

  struct IoUringOp;
  typedef void (*CompletionFn)(struct io_uring_cqe* cqe, IoUringOp* op);
  typedef void (*ContextDeleter)(void* context);

  struct IoUringOp {
    enum class State { kInit, kInflight, kCanceled, kDone };
    OpType type{OpType::kNone};
    Eventer* eventer{nullptr};
    void* context{nullptr};
    int fd{-1};
    CompletionFn completion{nullptr};
    ContextDeleter context_deleter{nullptr};
    std::atomic<State> state{State::kInit};

    // Per-CQE flag: completion may set this to keep the provided recv buffer
    // (IORING_CQE_F_BUFFER) until it is explicitly returned later.
    bool skip_buf_release{false};

    // For write ops that use Poller's provided buffers (see
    // SubmitWriteProvidedBuffer). If the op gets canceled (completion cleared),
    // Poller will return the buffer using this metadata.
    static constexpr size_t kProvidedIovMax = 8;
    bool uses_provided_buffer{false};
    uint8_t provided_buf_count{0};
    std::array<uint16_t, kProvidedIovMax> provided_buf_ids{};
    std::array<struct iovec, kProvidedIovMax> provided_iovs{};
  };

  Poller();
  ~Poller();

  // Poll the completion queue, return current time, and fill active Eventers.
  TimePoint Poll(int timeout, EventerList* active_eventers);

  void AddEventer(Eventer* eventer);
  void ModifyEventer(Eventer* eventer);
  void RemoveEventer(Eventer* eventer);

  // Submit I/O operations directly.
  uint64_t SubmitRead(Eventer* eventer, struct iovec* iov, int iovcnt,
                      CompletionFn completion = nullptr, void* ctx = nullptr,
                      uint64_t key = 0,
                      ContextDeleter context_deleter = nullptr);
  uint64_t SubmitReadMultishot(Eventer* eventer, int buf_group,
                               CompletionFn completion = nullptr,
                               void* ctx = nullptr, uint64_t key = 0,
                               ContextDeleter context_deleter = nullptr);
  uint64_t SubmitWrite(Eventer* eventer, struct iovec* iov, int iovcnt,
                       CompletionFn completion = nullptr, void* ctx = nullptr,
                       uint64_t key = 0,
                       ContextDeleter context_deleter = nullptr);
  uint64_t SubmitWriteProvidedBuffer(Eventer* eventer, uint16_t buf_id,
                                     size_t offset, size_t len,
                                     CompletionFn completion = nullptr,
                                     void* ctx = nullptr, uint64_t key = 0,
                                     ContextDeleter context_deleter = nullptr);
  uint64_t SubmitWriteProvidedBuffers(Eventer* eventer, const uint16_t* buf_ids,
                                      const size_t* offsets, const size_t* lens,
                                      size_t count,
                                      CompletionFn completion = nullptr,
                                      void* ctx = nullptr, uint64_t key = 0,
                                      ContextDeleter context_deleter = nullptr);
  uint64_t SubmitAccept(int fd, struct sockaddr* addr, socklen_t* addrlen,
                        void* ctx, CompletionFn completion = nullptr,
                        uint64_t key = 0, bool multishot = false,
                        ContextDeleter context_deleter = nullptr);

  bool CancelOp(uint64_t user_data_key);

  // Limit CQE handling per poll to avoid starving timers.
  void SetCqeBatchLimit(size_t limit) { cqe_batch_limit_ = limit; }
  void SetCqeTimeBudgetUs(int64_t budget_us) {
    cqe_time_budget_us_ = budget_us;
  }

  bool UseSqpoll() const { return use_sqpoll_; }
  bool UseMultishotAccept() const { return use_multishot_accept_; }
  bool BuffersRegistered() const { return buffers_registered_; }
  bool UseBufRing() const { return use_buf_ring_; }
  size_t BufferCount() const { return kBufCount; }
  // The buffer pointer is valid while Poller's provided-buffer pool is
  // registered. For recv-multishot buffers, it must be returned via
  // ReturnBuffer() when no longer needed (Poller auto-returns it unless
  // completion sets IoUringOp::skip_buf_release=true).
  char* GetBuffer(uint16_t id);
  void ReturnBuffer(uint16_t id);
  bool TryLeaseBuffer(uint16_t id);
  static constexpr int kBufferGroupId = 1;
  static constexpr size_t kBufSize = 64 * 1024;
  static constexpr size_t kBufCount = 256;

 private:
  static uint64_t EncodeOp(IoUringOp* op);
  static IoUringOp* DecodeOp(uint64_t token);
  IoUringOp* AcquireOp(OpType type, Eventer* eventer, void* ctx, int fd,
                       CompletionFn completion, ContextDeleter context_deleter);
  void RecycleOp(IoUringOp* op);
  void CleanupOpContext(IoUringOp* op);

  void SubmitPoll(Eventer* eventer);
  void CancelPoll(Eventer* eventer);
  void HandleCqe(struct io_uring_cqe* cqe, const TimePoint& now,
                 EventerList* active_eventers);
  void SubmitPending(bool force = false);
  void RegisterBuffers();
  void UnregisterBuffers();
  void ReleaseBufferFromCqe(struct io_uring_cqe* cqe);

  struct io_uring ring_;
  bool use_sqpoll_{false};
  bool use_multishot_accept_{true};
  bool buffers_registered_{false};
  bool use_buf_ring_{false};
  struct io_uring_buf_ring* buf_ring_{nullptr};
  unsigned buf_ring_entries_{0};
  unsigned buf_ring_mask_{0};
  // Large provided-buffer pool for recv-multishot. Stored on heap to avoid
  // inflating Poller size (EventManager is often stack-allocated in tests).
  // Not value-initialized to avoid touching (zeroing) large memory on Poller
  // construction.
  std::unique_ptr<char[]> buffers_{};
  std::array<uint8_t, kBufCount> leased_buffers_{};  // 0/1
  size_t leased_buffer_count_{0};
  size_t leased_buffer_limit_{0};
  std::vector<IoUringOp*> op_pool_;
  size_t op_pool_limit_{1U << 16};
  size_t submit_batch_{1};
  size_t cqe_batch_limit_{1024};
  int64_t cqe_time_budget_us_{1000};
};

}  // namespace taotu

#endif  // TAOTU_SRC_POLLER_H_
