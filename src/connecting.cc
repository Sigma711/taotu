/**
 * @file connecting.cc
 * @author Sigma711 (sigma711 at foxmail dot com)
 * @brief Implementation of class "Connecting" which is a TCP connection
 * (status).
 * @date 2021-12-27
 *
 * @copyright Copyright (c) 2021 Sigma711
 *
 */

#include "connecting.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "event_manager.h"
#include "logger.h"

namespace taotu {
namespace {
const char* StrError(int err, char* buf, size_t len) {
#if defined(_GNU_SOURCE)
  char* msg = ::strerror_r(err, buf, len);
  if (msg == nullptr || *msg == '\0') {
    ::snprintf(buf, len, "errno(%d)", err);
    return buf;
  }
  return msg;
#else
  if (::strerror_r(err, buf, len) != 0) {
    ::snprintf(buf, len, "errno(%d)", err);
  }
  if (*buf == '\0') {
    ::snprintf(buf, len, "errno(%d)", err);
  }
  return buf;
#endif
}
}  // namespace

Connecting::Connecting(EventManager* event_manager, int socket_fd,
                       const NetAddress& local_address,
                       const NetAddress& peer_address)
    : event_manager_(event_manager),
      socketer_(socket_fd),
      eventer_(event_manager->GetPoller(), socket_fd),
      local_address_(local_address),
      peer_address_(peer_address),
      state_(ConnectionState::kConnecting) {
  socketer_.SetKeepAlive(true);
  eventer_.RegisterReadCallback(
      [this](TimePoint receive_time) { this->DoReading(receive_time); });
  eventer_.RegisterWriteCallback([this] { this->DoWriting(); });
  eventer_.RegisterCloseCallback([this] { this->DoClosing(); });
  eventer_.RegisterErrorCallback([this] { this->DoWithError(); });
  LOG_DEBUG("The TCP connection with fd(%d) is being created.", socket_fd);
}
Connecting::~Connecting() {
  CancelPendingIo();
  LOG_DEBUG("The TCP connection with fd(%d) is closing.", Fd());
}

void Connecting::DoReading(TimePoint receive_time) {
  (void)receive_time;
  if (!read_in_flight_) {
    SubmitReadOnce();
  }
}

void Connecting::OnReadComplete(struct io_uring_cqe* cqe,
                                Poller::IoUringOp* op) {
  auto* ctx = static_cast<ReadContext*>(op->context);
  if (!ctx || ctx->self == nullptr) {
    op->context = nullptr;
    return;
  }
  auto* connecting = ctx->self;
  ssize_t res = cqe->res;
  int err = res < 0 ? -res : 0;
  LOG_DEBUG("Read complete fd(%d) res(%zd) err(%d)", connecting->Fd(), res,
            err);
  bool more = (cqe->flags & IORING_CQE_F_MORE) != 0;
  bool has_buffer = (cqe->flags & IORING_CQE_F_BUFFER) != 0;
  if (ctx->multishot && has_buffer) {
    ctx->buf_id = static_cast<uint16_t>(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
  }
  if (ctx->multishot && !more && !has_buffer) {
    // recv-multishot ended without a selected buffer. This can happen on EOF,
    // transient ENOBUFS (buffer pool empty), or other errors. We must not
    // swallow EOF here; otherwise peer shutdown may hang in CLOSE-WAIT.
    connecting->read_in_flight_ = false;
    if (res == 0) {
      connecting->DoClosing();
      connecting->CompletePendingIo();
      if (connecting->read_ctx_ == ctx) {
        connecting->read_ctx_ = nullptr;
      }
      ctx->self = nullptr;
      op->context = nullptr;
      return;
    }
    if (res < 0 && err == ENOBUFS) {
      // For provided-buffer multishot recv, ENOBUFS means the buffer pool is
      // temporarily empty. Rearm and continue.
      connecting->SubmitReadOnce();
      connecting->CompletePendingIo();
      // SubmitReadOnce reuses ctx storage; keep ctx->self for the new in-flight
      // op.
      op->context = nullptr;
      return;
    }
    // Other errors: report and stop reading.
    if (res < 0 && err != ECANCELED) {
      LOG_ERROR("OnReadComplete(multishot-end) error: fd(%d) res(%zd) err(%d)",
                connecting->Fd(), res, err);
      connecting->DoWithError(err);
    }
    connecting->CompletePendingIo();
    if (connecting->read_ctx_ == ctx) {
      connecting->read_ctx_ = nullptr;
    }
    ctx->self = nullptr;
    op->context = nullptr;
    return;
  }
  if (!more) {
    connecting->read_in_flight_ = false;
  }
  if (res > 0) {
    bool rearmed = false;
    bool consumed = false;
    // Update the input buffer.
    if (ctx->multishot && has_buffer) {
      auto* buf =
          connecting->event_manager_->GetPoller()->GetBuffer(ctx->buf_id);
      if (buf) {
        if (connecting->OnBorrowedMessageCallback_) {
          consumed = connecting->OnBorrowedMessageCallback_(
              *connecting, buf, static_cast<size_t>(res), ctx->buf_id,
              TimePoint{});
          if (consumed) {
            op->skip_buf_release = true;
          } else {
            connecting->input_buffer_.Append(buf, static_cast<size_t>(res));
          }
        } else {
          connecting->input_buffer_.Append(buf, static_cast<size_t>(res));
        }
      } else {
        LOG_WARN("buffer id out of range(%u)", ctx->buf_id);
      }
    } else {
      size_t writable = ctx->writable;
      if (static_cast<size_t>(res) <= writable) {
        connecting->input_buffer_.RefreshW(static_cast<size_t>(res));
      } else {
        connecting->input_buffer_.RefreshW(writable);
        connecting->input_buffer_.Append(ctx->extra_buffer,
                                         static_cast<size_t>(res) - writable);
      }
    }
    if (!consumed && connecting->OnMessageCallback_) {
      connecting->OnMessageCallback_(*connecting, &connecting->input_buffer_,
                                     TimePoint{});
    }
    // Submit the next read continuously (re-armed after one-shot or
    // when multishot completes).
    if (!more) {
      connecting->SubmitReadOnce();
      rearmed = true;
    }
    if (!more) {
      connecting->CompletePendingIo();
      if (!rearmed) {
        if (connecting->read_ctx_ == ctx) {
          connecting->read_ctx_ = nullptr;
        }
        ctx->self = nullptr;
      }
      op->context = nullptr;
    }
  } else if (res == 0) {  // Peer closed.
    connecting->DoClosing();
    if (!more) {
      connecting->CompletePendingIo();
      if (connecting->read_ctx_ == ctx) {
        connecting->read_ctx_ = nullptr;
      }
      ctx->self = nullptr;
      op->context = nullptr;
    }
  } else {  // res < 0
    if (err == EAGAIN || err == EWOULDBLOCK || err == EINTR) {
      bool rearmed = false;
      if (!more) {
        connecting->SubmitReadOnce();
        rearmed = true;
      }
      if (!more) {
        connecting->CompletePendingIo();
        if (!rearmed) {
          if (connecting->read_ctx_ == ctx) {
            connecting->read_ctx_ = nullptr;
          }
          ctx->self = nullptr;
        }
        op->context = nullptr;
      }
    } else if (err == ECONNRESET || err == ECONNABORTED || err == EPIPE) {
      char errbuf[128];
      const char* err_str = StrError(err, errbuf, sizeof(errbuf));
      if (err_str == nullptr || *err_str == '\0') {
        err_str = "unknown";
      }
      LOG_INFO("Peer closed/reset the connection fd(%d) err(%d - %s)",
               connecting->Fd(), err, err_str);
      connecting->DoClosing();
      if (!more) {
        connecting->CompletePendingIo();
        if (connecting->read_ctx_ == ctx) {
          connecting->read_ctx_ = nullptr;
        }
        ctx->self = nullptr;
        op->context = nullptr;
      }
    } else {
      LOG_ERROR("OnReadComplete error: fd(%d) res(%zd) err(%d)",
                connecting->Fd(), res, err);
      connecting->DoWithError(err);
      if (!more) {
        connecting->CompletePendingIo();
        if (connecting->read_ctx_ == ctx) {
          connecting->read_ctx_ = nullptr;
        }
        ctx->self = nullptr;
        op->context = nullptr;
      }
    }
  }
}

void Connecting::OnWriteComplete(struct io_uring_cqe* cqe,
                                 Poller::IoUringOp* op) {
  auto* ctx = static_cast<WriteContext*>(op->context);
  if (!ctx || ctx->self == nullptr) {
    op->context = nullptr;
    return;
  }
  auto* connecting = ctx->self;
  connecting->write_in_flight_ = false;
  ssize_t res = cqe->res;
  int err = res < 0 ? -res : 0;
  LOG_DEBUG("Write complete fd(%d) res(%zd) err(%d)", connecting->Fd(), res,
            err);
  if (ctx->borrowed) {
    // Borrowed fixed buffer write (may contain multiple iovecs).
    auto* poller = connecting->event_manager_->GetPoller();
    if (res > 0) {
      size_t sent = static_cast<size_t>(res);
      if (connecting->borrowed_size_ == 0) {
        // Queue was cleared unexpectedly; return buffers best-effort.
        for (size_t i = 0; i < ctx->borrowed_iovcnt; ++i) {
          poller->ReturnBuffer(ctx->borrowed_ids[i]);
        }
      } else {
        for (size_t i = 0;
             i < ctx->borrowed_iovcnt && connecting->borrowed_size_ > 0; ++i) {
          BorrowedChunk* chunk =
              &connecting->borrowed_queue_[connecting->borrowed_head_];
          const size_t seg_len = ctx->borrowed_lens[i];
          if (sent >= seg_len) {
            sent -= seg_len;
            poller->ReturnBuffer(chunk->buf_id);
            connecting->borrowed_head_ =
                (connecting->borrowed_head_ + 1) % kBorrowedQueueCap;
            --connecting->borrowed_size_;
            continue;
          }
          // Partial within current buffer.
          chunk->off += static_cast<uint32_t>(sent);
          break;
        }
      }
      // Prefer owned pending output (if any), then borrowed queue.
      if (connecting->pending_output_buffer_.GetReadableBytes() > 0) {
        connecting->output_buffer_.Swap(connecting->pending_output_buffer_);
        connecting->SubmitWriteOnce();
        connecting->CompletePendingIo();
        if (connecting->write_ctx_ == ctx) {
          connecting->write_ctx_ = nullptr;
        }
        return;
      }
      if (connecting->borrowed_size_ > 0) {
        connecting->SubmitWriteOnce();
        connecting->CompletePendingIo();
        if (connecting->write_ctx_ == ctx) {
          connecting->write_ctx_ = nullptr;
        }
        return;
      }
      if (connecting->WriteCompleteCallback_) {
        connecting->WriteCompleteCallback_(*connecting);
      }
      if (Connecting::ConnectionState::kDisconnecting ==
              connecting->state_.load() &&
          connecting->output_buffer_.GetReadableBytes() == 0 &&
          connecting->pending_output_buffer_.GetReadableBytes() == 0 &&
          connecting->borrowed_size_ == 0) {
        connecting->socketer_.ShutdownWrite();
      }
    } else {
      if (err == EAGAIN || err == EWOULDBLOCK || err == EINTR) {
        connecting->SubmitWriteOnce();
        connecting->CompletePendingIo();
        if (connecting->write_ctx_ == ctx) {
          connecting->write_ctx_ = nullptr;
        }
        return;
      }
      // Fatal error: drop the borrowed buffers in this write attempt and
      // continue.
      if (connecting->borrowed_size_ == 0) {
        for (size_t i = 0; i < ctx->borrowed_iovcnt; ++i) {
          poller->ReturnBuffer(ctx->borrowed_ids[i]);
        }
      } else {
        for (size_t i = 0;
             i < ctx->borrowed_iovcnt && connecting->borrowed_size_ > 0; ++i) {
          BorrowedChunk* chunk =
              &connecting->borrowed_queue_[connecting->borrowed_head_];
          poller->ReturnBuffer(chunk->buf_id);
          connecting->borrowed_head_ =
              (connecting->borrowed_head_ + 1) % kBorrowedQueueCap;
          --connecting->borrowed_size_;
        }
      }
      LOG_ERROR("OnWriteComplete(borrowed) error: fd(%d) res(%zd) err(%d)",
                connecting->Fd(), res, err);
      connecting->DoWithError(err);
      if (connecting->pending_output_buffer_.GetReadableBytes() > 0) {
        connecting->output_buffer_.Swap(connecting->pending_output_buffer_);
        connecting->SubmitWriteOnce();
        connecting->CompletePendingIo();
        if (connecting->write_ctx_ == ctx) {
          connecting->write_ctx_ = nullptr;
        }
        return;
      }
      if (connecting->borrowed_size_ > 0) {
        connecting->SubmitWriteOnce();
        connecting->CompletePendingIo();
        if (connecting->write_ctx_ == ctx) {
          connecting->write_ctx_ = nullptr;
        }
        return;
      }
    }
    connecting->CompletePendingIo();
    if (connecting->write_ctx_ == ctx) {
      connecting->write_ctx_ = nullptr;
    }
    ctx->self = nullptr;
    op->context = nullptr;
    return;
  }
  if (res > 0) {
    connecting->output_buffer_.Refresh(static_cast<size_t>(res));
    if (connecting->output_buffer_.GetReadableBytes() > 0) {
      connecting->SubmitWriteOnce();
    } else {
      if (connecting->pending_output_buffer_.GetReadableBytes() > 0) {
        connecting->output_buffer_.Swap(connecting->pending_output_buffer_);
        connecting->SubmitWriteOnce();
        connecting->CompletePendingIo();
        if (connecting->write_ctx_ == ctx) {
          connecting->write_ctx_ = nullptr;
        }
        return;
      }
      if (connecting->borrowed_size_ > 0) {
        connecting->SubmitWriteOnce();
        connecting->CompletePendingIo();
        if (connecting->write_ctx_ == ctx) {
          connecting->write_ctx_ = nullptr;
        }
        return;
      }
      if (connecting->WriteCompleteCallback_) {
        connecting->WriteCompleteCallback_(*connecting);
      }
      if (Connecting::ConnectionState::kDisconnecting ==
              connecting->state_.load() &&
          connecting->output_buffer_.GetReadableBytes() == 0 &&
          connecting->pending_output_buffer_.GetReadableBytes() == 0 &&
          connecting->borrowed_size_ == 0) {
        connecting->socketer_.ShutdownWrite();
      }
    }
  } else {
    if (err == EAGAIN || err == EWOULDBLOCK || err == EINTR) {
      connecting->SubmitWriteOnce();
    } else {
      LOG_ERROR("OnWriteComplete error: fd(%d) res(%zd) err(%d)",
                connecting->Fd(), res, err);
      connecting->DoWithError(err);
    }
  }
  connecting->CompletePendingIo();
  if (connecting->write_ctx_ == ctx) {
    connecting->write_ctx_ = nullptr;
  }
  ctx->self = nullptr;
  op->context = nullptr;
}

void Connecting::SubmitReadOnce() {
  if (read_in_flight_) {
    return;
  }
  auto* ctx = &read_ctx_storage_;
  ctx->self = this;
  read_ctx_ = ctx;
  ctx->key = 0;
  ctx->buf_id = 0;
  auto* poller = event_manager_->GetPoller();
  read_in_flight_ = true;
#ifdef IORING_RECV_MULTISHOT
  if (poller->BuffersRegistered()) {
    // recv-multishot uses poller-owned provided buffers directly, so it does
    // not need the per-submit iovec/extra-buffer setup used by one-shot reads.
    ctx->multishot = true;
    ctx->writable = 0;
    ctx->extra_buffer = nullptr;
    ctx->extra_len = 0;
    uint64_t key = poller->SubmitReadMultishot(&eventer_, Poller::kBufferGroupId,
                                               &Connecting::OnReadComplete, ctx,
                                               0, nullptr);
    if (key == 0) {
      read_in_flight_ = false;
      read_cancel_key_ = 0;
      if (read_ctx_ == ctx) {
        read_ctx_ = nullptr;
      }
      ctx->self = nullptr;
      return;
    }
    ctx->key = key;
    read_cancel_key_ = key;
    BumpPendingIo();
    return;
  }
#endif
  ctx->multishot = false;
  ctx->extra_buffer = nullptr;
  ctx->extra_len = 0;
  ctx->writable = input_buffer_.GetWritableBytes();
  ctx->iov[0].iov_base = const_cast<char*>(input_buffer_.GetWritablePosition());
  ctx->iov[0].iov_len = ctx->writable;
  if (!poller->BuffersRegistered()) {
    if (!extra_read_buffer_) {
      extra_read_buffer_.reset(new char[64 * 1024]);
    }
    ctx->extra_buffer = extra_read_buffer_.get();
    ctx->extra_len = 64 * 1024;
  }
  ctx->iov[1].iov_base = ctx->extra_buffer;
  ctx->iov[1].iov_len = ctx->extra_len;
  int iovcnt;
  if (ctx->writable == 0) {
    iovcnt = 1;
    ctx->iov[0] = ctx->iov[1];
  } else {
    iovcnt = (ctx->extra_len > 0 && ctx->writable < ctx->extra_len) ? 2 : 1;
  }
  uint64_t key = poller->SubmitRead(&eventer_, ctx->iov.data(), iovcnt,
                                    &Connecting::OnReadComplete, ctx, 0,
                                    nullptr);
  if (key == 0) {
    read_in_flight_ = false;
    read_cancel_key_ = 0;
    if (read_ctx_ == ctx) {
      read_ctx_ = nullptr;
    }
    ctx->self = nullptr;
    return;
  }
  ctx->key = key;
  read_cancel_key_ = key;
  BumpPendingIo();
}
void Connecting::DoWriting() {
  if (!write_in_flight_ && output_buffer_.GetReadableBytes() > 0) {
    SubmitWriteOnce();
  }
}
void Connecting::SubmitWriteOnce() {
  if (write_in_flight_) {
    return;
  }
  const size_t readable = output_buffer_.GetReadableBytes();
  if (readable == 0 && borrowed_size_ == 0) {
    return;
  }
  auto* ctx = &write_ctx_storage_;
  ctx->self = this;
  write_ctx_ = ctx;
  ctx->key = 0;
  ctx->borrowed = false;
  ctx->borrowed_iovcnt = 0;
  auto* poller = event_manager_->GetPoller();

  // ctx->key = next_io_key_++; // Deprecated
  // write_cancel_key_ = ctx->key;
  write_in_flight_ = true;
  uint64_t key = 0;
  if (readable > 0) {
    ctx->to_send = readable;
    ctx->iov.iov_base = const_cast<char*>(output_buffer_.GetReadablePosition());
    ctx->iov.iov_len = ctx->to_send;
    key = poller->SubmitWrite(&eventer_, &ctx->iov, 1,
                              &Connecting::OnWriteComplete, ctx, 0, nullptr);
  } else {
    ctx->borrowed = true;
    const size_t iov_max = Poller::IoUringOp::kProvidedIovMax;
    const size_t cnt = borrowed_size_ < iov_max ? borrowed_size_ : iov_max;
    std::array<uint16_t, Poller::IoUringOp::kProvidedIovMax> ids{};
    std::array<size_t, Poller::IoUringOp::kProvidedIovMax> offs{};
    std::array<size_t, Poller::IoUringOp::kProvidedIovMax> lens{};
    size_t total = 0;
    for (size_t i = 0; i < cnt; ++i) {
      const size_t idx = (borrowed_head_ + i) % kBorrowedQueueCap;
      const BorrowedChunk& chunk = borrowed_queue_[idx];
      ids[i] = chunk.buf_id;
      offs[i] = static_cast<size_t>(chunk.off);
      lens[i] = static_cast<size_t>(chunk.len - chunk.off);
      ctx->borrowed_ids[i] = ids[i];
      ctx->borrowed_lens[i] = lens[i];
      total += lens[i];
    }
    ctx->borrowed_iovcnt = cnt;
    ctx->to_send = total;
    ctx->iov.iov_base = nullptr;
    ctx->iov.iov_len = 0;
    key = poller->SubmitWriteProvidedBuffers(
        &eventer_, ids.data(), offs.data(), lens.data(), cnt,
        &Connecting::OnWriteComplete, ctx, 0, nullptr);
  }
  if (key == 0) {
    write_in_flight_ = false;
    write_cancel_key_ = 0;
    if (write_ctx_ == ctx) {
      write_ctx_ = nullptr;
    }
    if (ctx->borrowed && borrowed_size_ > 0 && ctx->borrowed_iovcnt > 0) {
      // Fallback: keep correctness by copying and returning the buffers.
      for (size_t i = 0; i < ctx->borrowed_iovcnt && borrowed_size_ > 0; ++i) {
        BorrowedChunk* chunk = &borrowed_queue_[borrowed_head_];
        char* buf = poller->GetBuffer(chunk->buf_id);
        if (buf && chunk->off < chunk->len) {
          output_buffer_.Append(buf + chunk->off,
                                static_cast<size_t>(chunk->len - chunk->off));
        }
        poller->ReturnBuffer(chunk->buf_id);
        borrowed_head_ = (borrowed_head_ + 1) % kBorrowedQueueCap;
        --borrowed_size_;
      }
      if (output_buffer_.GetReadableBytes() > 0 && !write_in_flight_) {
        SubmitWriteOnce();
        // SubmitWriteOnce reuses ctx; keep ctx->self for the new in-flight op.
        return;
      }
    }
    ctx->self = nullptr;
    return;
  }
  ctx->key = key;
  write_cancel_key_ = key;
  BumpPendingIo();
}
void Connecting::DoClosing() {
  if (state_.load() != ConnectionState::kDisconnected) {
    LOG_DEBUG("Fd(%d) with state(\"%s\") is closing.", Fd(),
              GetConnectionStateInfo(state_).c_str());
    SetState(ConnectionState::kDisconnected);
    StopReadingWriting();
    CancelPendingIo();
    if (OnConnectionCallback_) {
      OnConnectionCallback_(*this);
    }
    if (CloseCallback_) {
      CloseCallback_(*this);
    }
    event_manager_->DeleteConnection(
        Fd());  // Postpone the real destroying to the end of this loop
  }
}
void Connecting::DoWithError() const { DoWithError(0); }

void Connecting::DoWithError(int err) const {
#ifdef __linux__
  int opt_val;
  auto opt_len = static_cast<socklen_t>(sizeof(opt_val));
  int saved_errno;
  if (err != 0) {
    saved_errno = err;
  } else if (::getsockopt(Fd(), SOL_SOCKET, SO_ERROR,
                          reinterpret_cast<void*>(&opt_val), &opt_len) < 0) {
    saved_errno = errno;
  } else {
    saved_errno = opt_val;
  }
#else
  int saved_errno = err != 0 ? err : errno;
#endif
  if (saved_errno == 0) {
    LOG_WARN("Fd(%d) error callback but SO_ERROR=0", Fd());
    return;
  }
  char errno_info[512];
  const char* err_str = StrError(saved_errno, errno_info, sizeof(errno_info));
  if (err_str == nullptr || *err_str == '\0') {
    err_str = "unknown";
  }
  LOG_ERROR("Fd(%d) gets an error -- errno(%d) -- %s!!!", Fd(), saved_errno,
            err_str);
}

void Connecting::OnEstablishing() {
  if (ConnectionState::kConnecting ==
      state_.load()) {  // This TCP connection can only be created once
    SetState(ConnectionState::kConnected);
    OnConnectionCallback_(*this);
    SubmitReadOnce();
  }
}

void Connecting::Send(const void* message, size_t msg_len) {
  if (ConnectionState::kDisconnected == state_.load()) {
    LOG_ERROR("Fd(%d) is disconnected, so give up sending the message!!!",
              Fd());
    return;
  }
  if (ConnectionState::kConnected == state_.load()) {
    size_t queued_len = output_buffer_.GetReadableBytes() +
                        pending_output_buffer_.GetReadableBytes();
    if (HighWaterMarkCallback_ && queued_len + msg_len >= high_water_mark_ &&
        queued_len < high_water_mark_) {
      HighWaterMarkCallback_(*this, queued_len + msg_len);
    }
    if (write_in_flight_) {
      pending_output_buffer_.Append(message, msg_len);
    } else {
      output_buffer_.Append(message, msg_len);
      SubmitWriteOnce();
    }
  }
}
void Connecting::Send(const std::string& message) {
  Send(static_cast<const void*>(message.c_str()), message.size());
}
void Connecting::Send(IoBuffer* io_buffer) {
  if (io_buffer == nullptr) {
    return;
  }
  size_t msg_len = io_buffer->GetReadableBytes();
  if (msg_len == 0) {
    return;
  }
  if (ConnectionState::kDisconnected == state_.load()) {
    LOG_ERROR("Fd(%d) is disconnected, so give up sending the message!!!",
              Fd());
    return;
  }
  if (ConnectionState::kConnected != state_.load()) {
    return;
  }
  size_t queued_len = output_buffer_.GetReadableBytes() +
                      pending_output_buffer_.GetReadableBytes();
  if (HighWaterMarkCallback_ && queued_len + msg_len >= high_water_mark_ &&
      queued_len < high_water_mark_) {
    HighWaterMarkCallback_(*this, queued_len + msg_len);
  }
  if (write_in_flight_) {
    if (pending_output_buffer_.GetReadableBytes() == 0) {
      pending_output_buffer_.Swap(*io_buffer);
    } else {
      pending_output_buffer_.Append(io_buffer->GetReadablePosition(), msg_len);
      io_buffer->RefreshRW();
    }
    return;
  }
  if (output_buffer_.GetReadableBytes() == 0) {
    output_buffer_.Swap(*io_buffer);
    SubmitWriteOnce();
    return;
  }
  output_buffer_.Append(io_buffer->GetReadablePosition(), msg_len);
  io_buffer->RefreshRW();
  SubmitWriteOnce();
}

bool Connecting::SendBorrowed(uint16_t buf_id, size_t len) {
  if (len == 0) {
    return false;
  }
  if (ConnectionState::kDisconnected == state_.load()) {
    return false;
  }
  if (ConnectionState::kConnected != state_.load()) {
    return false;
  }
  auto* poller = event_manager_->GetPoller();
  if (!poller->BuffersRegistered()) {
    return false;
  }
  if (poller->GetBuffer(buf_id) == nullptr) {
    return false;
  }
  if (borrowed_size_ >= kBorrowedQueueCap) {
    return false;
  }
  if (!poller->TryLeaseBuffer(buf_id)) {
    return false;
  }
  const size_t tail = (borrowed_head_ + borrowed_size_) % kBorrowedQueueCap;
  borrowed_queue_[tail].buf_id = buf_id;
  borrowed_queue_[tail].len = static_cast<uint32_t>(len);
  borrowed_queue_[tail].off = 0;
  ++borrowed_size_;
  if (!write_in_flight_) {
    SubmitWriteOnce();
  }
  return true;
}

void Connecting::ShutDownWrite() {
  if (ConnectionState::kConnected == state_.load()) {
    SetState(ConnectionState::kDisconnecting);
    if (!eventer_
             .HasWriteEvents()) {  // If the writing event is not paid attention
                                   // to, shut down the writing end (this end)
      socketer_.ShutdownWrite();
    }
  }
}

void Connecting::ForceClose() {
  if (ConnectionState::kDisconnected != state_.load()) {
    SetState(ConnectionState::kDisconnecting);
    DoClosing();
  }
}

// FIXME: Make it be effective in the condition that the connection has been
// destroyed.
void Connecting::ForceCloseAfter(int64_t delay_microseconds) {
  if (ConnectionState::kDisconnected != state_.load()) {
    event_manager_->RunAfter(delay_microseconds,
                             [this]() { this->ForceClose(); });
  }
}

void Connecting::CancelPendingIo() {
  std::array<uint16_t, Poller::IoUringOp::kProvidedIovMax>
      inflight_borrowed_ids{};
  size_t inflight_borrowed_cnt = 0;
  if (write_in_flight_ && write_ctx_ && write_ctx_->borrowed) {
    inflight_borrowed_cnt = write_ctx_->borrowed_iovcnt;
    for (size_t i = 0; i < inflight_borrowed_cnt; ++i) {
      inflight_borrowed_ids[i] = write_ctx_->borrowed_ids[i];
    }
  }
  if (read_in_flight_) {
    if (read_cancel_key_ != 0) {
      (void)event_manager_->GetPoller()->CancelOp(read_cancel_key_);
      read_cancel_key_ = 0;
    }
    if (read_ctx_) {
      read_ctx_->self = nullptr;
      read_ctx_ = nullptr;
    }
    CompletePendingIo();
    read_in_flight_ = false;
  }
  if (write_in_flight_) {
    if (write_cancel_key_ != 0) {
      (void)event_manager_->GetPoller()->CancelOp(write_cancel_key_);
      write_cancel_key_ = 0;
    }
    if (write_ctx_) {
      write_ctx_->self = nullptr;
      write_ctx_ = nullptr;
    }
    CompletePendingIo();
    write_in_flight_ = false;
  }
  if (borrowed_size_ > 0) {
    auto* poller = event_manager_->GetPoller();
    for (size_t i = 0; i < borrowed_size_; ++i) {
      const size_t idx = (borrowed_head_ + i) % kBorrowedQueueCap;
      bool skip = false;
      for (size_t j = 0; j < inflight_borrowed_cnt; ++j) {
        if (borrowed_queue_[idx].buf_id == inflight_borrowed_ids[j]) {
          skip = true;
          break;
        }
      }
      if (skip) {
        continue;
      }
      poller->ReturnBuffer(borrowed_queue_[idx].buf_id);
    }
    borrowed_head_ = 0;
    borrowed_size_ = 0;
  }
}

std::string Connecting::GetConnectionStateInfo(ConnectionState state) {
  switch (state) {
    case ConnectionState::kDisconnected:
      return "Disconnected";
    case ConnectionState::kConnecting:
      return "Connecting";
    case ConnectionState::kConnected:
      return "Connected";
    case ConnectionState::kDisconnecting:
      return "Disconnecting";
  }
  return std::string{};
}

}  // namespace taotu
