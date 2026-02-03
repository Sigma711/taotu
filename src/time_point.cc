/**
 * @file time_point.cc
 * @author Sigma711 (sigma711 at foxmail dot com)
 * @brief Implementation of class "TimePoint" which is the encapsulation of one
 * time point for the timer.
 * @date 2021-12-06
 *
 * @copyright Copyright (c) 2021 Sigma711
 *
 */

#include "time_point.h"

#include <stddef.h>

#include <utility>

namespace taotu {
namespace {
thread_local bool g_now_cache_enabled = false;
thread_local int64_t g_now_cache_us = 0;
}  // namespace

TimePoint::NowCacheGuard::NowCacheGuard(int64_t now_microseconds)
    : old_enabled_(g_now_cache_enabled), old_now_microseconds_(g_now_cache_us) {
  g_now_cache_enabled = true;
  g_now_cache_us = now_microseconds;
}

TimePoint::NowCacheGuard::~NowCacheGuard() {
  g_now_cache_enabled = old_enabled_;
  g_now_cache_us = old_now_microseconds_;
}

void TimePoint::NowCacheGuard::Update(int64_t now_microseconds) {
  g_now_cache_us = now_microseconds;
}

TimePoint::TimePoint() : time_point_microseconds_(FNow()), context_(0) {}
TimePoint::TimePoint(int64_t duration_microseconds, bool repeated)
    : time_point_microseconds_(FNow() + duration_microseconds),
      context_(repeated ? duration_microseconds : 0) {}
TimePoint::TimePoint(int64_t duration_microseconds,
                     const TimePoint& start_time_point, bool repeated)
    : time_point_microseconds_(start_time_point.GetMicroseconds() +
                               duration_microseconds),
      context_(repeated ? duration_microseconds : 0) {}
TimePoint::TimePoint(int64_t absolute_microseconds, int)
    : time_point_microseconds_(absolute_microseconds), context_(0) {}

int64_t TimePoint::GetMicroseconds() const { return time_point_microseconds_; }

int64_t TimePoint::GetMillisecond() const {
  return time_point_microseconds_ / 1000;
}

void TimePoint::SetTaskContinueCallback(std::function<bool()> IsContinue) {
  if (0 != context_) {
    IsContinue_ = std::move(IsContinue);
  }
}
std::function<bool()> TimePoint::GetTaskContinueCallback() const {
  if (0 != context_) {
    return IsContinue_;
  }
  return std::function<bool()>{};
}

int64_t TimePoint::FNow() {
  if (g_now_cache_enabled) {
    return g_now_cache_us;
  }
  return FNowRaw();
}

int64_t TimePoint::FNowRaw() {
  struct timeval tv;
  ::gettimeofday(&tv, NULL);
  return static_cast<int64_t>(tv.tv_sec * 1000 * 1000 + tv.tv_usec);
}

TimePoint TimePoint::FromMicroseconds(int64_t absolute_microseconds) {
  return TimePoint(absolute_microseconds, 0);
}

}  // namespace taotu
