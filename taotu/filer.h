/**
 * @file filer.h
 * @author Sigma711 (sigma711 at foxmail dot com)
 * @brief Declaration of class "Filer" which is "single event operator".
 * @date 2021-11-28
 *
 * @copyright Copyright (c) 2021 Sigma711
 *
 */

#ifndef TAOTU_TAOTU_FILER_H_
#define TAOTU_TAOTU_FILER_H_

#include <functional>
#include <memory>

#include "event_manager.h"
#include "non_copyable_movable.h"
#include "time_point.h"

namespace taotu {

class EventerManager;

/**
 * @brief "Filer" is the encapsulation of "event" which is relavant to I/O
 * multiplexing and I/O callbacks.
 *
 */
class Filer : NonCopyableMovable {
 public:
  typedef std::function<void()> NormalCallback;
  typedef std::function<void(TimePoint)> ReadCallback;

  Filer(Eventer* eventer, int fd);
  ~Filer();

  // Handle all events
  void Work(TimePoint tp);

  void RegisterReadCallBack(ReadCallback cb);
  void RegisterWriteCallback(NormalCallback cb);
  void RegisterCloseCallback(NormalCallback cb);
  void RegisterErrorCallback(NormalCallback cb);

  int Fd() const;
  int Events() const;

  void ReceiveEvents(int in_events);

  bool HasNoEvent() const;
  bool HasReadEvents() const;
  bool HasWriteEvents() const;

  void EnableReadEvents();
  void DisableReadEvents();
  void EnableWriteEvents();
  void DisableWriteEvents();
  void DisableAllEvents();

  Eventer* HostEventer();

  void RemoveMyself();

 private:
  void UpdateEvents();

  // In <poll.h> or <sys/epoll.h>
  enum {
    kNoEvent = 0x0000,
    kReadEvents = 0x0001 | 0x0002,
    kWriteEvents = 0x0004,
  };

  Eventer* eventer_;

  const int fd_;

  // For receiving
  int in_events_;

  // For setting
  int out_events_;

  bool is_handling_;

  ReadCallback ReadCallback_;
  NormalCallback WriteCallback_;
  NormalCallback CloseCallback_;
  NormalCallback ErrorCallback_;
};

}  // namespace taotu

#endif  // !TAOTU_TAOTU_FILER_H_
