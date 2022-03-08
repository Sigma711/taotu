/**
 * @file echo.cc
 * @author Sigma711 (sigma711 at foxmail dot com)
 * @brief  // TODO:
 * @date 2022-02-22
 *
 * @copyright Copyright (c) 2022 Sigma711
 *
 */

#include "echo.h"

#include <functional>
#include <string>

EchoServer::EchoServer(const taotu::NetAddress& listen_address,
                       bool should_reuse_port)
    : server_(
          std::make_unique<taotu::Server>(listen_address, should_reuse_port)) {
  server_->SetMessageCallback(
      std::bind(&EchoServer::OnMessageCallback, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
}

void EchoServer::Start() { server_->Start(); }

void EchoServer::OnMessageCallback(taotu::Connecting& connection,
                                   taotu::IoBuffer* io_buffer,
                                   taotu::TimePoint time_point) {
  std::string message{io_buffer->RetrieveAllAsString()};
  taotu::LOG(taotu::logger::kDebug,
             "Fd(" + std::to_string(connection.Fd()) + ") is echoing " +
                 std::to_string(message.size()) + " bytes received at " +
                 std::to_string(time_point.GetMicroseconds()) + ".");
  connection.Send(message);
  connection.ForceClose();
}
