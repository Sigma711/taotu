/**
 * @file rpc_server.h
 * @author Sigma711 (sigma711 at foxmail dot com)
 * @brief Declarations of class "RpcServer" which the encapsulation of the RPC
 * server.
 * @date 2023-08-13
 *
 * @copyright Copyright (c) 2023 Sigma711
 *
 */

#ifndef TAOTU_SRC_RPC_SERVER_H_
#define TAOTU_SRC_RPC_SERVER_H_

#include <unordered_map>

#include "connecting.h"
#include "event_manager.h"
#include "net_address.h"
#include "server.h"

namespace google {

namespace protobuf {

class Service;

}  // namespace protobuf

}  // namespace google

namespace taotu {

/**
 * @brief "RpcServer" offers the ability of server for RPC services generated by
 * Protobuf.
 *
 */
class RpcServer {
 public:
  typedef std::vector<EventManager*> EventManagers;

  RpcServer(EventManagers* event_managers, const NetAddress& listen_address);

  void RegisterService(::google::protobuf::Service*);
  void Start();

 private:
  void OnConnectionCallback(Connecting& connection);

  Server server_;
  std::unordered_map<std::string, ::google::protobuf::Service*> services_;
};

}  // namespace taotu

#endif  // !TAOTU_SRC_RPC_SERVER_H_
