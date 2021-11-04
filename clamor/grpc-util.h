#ifndef GRPC_UTIL_H
#define GRPC_UTIL_H

#include <grpc/grpc.h>
#include <grpcpp/server_context.h>

#include "net-util.h"

#include <string>

namespace GRPCUtil {
  const std::string CLIENT_ID_FIELD = "client_id";
  const std::string CLIENT_IP_FIELD = "client_ip";
  const std::string CLIENT_PORT_FIELD = "client_port";
  
  std::string extract_client_id(grpc::ServerContext* context);
  Addr extract_client_addr(grpc::ServerContext* context);
}
  
#endif // GRPC_UTIL_H
