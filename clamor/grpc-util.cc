#include "grpc-util.h"

#include <exception>

using grpc::ServerContext;

using namespace std;

string GRPCUtil::extract_client_id(ServerContext* context) {
  auto metadata = context->client_metadata();
  auto id_iter = metadata.find(GRPCUtil::CLIENT_ID_FIELD);
  if ( id_iter == metadata.end() ) {
    throw invalid_argument("Client did not provide ID");
  }
  
  string client_id((id_iter->second).begin(), (id_iter->second).end());

  return client_id;
}

Addr GRPCUtil::extract_client_addr(ServerContext* context) {
  auto metadata = context->client_metadata();
  auto ip_iter = metadata.find(GRPCUtil::CLIENT_IP_FIELD);
  if ( ip_iter == metadata.end() ) {
    throw invalid_argument("Client did not provide IP");
  }

  auto port_iter = metadata.find(GRPCUtil::CLIENT_PORT_FIELD);
  if ( port_iter == metadata.end() ) {
    throw invalid_argument("Client did not provide port");
  }

  string ip((ip_iter->second).begin(), (ip_iter->second).end());
  string port((port_iter->second).begin(), (port_iter->second).end());

  uint32_t int_port = std::stoi(port);

  Addr ret;
  ret.ip = ip;
  ret.port = int_port;

  return ret;
}
