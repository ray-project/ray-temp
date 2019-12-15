#include "gcs_server.h"
#include "job_info_handler_impl.h"

namespace ray {
namespace gcs {

GcsServer::GcsServer(const ray::gcs::GcsServerConfig &config)
    : config_(config),
      rpc_server_(config.grpc_server_name, config.grpc_server_port,
                  config.grpc_server_thread_num) {}

GcsServer::~GcsServer() { Stop(); }

void GcsServer::Start() {
  // Init backend client.
  InitBackendClient();

  // Register rpc service.
  job_info_handler_ = InitJobInfoHandler();
  job_info_service_.reset(new rpc::JobInfoGrpcService(main_service_, *job_info_handler_));
  rpc_server_.RegisterService(*job_info_service_);

  // Run rpc server.
  rpc_server_.Run();

  // Run the event loop.
  // Using boost::asio::io_context::work to avoid ending the event loop when
  // there are no events to handle.
  boost::asio::io_context::work worker(main_service_);
  main_service_.run();
}

void GcsServer::Stop() {
  // Shutdown the rpc server
  rpc_server_.Shutdown();

  // Stop the event loop.
  main_service_.stop();
}

void GcsServer::InitBackendClient() {
  GcsClientOptions options(config_.redis_address, config_.redis_port,
                           config_.redis_password);
  redis_gcs_client_ = std::make_shared<RedisGcsClient>(options);
  auto status = redis_gcs_client_->Connect(main_service_);
  RAY_CHECK(status.ok()) << "Failed to init redis gcs client as " << status;
}

std::unique_ptr<rpc::JobInfoHandler> GcsServer::InitJobInfoHandler() {
  return std::unique_ptr<rpc::DefaultJobInfoHandler>(new rpc::DefaultJobInfoHandler());
}

}  // namespace gcs
}  // namespace ray
