#ifndef RAY_GCS_STORE_CLIENT_REDIS_STORE_CLIENT_H
#define RAY_GCS_STORE_CLIENT_REDIS_STORE_CLIENT_H

#include <memory>
#include <unordered_set>
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/gcs/store_client/store_client.h"

namespace ray {

namespace gcs {

class RedisStoreClient : public StoreClient {
 public:
  RedisStoreClient(const StoreClientOptions &options);

  virtual ~RedisStoreClient();

  Status Connect(std::shared_ptr<IOServicePool> io_service_pool) override;

  void Disconnect() override;

  Status AsyncPut(const std::string &table_name, const std::string &key,
                  const std::string &value, const StatusCallback &callback) override;

  Status AsyncPut(const std::string &table_name, const std::string &key,
                  const std::string &index, const std::string &value,
                  const StatusCallback &callback) override;

  Status AsyncGet(const std::string &table_name, const std::string &key,
                  const OptionalItemCallback<std::string> &callback) override;

  Status AsyncGetByIndex(const std::string &table_name, const std::string &index,
                         const MultiItemCallback<std::string> &callback) override;

  Status AsyncGetAll(
      const std::string &table_name,
      const ScanCallback<std::pair<std::string, std::string>> &callback) override;

  Status AsyncDelete(const std::string &table_name, const std::string &key,
                     const StatusCallback &callback) override;

  Status AsyncDeleteByIndex(const std::string &table_name, const std::string &index,
                            const StatusCallback &callback) override;

 private:
  Status DoPut(const std::string &key, const std::string &value,
               const StatusCallback &callback);

  std::shared_ptr<RedisClient> redis_client_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_REDIS_STORE_CLIENT_H
