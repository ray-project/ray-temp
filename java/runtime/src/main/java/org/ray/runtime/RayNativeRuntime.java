package org.ray.runtime;

import java.util.HashMap;
import java.util.Map;
import org.ray.api.id.JobId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.gcs.RedisClient;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.runner.RunManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native runtime for cluster mode.
 */
public final class RayNativeRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayNativeRuntime.class);

  private RunManager manager = null;

  public RayNativeRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  @Override
  public void start() {
    // Reset library path at runtime.
    resetLibraryPath();

    if (rayConfig.getRedisAddress() == null) {
      manager = new RunManager(rayConfig);
      manager.startRayProcesses(true);
    }

    gcsClient = new GcsClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);

    if (rayConfig.getJobId() == JobId.NIL) {
      rayConfig.setJobId(gcsClient.nextJobId());
    }
    // TODO(qwang): Get object_store_socket_name and raylet_socket_name from Redis.
    worker = new Worker(rayConfig.workerMode, this, functionManager,
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName,
        rayConfig.workerMode == WorkerType.DRIVER ? rayConfig.getJobId() : JobId.NIL);

    // register
    registerWorker();

    LOGGER.info("RayNativeRuntime started with store {}, raylet {}",
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName);
  }

  @Override
  public void shutdown() {
    if (null != manager) {
      manager.cleanup();
    }
    worker.destroy();
  }

  /**
   * Register this worker or driver to GCS.
   */
  private void registerWorker() {
    RedisClient redisClient = new RedisClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);
    Map<String, String> workerInfo = new HashMap<>();
    String workerId = new String(worker.getWorkerContext().getCurrentWorkerId().getBytes());
    if (rayConfig.workerMode == WorkerType.DRIVER) {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("driver_id", workerId);
      workerInfo.put("start_time", String.valueOf(System.currentTimeMillis()));
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      workerInfo.put("name", System.getProperty("user.dir"));
      //TODO: worker.redis_client.hmset(b"Drivers:" + worker.workerId, driver_info)
      redisClient.hmset("Drivers:" + workerId, workerInfo);
    } else {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      //TODO: b"Workers:" + worker.workerId,
      redisClient.hmset("Workers:" + workerId, workerInfo);
    }
  }
}
