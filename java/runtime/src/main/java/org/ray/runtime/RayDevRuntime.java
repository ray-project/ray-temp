package org.ray.runtime;

import org.ray.api.id.JobId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.objectstore.MockObjectStore;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.MockRayletClient;

import java.util.concurrent.atomic.AtomicLong;

public class RayDevRuntime extends AbstractRayRuntime {

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private MockObjectStore store;

  private AtomicLong jobCounter = new AtomicLong(0L);

  @Override
  public void start() {
    store = new MockObjectStore(this);
    if (rayConfig.getJobId().isNil()) {
      rayConfig.setJobId(nextJobId());
    }
    workerContext = new WorkerContext(rayConfig.workerMode,
        rayConfig.getJobId(), rayConfig.runMode);
    objectStoreProxy = new ObjectStoreProxy(this, null);
    rayletClient = new MockRayletClient(this, rayConfig.numberExecThreadsForDevRuntime);
  }

  @Override
  public void shutdown() {
    rayletClient.destroy();
  }

  public MockObjectStore getObjectStore() {
    return store;
  }

  @Override
  public Worker getWorker() {
    return ((MockRayletClient) rayletClient).getCurrentWorker();
  }

  private JobId nextJobId() {
    return JobId.fromLong(jobCounter.getAndIncrement());
  }
}
