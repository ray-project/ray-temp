package org.ray.streaming.runtime.context;

import static org.ray.streaming.util.ConfigKey.STREAMING_BATCH_MAX_COUNT;

import java.util.Map;

import org.ray.streaming.core.graph.ExecutionTask;

/**
 * Use Ray to implement RuntimeContext.
 */
public class RayRuntimeContext implements RuntimeContext {
  private int taskId;
  private int taskIndex;
  private int parallelism;
  private Long batchId;
  private final Long maxBatch;
  private Map<String, Object> config;

  public RayRuntimeContext(ExecutionTask executionTask, Map<String, Object> config,
      int parallelism) {
    this.taskId = executionTask.getTaskId();
    this.config = config;
    this.taskIndex = executionTask.getTaskIndex();
    this.parallelism = parallelism;
    if (config.containsKey(STREAMING_BATCH_MAX_COUNT)) {
      this.maxBatch = Long.valueOf(String.valueOf(config.get(STREAMING_BATCH_MAX_COUNT)));
    } else {
      this.maxBatch = Long.MAX_VALUE;
    }
  }

  @Override
  public int getTaskId() {
    return taskId;
  }

  @Override
  public int getTaskIndex() {
    return taskIndex;
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  @Override
  public Long getBatchId() {
    return batchId;
  }

  @Override
  public Long getMaxBatch() {
    return maxBatch;
  }

  public void setBatchId(Long batchId) {
    this.batchId = batchId;
  }
}
