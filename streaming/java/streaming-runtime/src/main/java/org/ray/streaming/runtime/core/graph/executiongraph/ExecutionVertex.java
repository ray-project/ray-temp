package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.RayJavaActor;
import org.ray.api.id.ActorId;
import org.ray.streaming.runtime.config.master.ResourceConfig;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.master.JobRuntimeContext;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 * Physical vertex, correspond to {@link ExecutionJobVertex}.
 */
public class ExecutionVertex implements Serializable {

  /**
   * Unique id for execution vertex.
   */
  private final int vertexId;

  /**
   * Ordered index for execution vertex.
   */
  private final int vertexIndex;

  /**
   * Unique name generated by vertex name and index for execution vertex.
   */
  private final String vertexName;
  /**
   * Resources used by ExecutionVertex.
   */
  private final Map<String, Double> resources;

  private ExecutionVertexState state = ExecutionVertexState.TO_ADD;
  private Slot slot;
  private RayJavaActor<JobWorker> workerActor;
  private List<ExecutionEdge> inputEdges = new ArrayList<>();
  private List<ExecutionEdge> outputEdges = new ArrayList<>();

  public ExecutionVertex(int jobVertexId, int index, ExecutionJobVertex executionJobVertex) {
    this.vertexId = generateExecutionVertexId(jobVertexId, index);
    this.vertexIndex = index;
    this.vertexName = executionJobVertex.getJobVertexName() + "-" + vertexIndex;
    this.resources = generateResources(executionJobVertex.getRuntimeContext());
  }

  private int generateExecutionVertexId(int jobVertexId, int index) {
    return jobVertexId * 100000 + index;
  }

  public int getVertexId() {
    return vertexId;
  }

  public int getVertexIndex() {
    return vertexIndex;
  }

  public ExecutionVertexState getState() {
    return state;
  }

  public void setState(ExecutionVertexState state) {
    this.state = state;
  }

  public boolean is2Add() {
    return state == ExecutionVertexState.TO_ADD;
  }

  public boolean isRunning() {
    return state == ExecutionVertexState.RUNNING;
  }

  public boolean is2Delete() {
    return state == ExecutionVertexState.TO_DEL;
  }

  public RayJavaActor<JobWorker> getWorkerActor() {
    return workerActor;
  }

  public ActorId getWorkerActorId() {
    return workerActor.getId();
  }

  public void setWorkerActor(RayJavaActor<JobWorker> workerActor) {
    this.workerActor = workerActor;
  }

  public List<ExecutionEdge> getInputEdges() {
    return inputEdges;
  }

  public void setInputEdges(
      List<ExecutionEdge> inputEdges) {
    this.inputEdges = inputEdges;
  }

  public List<ExecutionEdge> getOutputEdges() {
    return outputEdges;
  }

  public void setOutputEdges(
      List<ExecutionEdge> outputEdges) {
    this.outputEdges = outputEdges;
  }

  public String getVertexName() {
    return vertexName;
  }

  public Map<String, Double> getResources() {
    return resources;
  }

  public Slot getSlot() {
    return slot;
  }

  public void setSlot(Slot slot) {
    this.slot = slot;
  }

  public void setSlotIfNotExist(Slot slot) {
    if (null == this.slot) {
      this.slot = slot;
    }
  }

  private Map<String, Double> generateResources(JobRuntimeContext runtimeContext) {
    Map<String, Double> resourceMap = new HashMap<>();
    ResourceConfig resourceConfig = runtimeContext.getConf().masterConfig.resourceConfig;
    if (resourceConfig.isTaskCpuResourceLimit()) {
      resourceMap.put(ResourceConfig.RESOURCE_KEY_CPU, resourceConfig.taskCpuResource());
    }
    if (resourceConfig.isTaskMemResourceLimit()) {
      resourceMap.put(ResourceConfig.RESOURCE_KEY_MEM, resourceConfig.taskMemResource());
    }
    return resourceMap;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("vertexId", vertexId)
        .add("vertexIndex", vertexIndex)
        .add("vertexName", vertexName)
        .add("resources", resources)
        .add("state", state)
        .add("slot", slot)
        .add("workerActor", workerActor)
        .toString();
  }
}
