package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import io.ray.api.RayActor;
import io.ray.api.id.ActorId;
import io.ray.streaming.api.Language;
import io.ray.streaming.jobgraph.VertexType;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.runtime.config.master.ResourceConfig;
import io.ray.streaming.runtime.core.resource.ContainerID;
import io.ray.streaming.runtime.core.resource.ResourceType;
import io.ray.streaming.runtime.worker.JobWorker;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Physical vertex, correspond to {@link ExecutionJobVertex}.
 */
public class ExecutionVertex implements Serializable {

  /**
   * Unique id for execution vertex.
   */
  private final int id;

  /**
   * Immutable field inherited from {@link ExecutionJobVertex}.
   */
  private final int jobVertexId;
  private final String jobVertexName;
  private final StreamOperator streamOperator;
  private final VertexType vertexType;
  private final Language language;

  /**
   * Resources used by ExecutionVertex.
   */
  private final Map<String, Double> resources;

  /**
   * Ordered sub index for execution vertex in a execution job vertex.
   * Might be changed in dynamic scheduling.
   */
  private int vertexIndex;

  private ExecutionVertexState state = ExecutionVertexState.TO_ADD;
  private ContainerID containerId;
  private RayActor<JobWorker> workerActor;
  private List<ExecutionEdge> inputEdges = new ArrayList<>();
  private List<ExecutionEdge> outputEdges = new ArrayList<>();

  public ExecutionVertex(
      int globalIndex,
      int index,
      ExecutionJobVertex executionJobVertex,
      ResourceConfig resourceConfig) {
    this.id = globalIndex;
    this.jobVertexId = executionJobVertex.getJobVertexId();
    this.jobVertexName = executionJobVertex.getJobVertexName();
    this.streamOperator = executionJobVertex.getStreamOperator();
    this.vertexType = executionJobVertex.getVertexType();
    this.language = executionJobVertex.getLanguage();
    this.vertexIndex = index;
    this.resources = generateResources(resourceConfig);
  }

  public int getId() {
    return id;
  }

  public int getJobVertexId() {
    return jobVertexId;
  }

  public String getJobVertexName() {
    return jobVertexName;
  }

  public StreamOperator getStreamOperator() {
    return streamOperator;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  public Language getLanguage() {
    return language;
  }

  public int getVertexIndex() {
    return vertexIndex;
  }

  /**
   * Unique name generated by vertex name and index for execution vertex.
   * e.g. 1-SourceOperator-3 (vertex index is 3)
   */
  public String getVertexName() {
    return jobVertexId + "-" + jobVertexName + "-" + vertexIndex;
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

  public RayActor<JobWorker> getWorkerActor() {
    return workerActor;
  }

  public ActorId getWorkerActorId() {
    return workerActor.getId();
  }

  public void setWorkerActor(RayActor<JobWorker> workerActor) {
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

  public Map<String, Double> getResources() {
    return resources;
  }

  public ContainerID getContainerId() {
    return containerId;
  }

  public void setContainerId(ContainerID containerId) {
    this.containerId = containerId;
  }

  public void setContainerIfNotExist(ContainerID containerId) {
    if (null == this.containerId) {
      this.containerId = containerId;
    }
  }

  private Map<String, Double> generateResources(ResourceConfig resourceConfig) {
    Map<String, Double> resourceMap = new HashMap<>();
    if (resourceConfig.isTaskCpuResourceLimit()) {
      resourceMap.put(ResourceType.CPU.name(), resourceConfig.taskCpuResource());
    }
    if (resourceConfig.isTaskMemResourceLimit()) {
      resourceMap.put(ResourceType.MEM.name(), resourceConfig.taskMemResource());
    }
    return resourceMap;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ExecutionVertex) {
      return this.id == ((ExecutionVertex)obj).getId();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, outputEdges);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("name", getVertexName())
        .add("resources", resources)
        .add("state", state)
        .add("containerId", containerId)
        .add("workerActor", workerActor)
        .toString();
  }
}
