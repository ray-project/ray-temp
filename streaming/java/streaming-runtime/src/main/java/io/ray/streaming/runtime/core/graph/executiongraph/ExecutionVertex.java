package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.streaming.api.Language;
import io.ray.streaming.jobgraph.VertexType;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.runtime.config.master.ResourceConfig;
import io.ray.streaming.runtime.core.resource.ContainerID;
import io.ray.streaming.runtime.core.resource.ResourceType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Physical vertex, correspond to {@link ExecutionJobVertex}.
 */
public class ExecutionVertex implements Serializable {

  /**
   * Unique id for execution vertex.
   */
  private final int executionVertexId;

  /**
   * Immutable field inherited from {@link ExecutionJobVertex}.
   */
  private final int executionJobVertexId;
  private final String executionJobVertexName;
  private final StreamOperator streamOperator;
  private final VertexType vertexType;
  private final Language language;
  private final long buildTime;

  /**
   * Resource used by ExecutionVertex.
   */
  private final Map<String, Double> resource;

  /**
   * Parallelism of current vertex's operator.
   */
  private int parallelism;

  /**
   * Ordered sub index for execution vertex in a execution job vertex.
   * Might be changed in dynamic scheduling.
   */
  private int executionVertexIndex;

  private ExecutionVertexState state = ExecutionVertexState.TO_ADD;

  /**
   * The id of the container which this vertex's worker actor belongs to.
   */
  private ContainerID containerId;

  /**
   * Worker actor handle.
   */
  private BaseActorHandle workerActor;

  /**
   * Op config + job config.
   */
  private Map<String, String> workerConfig;

  private List<ExecutionEdge> inputEdges = new ArrayList<>();
  private List<ExecutionEdge> outputEdges = new ArrayList<>();

  public ExecutionVertex(
      int globalIndex,
      int index,
      ExecutionJobVertex executionJobVertex,
      ResourceConfig resourceConfig) {
    this.executionVertexId = globalIndex;
    this.executionJobVertexId = executionJobVertex.getExecutionJobVertexId();
    this.executionJobVertexName = executionJobVertex.getExecutionJobVertexName();
    this.streamOperator = executionJobVertex.getStreamOperator();
    this.vertexType = executionJobVertex.getVertexType();
    this.language = executionJobVertex.getLanguage();
    this.buildTime = executionJobVertex.getBuildTime();
    this.parallelism = executionJobVertex.getParallelism();
    this.executionVertexIndex = index;
    this.resource = generateResources(resourceConfig);
    this.workerConfig = genWorkerConfig(executionJobVertex.getJobConfig());
  }

  private Map<String, String> genWorkerConfig(Map<String, String> jobConfig) {
    Map<String, String> workerConfig = new HashMap<>();
    workerConfig.putAll(jobConfig);
    return workerConfig;
  }

  public int getExecutionVertexId() {
    return executionVertexId;
  }

  /**
   * Unique name generated by execution job vertex name and index of current execution vertex.
   * e.g. 1-SourceOperator-3 (vertex index is 3)
   */
  public String getExecutionVertexName() {
    return executionJobVertexName + "-" + executionVertexIndex;
  }

  public int getExecutionJobVertexId() {
    return executionJobVertexId;
  }

  public String getExecutionJobVertexName() {
    return executionJobVertexName;
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

  public int getParallelism() {
    return parallelism;
  }

  public int getExecutionVertexIndex() {
    return executionVertexIndex;
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

  public BaseActorHandle getWorkerActor() {
    return workerActor;
  }

  public ActorId getWorkerActorId() {
    return workerActor.getId();
  }

  public void setWorkerActor(BaseActorHandle workerActor) {
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

  public List<ExecutionVertex> getInputVertices() {
    return inputEdges.stream()
        .map(ExecutionEdge::getSourceExecutionVertex)
        .collect(Collectors.toList());
  }

  public List<ExecutionVertex> getOutputVertices() {
    return outputEdges.stream()
        .map(ExecutionEdge::getTargetExecutionVertex)
        .collect(Collectors.toList());
  }

  public Map<String, Double> getResource() {
    return resource;
  }

  public Map<String, String> getWorkerConfig() {
    return workerConfig;
  }

  public long getBuildTime() {
    return buildTime;
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
      return this.executionVertexId == ((ExecutionVertex) obj).getExecutionVertexId();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(executionVertexId, outputEdges);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", executionVertexId)
        .add("name", getExecutionVertexName())
        .add("resources", resource)
        .add("state", state)
        .add("containerId", containerId)
        .add("workerActor", workerActor)
        .toString();
  }
}
