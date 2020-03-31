package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.streaming.api.Language;
import org.ray.streaming.jobgraph.VertexType;
import org.ray.streaming.operator.StreamOperator;
import org.ray.streaming.runtime.config.master.ResourceConfig;
import org.ray.streaming.runtime.core.resource.ResourceKey;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.worker.JobWorker;

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
  private final int operatorId;
  private final String operatorName;
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
  private Slot slot;
  private RayActor<JobWorker> workerActor;
  private List<ExecutionEdge> inputEdges = new ArrayList<>();
  private List<ExecutionEdge> outputEdges = new ArrayList<>();

  public ExecutionVertex(
      int globalIndex,
      int index,
      ExecutionJobVertex executionJobVertex,
      ResourceConfig resourceConfig) {
    this.id = globalIndex;
    this.operatorId = executionJobVertex.getOperatorId();
    this.operatorName = executionJobVertex.getOperatorName();
    this.streamOperator = executionJobVertex.getStreamOperator();
    this.vertexType = executionJobVertex.getVertexType();
    this.language = executionJobVertex.getLanguage();
    this.vertexIndex = index;
    this.resources = generateResources(resourceConfig);
  }

  public int getId() {
    return id;
  }

  public int getOperatorId() {
    return operatorId;
  }

  public String getOperatorName() {
    return operatorName;
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
    return operatorId + "-" + operatorName + "-" + vertexIndex;
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

  private Map<String, Double> generateResources(ResourceConfig resourceConfig) {
    Map<String, Double> resourceMap = new HashMap<>();
    if (resourceConfig.isTaskCpuResourceLimit()) {
      resourceMap.put(ResourceKey.CPU.name(), resourceConfig.taskCpuResource());
    }
    if (resourceConfig.isTaskMemResourceLimit()) {
      resourceMap.put(ResourceKey.MEM.name(), resourceConfig.taskMemResource());
    }
    return resourceMap;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("name", getVertexName())
        .add("resources", resources)
        .add("state", state)
        .add("slot", slot)
        .add("workerActor", workerActor)
        .toString();
  }
}
