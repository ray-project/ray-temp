package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.RayActor;
import org.ray.streaming.jobgraph.JobVertex;
import org.ray.streaming.jobgraph.VertexType;
import org.ray.streaming.operator.StreamOperator;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 * Physical job vertex.
 *
 * <p>Execution job vertex is the physical form of {@link JobVertex} and
 * every execution job vertex is corresponding to a group of {@link ExecutionVertex}.
 */
public class ExecutionJobVertex {

  /**
   * Unique id for execution job vertex.
   */
  private final int jobVertexId;

  /**
   * Unique name generated by vertex name and index for execution job vertex.
   */
  private final String jobVertexName;
  private final StreamOperator streamOperator;
  private final VertexType vertexType;
  private int parallelism;
  private List<ExecutionVertex> executionVertices;

  private List<ExecutionJobEdge> inputEdges = new ArrayList<>();
  private List<ExecutionJobEdge> outputEdges = new ArrayList<>();

  public ExecutionJobVertex(JobVertex jobVertex) {
    this.jobVertexId = jobVertex.getVertexId();
    this.jobVertexName = generateVertexName(jobVertexId, jobVertex.getStreamOperator());
    this.streamOperator = jobVertex.getStreamOperator();
    this.vertexType = jobVertex.getVertexType();
    this.parallelism = jobVertex.getParallelism();
    this.executionVertices = createExecutionVertics();
  }

  private String generateVertexName(int vertexId, StreamOperator streamOperator) {
    return vertexId + "-" + streamOperator.getName();
  }

  private List<ExecutionVertex> createExecutionVertics() {
    List<ExecutionVertex> executionVertices = new ArrayList<>();
    for (int index = 1; index <= parallelism; index++) {
      executionVertices.add(new ExecutionVertex(jobVertexId, index, this));
    }
    return executionVertices;
  }

  public Map<Integer, RayActor<JobWorker>> getExecutionVertexWorkers() {
    Map<Integer, RayActor<JobWorker>> executionVertexWorkersMap = new HashMap<>();

    Preconditions.checkArgument(
        executionVertices != null && !executionVertices.isEmpty(),
        "Empty execution vertex.");
    executionVertices.stream().forEach(vertex -> {
      Preconditions.checkArgument(
          vertex.getWorkerActor() != null,
          "Empty execution vertex worker actor.");
      executionVertexWorkersMap.put(vertex.getVertexId(), vertex.getWorkerActor());
    });

    return executionVertexWorkersMap;
  }

  public int getJobVertexId() {
    return jobVertexId;
  }

  public String getJobVertexName() {
    return jobVertexName;
  }

  public int getParallelism() {
    return parallelism;
  }

  public List<ExecutionVertex> getExecutionVertices() {
    return executionVertices;
  }

  public void setExecutionVertices(
      List<ExecutionVertex> executionVertex) {
    this.executionVertices = executionVertex;
  }

  public List<ExecutionJobEdge> getOutputEdges() {
    return outputEdges;
  }

  public void setOutputEdges(
      List<ExecutionJobEdge> outputEdges) {
    this.outputEdges = outputEdges;
  }

  public List<ExecutionJobEdge> getInputEdges() {
    return inputEdges;
  }

  public void setInputEdges(
      List<ExecutionJobEdge> inputEdges) {
    this.inputEdges = inputEdges;
  }

  public StreamOperator getStreamOperator() {
    return streamOperator;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  public boolean isSourceVertex() {
    return getVertexType() == VertexType.SOURCE;
  }

  public boolean isTransformationVertex() {
    return getVertexType() == VertexType.TRANSFORMATION;
  }

  public boolean isSinkVertex() {
    return getVertexType() == VertexType.SINK;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobVertexId", jobVertexId)
        .add("jobVertexName", jobVertexName)
        .add("streamOperator", streamOperator)
        .add("vertexType", vertexType)
        .add("parallelism", parallelism)
        .add("executionVertices", executionVertices)
        .add("inputEdges", inputEdges)
        .add("outputEdges", outputEdges)
        .toString();
  }
}
