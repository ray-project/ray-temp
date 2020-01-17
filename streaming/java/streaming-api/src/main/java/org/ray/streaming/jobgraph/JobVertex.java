package org.ray.streaming.jobgraph;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import org.ray.streaming.operator.StreamOperator;

/**
 * Job vertex is a cell node where logic is executed.
 */
public class JobVertex implements Serializable {

  private int vertexId;
  private int parallelism;
  private VertexType vertexType;
  private LanguageType languageType;
  private StreamOperator streamOperator;

  public JobVertex(int vertexId, int parallelism, VertexType vertexType,
      StreamOperator streamOperator) {
    this(vertexId, parallelism, vertexType, LanguageType.JAVA, streamOperator);
  }

  public JobVertex(int vertexId, int parallelism, VertexType vertexType, LanguageType languageType,
      StreamOperator streamOperator) {
    this.vertexId = vertexId;
    this.parallelism = parallelism;
    this.vertexType = vertexType;
    this.languageType = languageType;
    this.streamOperator = streamOperator;
  }

  public int getVertexId() {
    return vertexId;
  }

  public int getParallelism() {
    return parallelism;
  }

  public StreamOperator getStreamOperator() {
    return streamOperator;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  public LanguageType getLanguageType() {
    return languageType;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("vertexId", vertexId)
        .add("parallelism", parallelism)
        .add("vertexType", vertexType)
        .add("languageType", languageType)
        .add("streamOperator", streamOperator)
        .toString();
  }

}
