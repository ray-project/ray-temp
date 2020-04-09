package org.ray.streaming.runtime.graph;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.DataStream;
import org.ray.streaming.api.stream.DataStreamSource;
import org.ray.streaming.api.stream.StreamSink;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.jobgraph.JobGraphBuilder;
import org.ray.streaming.runtime.BaseUnitTest;
import org.ray.streaming.runtime.config.StreamingConfig;
import org.ray.streaming.runtime.config.master.ResourceConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.resource.ResourceType;
import org.ray.streaming.runtime.master.JobRuntimeContext;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExecutionGraphTest extends BaseUnitTest {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraphTest.class);

  @Test
  public void testBuildExecutionGraph() {
    Map<String, String> jobConf = new HashMap<>();
    StreamingConfig streamingConfig = new StreamingConfig(jobConf);
    GraphManager graphManager = new GraphManagerImpl(new JobRuntimeContext(streamingConfig));
    JobGraph jobGraph = buildJobGraph();
    ExecutionGraph executionGraph = buildExecutionGraph(graphManager, jobGraph);
    List<ExecutionJobVertex> executionJobVertices = executionGraph.getExecutionJobVertices();

    Assert.assertEquals(executionJobVertices.size(), jobGraph.getJobVertexList().size());

    int totalVertexNum = jobGraph.getJobVertexList().stream()
        .mapToInt(vertex -> vertex.getParallelism()).sum();
    Assert.assertEquals(executionGraph.getAllExecutionVertices().size(), totalVertexNum);
    Assert.assertEquals(executionGraph.getAllExecutionVertices().size(),
        executionGraph.getExecutionVertexIdGenerator().get());

    executionGraph.getAllExecutionVertices().forEach(vertex -> {
        Assert.assertNotNull(vertex.getStreamOperator());
        Assert.assertNotNull(vertex.getJobVertexName());
        Assert.assertNotNull(vertex.getVertexType());
        Assert.assertNotNull(vertex.getLanguage());
        Assert.assertEquals(vertex.getVertexName(),
          vertex.getJobVertexId() + "-" + vertex.getJobVertexName() + "-" + vertex.getVertexIndex());
    });

    int startIndex = 0;
    ExecutionJobVertex upStream = executionJobVertices.get(startIndex);
    ExecutionJobVertex downStream = executionJobVertices.get(startIndex + 1);
    Assert.assertEquals(upStream.getOutputEdges().get(0).getTargetVertex(), downStream);

    List<ExecutionVertex> upStreamVertices = upStream.getExecutionVertices();
    List<ExecutionVertex> downStreamVertices = downStream.getExecutionVertices();
    upStreamVertices.forEach(vertex -> {
        Assert.assertEquals(vertex.getResources().get(ResourceType.CPU.name()), 2.0);
        vertex.getOutputEdges().stream().forEach(upStreamOutPutEdge -> {
            Assert.assertTrue(downStreamVertices.contains(upStreamOutPutEdge.getTargetVertex()));
        });
    });
  }

  public static ExecutionGraph buildExecutionGraph(GraphManager graphManager) {
    return graphManager.buildExecutionGraph(buildJobGraph());
  }

  public static ExecutionGraph buildExecutionGraph(GraphManager graphManager, JobGraph jobGraph) {
    return graphManager.buildExecutionGraph(jobGraph);
  }

  public static JobGraph buildJobGraph() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    DataStream<String> dataStream = DataStreamSource.buildSource(streamingContext,
        Lists.newArrayList("a", "b", "c"));
    StreamSink streamSink = dataStream.sink(x -> LOG.info(x));

    Map<String, String> jobConfig = new HashMap<>();
    jobConfig.put("key1", "value1");
    jobConfig.put("key2", "value2");
    jobConfig.put(ResourceConfig.TASK_RESOURCE_CPU, "2.0");

    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(
        Lists.newArrayList(streamSink), "test", jobConfig);

    return jobGraphBuilder.build();
  }

}
