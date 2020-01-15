package org.ray.streaming.runtime.python;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import org.ray.api.RayActor;
import org.ray.api.RayPyActor;
import org.ray.streaming.api.function.Function;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.python.PythonFunction;
import org.ray.streaming.python.PythonPartition;
import org.ray.streaming.runtime.core.graph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.ExecutionNode;
import org.ray.streaming.runtime.core.graph.ExecutionTask;
import org.ray.streaming.runtime.generated.RemoteCall;
import org.ray.streaming.runtime.worker.JobWorker;

public class GraphPbBuilder {

  /**
   * For simple scenario, a single ExecutionNode is enough. Buf some cases may need
   * sub-graph information, so we serialize entire graph.
   */
  public static RemoteCall.ExecutionGraph buildExecutionGraphPb(ExecutionGraph graph) {
    RemoteCall.ExecutionGraph.Builder builder = RemoteCall.ExecutionGraph.newBuilder();
    builder.setBuildTime(graph.getBuildTime());
    for (ExecutionNode node : graph.getExecutionNodeList()) {
      RemoteCall.ExecutionGraph.ExecutionNode.Builder nodeBuilder =
          RemoteCall.ExecutionGraph.ExecutionNode.newBuilder();
      nodeBuilder.setNodeId(node.getNodeId());
      nodeBuilder.setParallelism(node.getParallelism());
      nodeBuilder.setNodeType(
          RemoteCall.ExecutionGraph.NodeType.valueOf(node.getNodeType().name()));
      nodeBuilder.setLanguage(RemoteCall.Language.valueOf(node.getLanguage().name()));
      byte[] functionBytes = serializeFunction(node.getStreamOperator().getFunction());
      nodeBuilder.setFunction(ByteString.copyFrom(functionBytes));

      // build tasks
      for (ExecutionTask task : node.getExecutionTasks()) {
        RemoteCall.ExecutionGraph.ExecutionTask.Builder taskBuilder =
            RemoteCall.ExecutionGraph.ExecutionTask.newBuilder();
        taskBuilder
            .setTaskId(task.getTaskId())
            .setTaskIndex(task.getTaskIndex())
            .setWorkerActor(ByteString.copyFrom(serializeWorkerActor(task.getWorker())));
        nodeBuilder.addExecutionTasks(taskBuilder.build());
      }

      // build edges
      for (ExecutionEdge edge : node.getInputsEdges()) {
        nodeBuilder.addInputsEdges(buildEdge(edge));
      }
      for (ExecutionEdge edge : node.getInputsEdges()) {
        nodeBuilder.addOutputEdges(buildEdge(edge));
      }

      builder.addExecutionNodes(nodeBuilder.build());
    }

    return builder.build();
  }

  private static RemoteCall.ExecutionGraph.ExecutionEdge buildEdge(ExecutionEdge edge) {
    RemoteCall.ExecutionGraph.ExecutionEdge.Builder edgeBuilder =
        RemoteCall.ExecutionGraph.ExecutionEdge.newBuilder();
    edgeBuilder.setSrcNodeId(edge.getSrcNodeId());
    edgeBuilder.setTargetNodeId(edge.getTargetNodeId());
    edgeBuilder.setPartition(ByteString.copyFrom(serializePartition(edge.getPartition())));
    return edgeBuilder.build();
  }

  private static byte[] serializeFunction(Function function) {
    if (function instanceof PythonFunction) {
      PythonFunction pyFunc = (PythonFunction) function;
      // function_bytes, module_name, class_name, function_name, function_interface
      return new MsgPackSerializer().serialize(Arrays.asList(
          pyFunc.getFunction(), pyFunc.getModuleName(),
          pyFunc.getClassName(), pyFunc.getFunctionName(),
          pyFunc.getFunctionInterface()
      ));
    } else {
      return new byte[0];
    }
  }

  private static byte[] serializePartition(Partition partition) {
    if (partition instanceof PythonPartition) {
      PythonPartition pythonPartition = (PythonPartition) partition;
      // partition_bytes, module_name, class_name, function_name
      return new MsgPackSerializer().serialize(Arrays.asList(
          pythonPartition.getPartition(), pythonPartition.getModuleName(),
          pythonPartition.getClassName(), pythonPartition.getFunctionName()
      ));
    } else {
      return new byte[0];
    }
  }

  private static byte[] serializeWorkerActor(RayActor actor) {
    if (actor instanceof RayPyActor) {
      RayPyActor pyActor = (RayPyActor) actor;
      return new MsgPackSerializer().serialize(Arrays.asList(
          pyActor.getModuleName(), pyActor.getClassName(), pyActor.getId().getBytes()
      ));
    } else {
      return new MsgPackSerializer().serialize(Arrays.asList(
          JobWorker.class.getName(), actor.getId().getBytes()
      ));
    }
  }

}
