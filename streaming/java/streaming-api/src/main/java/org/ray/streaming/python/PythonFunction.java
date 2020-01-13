package org.ray.streaming.python;

import org.ray.streaming.api.function.Function;

/**
 * Represents a user defined python function.
 *
 * <p>Python worker can use information in this class to create a function object.</p>
 *
 * <p>If this object is constructed from serialized python function,
 * python worker can deserialize it to create python function directly.
 * If this object is constructed from moduleName and className/functionName,
 * python worker will use `importlib` to load python function.</p>
 *
 * <p>If the python data stream api is invoked from python, `function` will be not null.</p>
 * <p>If the python data stream api is invoked from java, `moduleName` and
 * `className`/`functionName` will be not null.</p>
 * <p>
 * TODO serialize to bytes using protobuf
 */
public class PythonFunction implements Function {
  public enum FunctionInterface {
    SOURCE_FUNCTION("ray.streaming.function.SourceFunction"),
    MAP_FUNCTION("ray.streaming.function.MapFunction"),
    FLAT_MAP_FUNCTION("ray.streaming.function.FlatMapFunction"),
    FILTER_FUNCTION("ray.streaming.function.FilterFunction"),
    KEY_FUNCTION("ray.streaming.function.KeyFunction"),
    REDUCE_FUNCTION("ray.streaming.function.ReduceFunction"),
    SINK_FUNCTION("ray.streaming.function.SinkFunction");

    private String functionInterface;

    FunctionInterface(String functionInterface) {
      this.functionInterface = functionInterface;
    }
  }

  private byte[] function;
  private String moduleName;
  private String className;
  private String functionName;
  /**
   * FunctionInterface can be used to validate python function,
   * and look up operator class from FunctionInterface.
   */
  private String functionInterface;

  private PythonFunction(byte[] function,
                         String moduleName,
                         String className,
                         String functionName) {
    this.function = function;
    this.moduleName = moduleName;
    this.className = className;
    this.functionName = functionName;
  }

  public void setFunctionInterface(FunctionInterface functionInterface) {
    this.functionInterface = functionInterface.functionInterface;
  }

  /**
   * Create a {@link PythonFunction} using python serialized function
   *
   * @param function serialized python function sent from python driver
   */
  public static PythonFunction fromFunction(byte[] function) {
    return new PythonFunction(function, null, null, null);
  }

  /**
   * Create a {@link PythonFunction} using <code>moduleName</code> and
   * <code>className</code>.
   *
   * @param moduleName python module name
   * @param className  python class name
   */
  public static PythonFunction fromClassName(String moduleName, String className) {
    return new PythonFunction(null, moduleName, className, null);
  }

  /**
   * Create a {@link PythonFunction} using <code>moduleName</code> and
   * <code>functionName</code>.
   *
   * @param moduleName   python module name
   * @param functionName python function name
   */
  public static PythonFunction fromFunctionName(String moduleName, String functionName) {
    return new PythonFunction(null, moduleName, null, functionName);
  }
}
