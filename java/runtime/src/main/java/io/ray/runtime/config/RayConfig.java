package io.ray.runtime.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import io.ray.api.id.JobId;
import io.ray.runtime.generated.Common.WorkerType;
import io.ray.runtime.util.NetworkUtil;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Configurations of Ray runtime.
 * See `ray.default.conf` for the meaning of each field.
 */
public class RayConfig {

  public static final String DEFAULT_CONFIG_FILE = "ray.default.conf";
  public static final String CUSTOM_CONFIG_FILE = "ray.conf";

  private Config config;

  /**
   * IP of this node. if not provided, IP will be automatically detected.
   */
  public final String nodeIp;
  public final WorkerType workerMode;
  public final RunMode runMode;
  private JobId jobId;
  public String sessionDir;
  public String logDir;

  private String redisAddress;
  public final String redisPassword;

  // RPC socket name of object store.
  public String objectStoreSocketName;

  // RPC socket name of Raylet.
  public String rayletSocketName;
  // Listening port for node manager.
  public int nodeManagerPort;
  public final Map<String, String> rayletConfigParameters;

  public List<String> codeSearchPath;

  public final List<String> headArgs;

  public final int numWorkersPerProcess;

  public final List<String> jvmOptionsForJavaWorker;
  public final Map<String, String> workerEnv;

  private void validate() {
    if (workerMode == WorkerType.WORKER) {
      Preconditions.checkArgument(redisAddress != null,
          "Redis address must be set in worker mode.");
    }
  }

  private String removeTrailingSlash(String path) {
    if (path.endsWith("/")) {
      return path.substring(0, path.length() - 1);
    } else {
      return path;
    }
  }

  public RayConfig(Config config) {
    this.config = config;
    // Worker mode.
    WorkerType localWorkerMode;
    try {
      localWorkerMode = config.getEnum(WorkerType.class, "ray.worker.mode");
    } catch (ConfigException.Missing e) {
      localWorkerMode = WorkerType.DRIVER;
    }
    workerMode = localWorkerMode;
    boolean isDriver = workerMode == WorkerType.DRIVER;
    // Run mode.
    if (config.hasPath("ray.local-mode")) {
      runMode = config.getBoolean("ray.local-mode") ? RunMode.SINGLE_PROCESS : RunMode.CLUSTER;
    } else {
      runMode = config.getEnum(RunMode.class, "ray.run-mode");
    }
    // Node ip.
    if (config.hasPath("ray.node-ip")) {
      nodeIp = config.getString("ray.node-ip");
    } else {
      nodeIp = NetworkUtil.getIpAddress(null);
    }
    // Job id.
    String jobId = config.getString("ray.job.id");
    if (!jobId.isEmpty()) {
      this.jobId = JobId.fromHexString(jobId);
    } else {
      this.jobId = JobId.NIL;
    }

    // jvm options for java workers of this job.
    jvmOptionsForJavaWorker = config.getStringList("ray.job.jvm-options");

    ImmutableMap.Builder<String, String> workerEnvBuilder = ImmutableMap.builder();
    Config workerEnvConfig = config.getConfig("ray.job.worker-env");
    if (workerEnvConfig != null) {
      for (Map.Entry<String, ConfigValue> entry : workerEnvConfig.entrySet()) {
        workerEnvBuilder.put(entry.getKey(), workerEnvConfig.getString(entry.getKey()));
      }
    }
    workerEnv = workerEnvBuilder.build();
    updateSessionDir(null);

    // Object store socket name.
    if (config.hasPath("ray.object-store.socket-name")) {
      objectStoreSocketName = config.getString("ray.object-store.socket-name");
    }

    // Raylet socket name.
    if (config.hasPath("ray.raylet.socket-name")) {
      rayletSocketName = config.getString("ray.raylet.socket-name");
    }

    // Redis configurations.
    String redisAddress = config.getString("ray.redis.address");
    if (StringUtils.isNotBlank(redisAddress)) {
      setRedisAddress(redisAddress);
    } else {
      // We need to start gcs using `RunManager` for local cluster
      this.redisAddress = null;
    }

    redisPassword = config.getString("ray.redis.password");
    // Raylet node manager port.
    if (config.hasPath("ray.raylet.node-manager-port")) {
      nodeManagerPort = config.getInt("ray.raylet.node-manager-port");
    } else {
      Preconditions.checkState(workerMode != WorkerType.WORKER,
          "Worker started by raylet should accept the node manager port from raylet.");
      nodeManagerPort = NetworkUtil.getUnusedPort();
    }

    // Raylet parameters.
    rayletConfigParameters = new HashMap<>();
    Config rayletConfig = config.getConfig("ray.raylet.config");
    for (Map.Entry<String, ConfigValue> entry : rayletConfig.entrySet()) {
      Object value = entry.getValue().unwrapped();
      rayletConfigParameters.put(entry.getKey(), value == null ? "" : value.toString());
    }

    // Job code search path.
    if (config.hasPath("ray.job.code-search-path")) {
      codeSearchPath = Arrays.asList(
        config.getString("ray.job.code-search-path").split(":"));
    } else {
      codeSearchPath = Collections.emptyList();
    }

    boolean enableMultiTenancy = false;
    if (config.hasPath("ray.raylet.config.enable_multi_tenancy")) {
      enableMultiTenancy =
        Boolean.valueOf(config.getString("ray.raylet.config.enable_multi_tenancy"));
    }

    if (!enableMultiTenancy) {
      if (!isDriver) {
        numWorkersPerProcess = config.getInt("ray.raylet.config.num_workers_per_process_java");
      } else {
        numWorkersPerProcess = 1; // Actually this value isn't used in RayNativeRuntime.
      }
    } else {
      numWorkersPerProcess = config.getInt("ray.job.num-java-workers-per-process");
    }

    headArgs = config.getStringList("ray.head.args");

    // Validate config.
    validate();
  }

  public void setRedisAddress(String redisAddress) {
    Preconditions.checkNotNull(redisAddress);
    Preconditions.checkState(this.redisAddress == null, "Redis address was already set");

    this.redisAddress = redisAddress;
  }

  public String getRedisAddress() {
    return redisAddress;
  }

  public void setJobId(JobId jobId) {
    this.jobId = jobId;
  }

  public JobId getJobId() {
    return this.jobId;
  }

  public int getNodeManagerPort() {
    return nodeManagerPort;
  }

  public void setSessionDir(String sessionDir) {
    updateSessionDir(sessionDir);
  }

  public Config getInternalConfig() {
    return config;
  }

  /**
   * Renders the config value as a HOCON string.
   */
  public String render() {
    // These items might be dynamically generated or mutated at runtime.
    // Explicitly include them.
    Map<String, Object> dynamic = new HashMap<>();
    dynamic.put("ray.session-dir", sessionDir);
    dynamic.put("ray.raylet.socket-name", rayletSocketName);
    dynamic.put("ray.object-store.socket-name", objectStoreSocketName);
    dynamic.put("ray.raylet.node-manager-port", nodeManagerPort);
    dynamic.put("ray.redis.address", redisAddress);
    dynamic.put("ray.job.code-search-path", codeSearchPath);
    Config toRender = ConfigFactory.parseMap(dynamic).withFallback(config);
    return toRender.root().render(ConfigRenderOptions.concise());
  }

  private void updateSessionDir(String sessionDir) {
    // session dir
    if (config.hasPath("ray.session-dir")) {
      sessionDir = config.getString("ray.session-dir");
    }
    if (sessionDir != null) {
      sessionDir = removeTrailingSlash(sessionDir);
    }
    this.sessionDir = sessionDir;

    // Log dir.
    String localLogDir = null;
    if (config.hasPath("ray.logging.dir")) {
      localLogDir = removeTrailingSlash(config.getString("ray.logging.dir"));
    }
    if (Strings.isNullOrEmpty(localLogDir)) {
      logDir = String.format("%s/logs", sessionDir);
    } else {
      logDir = localLogDir;
    }
  }

  @Override
  public String toString() {
    return render();
  }

  /**
   * Create a RayConfig by reading configuration in the following order:
   * 1. System properties.
   * 2. `ray.conf` file.
   * 3. `ray.default.conf` file.
   */
  public static RayConfig create() {
    ConfigFactory.invalidateCaches();
    Config config = ConfigFactory.systemProperties();
    String configPath = System.getProperty("ray.config-file");
    if (Strings.isNullOrEmpty(configPath)) {
      config = config.withFallback(ConfigFactory.load(CUSTOM_CONFIG_FILE));
    } else {
      config = config.withFallback(ConfigFactory.parseFile(new File(configPath)));
    }
    config = config.withFallback(ConfigFactory.load(DEFAULT_CONFIG_FILE));
    return new RayConfig(config.withOnlyPath("ray"));
  }

}
