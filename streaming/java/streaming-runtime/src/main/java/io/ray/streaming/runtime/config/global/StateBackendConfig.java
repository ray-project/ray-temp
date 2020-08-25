package io.ray.streaming.runtime.config.global;

import org.aeonbits.owner.Config;

public interface StateBackendConfig extends Config {

  String STATE_BACKEND_TYPE = "streaming.state-backend.type";
  String FILE_STATE_ROOT_PATH = "streaming.state-backend.file-state.root";

  @Config.DefaultValue(value = "memory")
  @Key(value = STATE_BACKEND_TYPE)
  String stateBackendType();

  @Config.DefaultValue(value = "/tmp/ray_streaming_state")
  @Key(value = FILE_STATE_ROOT_PATH)
  String fileStateRootPath();
}
