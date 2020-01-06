package org.ray.streaming.runtime.config;

import java.util.HashMap;
import java.util.Map;
import org.aeonbits.owner.ConfigFactory;
import org.nustaq.serialization.FSTConfiguration;
import org.ray.streaming.runtime.BaseUnitTest;
import org.ray.streaming.runtime.config.global.CommonConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConfigTest extends BaseUnitTest {

  @Test
  public void testBaseFunc() {
    // conf using
    CommonConfig commonConfig = ConfigFactory.create(CommonConfig.class);
    Assert.assertTrue(commonConfig.fileEncoding().equals("UTF-8"));

    // override conf
    Map<String, String> customConf = new HashMap<>();
    customConf.put(CommonConfig.FILE_ENCODING, "GBK");
    CommonConfig commonConfig2 = ConfigFactory.create(CommonConfig.class, customConf);
    Assert.assertTrue(commonConfig2.fileEncoding().equals("GBK"));
  }

  @Test
  public void testMapTransformation() {
    Map<String, String> conf = new HashMap<>();
    String encodingType = "GBK";
    conf.put(CommonConfig.FILE_ENCODING, encodingType);

    StreamingConfig config = new StreamingConfig(conf);
    Map<String, String> wholeConfigMap = config.getMap();

    Assert.assertTrue(wholeConfigMap.get(CommonConfig.FILE_ENCODING).equals(encodingType));
  }

  @Test
  public void testCustomConfKeeping() {
    Map<String, String> conf = new HashMap<>();
    String customKey = "test_key";
    String customValue = "test_value";
    conf.put(customKey, customValue);
    StreamingConfig config = new StreamingConfig(conf);
    Assert.assertEquals(config.getMap().get(customKey), customValue);
  }

  @Test
  public void testSerialization() {
    Map<String, String> conf = new HashMap<>();
    String customKey = "test_key";
    String customValue = "test_value";
    conf.put(customKey, customValue);
    StreamingConfig config = new StreamingConfig(conf);

    FSTConfiguration fstConf = FSTConfiguration.createDefaultConfiguration();
    byte[] configBytes = fstConf.asByteArray(config);
    StreamingConfig deserializedConfig = (StreamingConfig) fstConf.asObject(configBytes);

    Assert.assertEquals(deserializedConfig.masterConfig.commonConfig.fileEncoding(), "UTF-8");
    Assert.assertEquals(deserializedConfig.getMap().get(customKey), customValue);
  }
}
