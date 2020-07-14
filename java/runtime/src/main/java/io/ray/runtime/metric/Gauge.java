package io.ray.runtime.metric;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Gauge metric for recording last value and mapping object from stats.
 */
public class Gauge extends Metric {

  public Gauge(String name, String description, String unit, Map<TagKey, String> tags) {
    super(name, tags);
    metricNativePointer = registerGaugeNative(name, description, unit,
      tags.keySet().stream().map(TagKey::getTagKey).collect(Collectors.toList()));
    Preconditions.checkState(metricNativePointer != 0, "Gauge native pointer must not be 0.");
  }

  private native long registerGaugeNative(String name, String description,
                                           String unit, List<String> tagKeys);

  @Override
  public void reset() {

  }
}

