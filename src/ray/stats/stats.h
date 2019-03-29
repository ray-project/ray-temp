#ifndef RAY_STATS_STATS_H
#define RAY_STATS_STATS_H

#include <exception>
#include <string>

#include "opencensus/exporters/stats/prometheus/prometheus_exporter.h"
#include "opencensus/exporters/stats/stdout/stdout_exporter.h"
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"
#include "prometheus/exposer.h"

#include "ray/stats/metric.h"
#include "ray/util/logging.h"

namespace ray {

namespace stats {

/// Include metric_defs.h to define measure items.
#include "metric_defs.h"

/// Initialize stats.
static void Init(const std::string &address) {
  // Enable the Prometheus exporter.
  // Note that the reason for we using local static variables
  // here is to make sure they are single-instances.
  static auto exporter =
      std::make_shared<opencensus::exporters::stats::PrometheusExporter>();

  // Enable stdout exporter by default.
  opencensus::exporters::stats::StdoutExporter::Register();

  // Enable prometheus exporter.
  try {
    static prometheus::Exposer exposer(address);
    exposer.RegisterCollectable(exporter);
    RAY_LOG(INFO) << "Succeeded to initialize stats: exporter address is " << address;
  } catch (std::exception &e) {
    RAY_LOG(WARNING) << "Failed to create the Prometheus exposer It doesn't "
                     << "affect anything except stats. Caused by: " << e.what();
  }

}

}  // namespace stats

}  // namespace ray

#endif  // RAY_STATS_STATS_H
