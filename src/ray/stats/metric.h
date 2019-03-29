#ifndef RAY_STATS_METRIC_H
#define RAY_STATS_METRIC_H

#include <memory>
#include <unordered_map>

#include "opencensus/exporters/stats/prometheus/prometheus_exporter.h"
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"
#include "prometheus/exposer.h"

#include "ray/util/logging.h"

namespace ray {

namespace stats {

/// Include tag_defs.h to define tag items
#include "tag_defs.h"

/// The helper function for global tags.
std::unique_ptr<TagsType> &GetGlobalTagsPtr();

/// The helper function for registering a view.
static void RegisterAsView(opencensus::stats::ViewDescriptor view_descriptor,
                           const std::vector<opencensus::tags::TagKey> &keys);

/// A thin wrapper that wraps the `opencensus::tag::measure` for using it simply.
class Metric {
 public:
  Metric(const std::string &name, const std::string &description, const std::string &unit,
         const std::vector<opencensus::tags::TagKey> &tag_keys = {})
      : measure_(nullptr),
        name_(name),
        description_(description),
        unit_(unit),
        tag_keys_(tag_keys){};

  virtual ~Metric() = default;

  Metric &operator()() { return *this; }

  /// Get the name of this metric.
  std::string GetName() const { return name_; }

  /// Record the value for this metric.
  void Record(double value) { Record(value, {}); }

  /// Record the value for this metric.
  ///
  /// \param value The value that we record.
  /// \param tags The tag values that we want to record for this metric record.
  void Record(double value, const TagsType &tags);

 protected:
  virtual void RegisterView() = 0;

 protected:
  std::string name_;
  std::string description_;
  std::string unit_;
  std::vector<opencensus::tags::TagKey> tag_keys_;
  std::unique_ptr<opencensus::stats::Measure<double>> measure_;

};  // class Metric

class Gauge : public Metric {
 public:
  Gauge(const std::string &name, const std::string &description, const std::string &unit,
        const std::vector<opencensus::tags::TagKey> &tag_keys = {})
      : Metric(name, description, unit, tag_keys) {}

 private:
  void RegisterView() override;

};  // class Gauge

class Histogram : public Metric {
 public:
  Histogram(const std::string &name, const std::string &description,
            const std::string &unit, const std::vector<double> boundaries,
            const std::vector<opencensus::tags::TagKey> &tag_keys = {})
      : Metric(name, description, unit, tag_keys), boundaries_(boundaries) {}

 private:
  void RegisterView() override;

 private:
  std::vector<double> boundaries_;

};  // class Histogram

class Count : public Metric {
 public:
  Count(const std::string &name, const std::string &description, const std::string &unit,
        const std::vector<opencensus::tags::TagKey> &tag_keys = {})
      : Metric(name, description, unit, tag_keys) {}

 private:
  void RegisterView() override;

};  // class Count

class Sum : public Metric {
 public:
  Sum(const std::string &name, const std::string &description, const std::string &unit,
      const std::vector<opencensus::tags::TagKey> &tag_keys = {})
      : Metric(name, description, unit, tag_keys) {}

 private:
  void RegisterView() override;

};  // class Sum

}  // namespace stats

}  // namespace ray

#endif  // RAY_STATS_METRIC_H
