#ifndef RAY_STATS_METRIC_DEFS_H
#define RAY_STATS_METRIC_DEFS_H

/// The definitions of metrics that you can use everywhere.
///
/// There are 4 types of metric:
///   Histogram: Histogram distribution of metric points.
///   Gauge: Keeps the last recorded value, drops everything before.
///   Count: The count of the number of metric points.
///   Sum: A sum up of the metric points.
///
/// You can follow these examples to define your metrics.

static Gauge CurrentWorker("current_worker",
                           "This metric is used for report states of workers."
                           "Through this, we can see the worker's state on dashboard.",
                           "1 pcs", {LanguageKey, WorkerPidKey});

static Gauge CurrentDriver("current_driver",
                           "This metric is used for report states of workers.",
                           "1 pcs", {LanguageKey, DriverPidKey});

static Count TaskCountReceived("task_count_received",
                               "The count that the raylet received.", "pcs",
                               {});

static Histogram RedisLatency("redis_latency", "The latency of a Redis operation.", "us",
                              {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000},
                              {CustomKey});

static Gauge LocalAvailableResource("local_available_resource",
                                    "The available resource of this node.", "pcs",
                                    {ResourceNameKey});

static Gauge LocalTotalResource("local_total_resource",
                                "The total resource of this node.", "pcs",
                                {ResourceNameKey});

static Gauge ActorStats("actor_stats",
                        "Stat the metric values of actor in raylet.", "pcs",
                        {ActorStatsValueTypeKey});

static Gauge ObjectStats("object_stats",
                         "Stat the metric values of object in raylet", "pcs",
                         {ObjectStatsValueTypeKey});

static Gauge LineageCacheStats("lineage_cache_stats",
                               "Stats the metric values of lineage cache.", "pcs",
                               {LineageCacheStatsValueTypeKey});

#endif  // RAY_STATS_METRIC_DEFS_H
