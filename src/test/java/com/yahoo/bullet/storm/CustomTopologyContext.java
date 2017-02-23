package com.yahoo.bullet.storm;

import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.TopologyContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class CustomTopologyContext extends TopologyContext {
    private Map<Integer, Map<String, IMetric>> registeredMetrics;

    public CustomTopologyContext() {
        super(null, null, null, null, null, null, null, null, null, null, null, null, null, null, new HashMap<>(), null);
    }

    @Override
    public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs) {
        if (!registeredMetrics.containsKey(timeBucketSizeInSecs)) {
            registeredMetrics.put(timeBucketSizeInSecs, new HashMap<>());
        }
        Map<String, IMetric> metrics = registeredMetrics.get(timeBucketSizeInSecs);
        metrics.put(name, metric);

        return metric;
    }

    @Override
    public IMetric getRegisteredMetricByName(String name) {
        Optional<Map.Entry<String, IMetric>> metric = registeredMetrics.values().stream()
                                                                       .flatMap(m -> m.entrySet().stream())
                                                                       .filter(e -> e.getKey().equals(name))
                                                                       .findFirst();
        return metric.isPresent() ? metric.get().getValue() : null;
    }
}
