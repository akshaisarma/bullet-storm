package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;

import java.util.Map;

/**
 * This {@link Strategy} uses {@link QuantileSketch} to find distributions of a numeric field. Based on the size
 * configured for the sketch, the normalized rank error can be determined and tightly bound.
 */
public class Distribution extends SketchingStrategy<QuantileSketch> {
    public static final int DEFAULT_ENTRIES = 1024;

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public Distribution(Aggregation aggregation) {
        super(aggregation);
        int entries = ((Number) config.getOrDefault(BulletConfig.DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES,
                                                    DEFAULT_ENTRIES)).intValue();

        Map<String, Object> attributes = aggregation.getAttributes();

        Map<String, String> fieldsToNames = aggregation.getFields();
        sketch = new QuantileSketch(entries);
    }

    @Override
    public void consume(BulletRecord data) {
    }
}
