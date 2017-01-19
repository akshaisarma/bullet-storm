package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupDataSummary;
import com.yahoo.bullet.operations.aggregations.grouping.GroupDataSummaryFactory;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Specification;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.tuple.CompactSketch;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.Union;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This {@link Strategy} implements a Tuple Sketch based approach to doing a group by. In particular, it
 * provides a uniform sample of the groups if the number of unique groups exceed the Sketch size. Metrics like
 * sum and count when summed across the uniform sample and divided the sketch theta gives an approximate estimate
 * of the total sum and count across all the groups.
 */
public class GroupBy implements Strategy {
    private CachingGroupData container;
    private int size;
    private UpdatableSketch<CachingGroupData, GroupDataSummary> updateSketch;
    private Union<GroupDataSummary> unionSketch;
    private List<String> groups;
    private Map<String, String> fieldMapping;

    private boolean consumed = false;
    private boolean combined = false;

    private Map<String, String> metadataKeys;
    // Separator for multiple fields when inserting into the Sketch
    private String separator;

    public static final float DEFAULT_SAMPLING_PROBABILITY = 1.0f;

    // Sketch * 8 its size upto 2 * nominal entries everytime it reaches cap
    public static final int DEFAULT_RESIZE_FACTOR = ResizeFactor.X8.lg();

    // This gives us a 13.27% error rate at 99.73% confidence (3 Standard Deviations). But we are only using this to cap
    // the distinct groups
    public static final int DEFAULT_NOMINAL_ENTRIES = 512;

    public static final String META_STD_DEV_1 = "1";
    public static final String META_STD_DEV_2 = "2";
    public static final String META_STD_DEV_3 = "3";
    public static final String META_STD_DEV_UB = "upperBound";
    public static final String META_STD_DEV_LB = "lowerBound";

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public GroupBy(Aggregation aggregation) {
        Map config = aggregation.getConfiguration();
        metadataKeys = (Map<String, String>) config.getOrDefault(BulletConfig.RESULT_METADATA_METRICS_MAPPING,
                                                                 Collections.emptyMap());

        separator = config.getOrDefault(BulletConfig.AGGREGATION_COMPOSITE_FIELD_SEPARATOR,
                                        Aggregation.DEFAULT_FIELD_SEPARATOR).toString();

        Map<GroupOperation, Number> metrics = GroupData.makeInitialMetrics(aggregation.getGroupOperations());
        groups = new ArrayList<>(aggregation.getFields().keySet());
        fieldMapping = aggregation.getFields();

        container = new CachingGroupData(null, metrics);

        float samplingProbability = ((Number) config.getOrDefault(BulletConfig.GROUP_AGGREGATION_SKETCH_SAMPLING,
                                                                  DEFAULT_SAMPLING_PROBABILITY)).floatValue();

        ResizeFactor resizeFactor = getResizeFactor((Number) config.getOrDefault(BulletConfig.GROUP_AGGREGATION_SKETCH_RESIZE_FACTOR,
                                                                                 DEFAULT_RESIZE_FACTOR));

        int nominalEntries = ((Number) config.getOrDefault(BulletConfig.GROUP_AGGREGATION_SKETCH_ENTRIES,
                                                           DEFAULT_NOMINAL_ENTRIES)).intValue();

        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        UpdatableSketchBuilder<CachingGroupData, GroupDataSummary> builder = new UpdatableSketchBuilder(factory);
        updateSketch = builder.setResizeFactor(resizeFactor)
                              .setNominalEntries(nominalEntries)
                              .setSamplingProbability(samplingProbability)
                              .build();

        unionSketch = new Union<>(nominalEntries, factory);
    }

    @Override
    public void consume(BulletRecord data) {
        Map<String, String> groupValues = getGroups(data);
        String key = getFieldsAsString(groups, groupValues, separator);

        // Set the record and the group values into the container. The metrics are already initialized.
        container.setCachedRecord(data);
        container.setGroupFields(groupValues);
        updateSketch.update(key, container);
        consumed = true;
    }

    @Override
    public void combine(byte[] serializedAggregation) {
        Sketch<GroupDataSummary> deserialized = Sketches.heapifySketch(new NativeMemory(serializedAggregation));
        unionSketch.update(deserialized);
        combined = true;
    }

    @Override
    public byte[] getSerializedAggregation() {
        CompactSketch<GroupDataSummary> compactSketch = merge();
        return compactSketch.toByteArray();
    }

    @Override
    public Clip getAggregation() {
        CompactSketch<GroupDataSummary> result = merge();
        Clip clip = new Clip();

        SketchIterator<GroupDataSummary> iterator = result.iterator();
        while (iterator.next()) {
            GroupData data = iterator.getSummary().getData();
            clip.add(data.getAsBulletRecord(fieldMapping));
        }

        String aggregationMetaKey = metadataKeys.getOrDefault(Concept.AGGREGATION_METADATA.getName(), null);
        if (aggregationMetaKey == null) {
            return clip;
        }

        Map<String, Object> sketchMetadata = getSketchMetadata(result, metadataKeys);
        Metadata meta = new Metadata().add(aggregationMetaKey, sketchMetadata);
        return clip.add(meta);
    }

    private CompactSketch<GroupDataSummary> merge() {
        // Merge the updateSketch into the unionSketch. Supporting it for completeness.
        if (consumed && combined) {
            unionSketch.update(updateSketch.compact());
        }

        if (combined) {
            return unionSketch.getResult();
        } else {
            return updateSketch.compact();
        }
    }

    private static String getFieldsAsString(List<String> fields, Map<String, String> mapping, String separator) {
        return fields.stream().map(mapping::get).collect(Collectors.joining(separator));
    }

    private Map<String, String> getGroups(BulletRecord record) {
        Map<String, String> groupMapping = new HashMap<>();
        for (String key : groups) {
            // This explicitly does not do a TypedObject checking. Nulls turn into Strings
            String value = Objects.toString(Specification.extractField(key, record));
            groupMapping.put(key, value);
        }
        return groupMapping;
    }

    private Map<String, Object> getSketchMetadata(Sketch sketch, Map<String, String> conceptKeys) {
        Map<String, Object> metadata = new HashMap<>();

        String standardDeviationsKey = conceptKeys.get(Concept.STANDARD_DEVIATIONS.getName());
        String isEstimatedKey = conceptKeys.get(Concept.ESTIMATED_RESULT.getName());
        String thetaKey = conceptKeys.get(Concept.SKETCH_THETA.getName());
        String uniquesEstimate = conceptKeys.get(Concept.UNIQUES_ESTIMATE.getName());

        addIfKeyNonNull(metadata, standardDeviationsKey, () -> getStandardDeviations(sketch));
        addIfKeyNonNull(metadata, isEstimatedKey, sketch::isEstimationMode);
        addIfKeyNonNull(metadata, thetaKey, sketch::getTheta);
        addIfKeyNonNull(metadata, uniquesEstimate, sketch::getEstimate);

        return metadata;
    }

    private Map<String, Map<String, Double>> getStandardDeviations(Sketch sketch) {
        Map<String, Map<String, Double>> standardDeviations = new HashMap<>();
        standardDeviations.put(META_STD_DEV_1, getStandardDeviation(sketch, 1));
        standardDeviations.put(META_STD_DEV_2, getStandardDeviation(sketch, 2));
        standardDeviations.put(META_STD_DEV_3, getStandardDeviation(sketch, 3));
        return standardDeviations;
    }

    private Map<String, Double> getStandardDeviation(Sketch sketch, int standardDeviation) {
        double lowerBound = sketch.getLowerBound(standardDeviation);
        double upperBound = sketch.getUpperBound(standardDeviation);
        Map<String, Double> bounds = new HashMap<>();
        bounds.put(META_STD_DEV_LB, lowerBound);
        bounds.put(META_STD_DEV_UB, upperBound);
        return bounds;
    }

    private static void addIfKeyNonNull(Map<String, Object> metadata, String key, Supplier<Object> supplier) {
        if (key != null) {
            metadata.put(key, supplier.get());
        }
    }

    /**
     * Converts a integer representing the resizing for Sketches into a {@link ResizeFactor}.
     *
     * @param factor An int representing the scaling when the Sketch reaches its threshold. Supports 1, 2, 4 and 8.
     * @return A {@link ResizeFactor} represented by the integer or {@link ResizeFactor#X8} otherwise.
     */
    static ResizeFactor getResizeFactor(Number factor) {
        int resizeFactor = factor.intValue();
        switch (resizeFactor) {
            case 1:
                return ResizeFactor.X1;
            case 2:
                return ResizeFactor.X2;
            case 4:
                return ResizeFactor.X4;
            default:
                return ResizeFactor.X8;
        }
    }
}
