package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupDataSummary;
import com.yahoo.bullet.operations.aggregations.grouping.GroupDataSummaryFactory;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.operations.aggregations.sketches.TupleKMVSketch;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This {@link Strategy} implements a Tuple Sketch based approach to doing a group by. In particular, it
 * provides a uniform sample of the groups if the number of unique groups exceed the Sketch size. Metrics like
 * sum and count when summed across the uniform sample and divided the sketch theta gives an approximate estimate
 * of the total sum and count across all the groups.
 */
public class GroupBy extends KMVStrategy {
    private int size;
    private Map<String, String> fieldMapping;

    // This is reused for the duration of the strategy.
    private CachingGroupData container;

    private UpdatableSketch<CachingGroupData, GroupDataSummary> updateSketch;
    private Union<GroupDataSummary> unionSketch;

    // This gives us a 13.27% error rate at 99.73% confidence (3 Standard Deviations). But we are only using this to cap
    // the distinct groups
    public static final int DEFAULT_NOMINAL_ENTRIES = 512;

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public GroupBy(Aggregation aggregation) {
        super(aggregation);

        Map config = aggregation.getConfiguration();

        Map<GroupOperation, Number> metrics = GroupData.makeInitialMetrics(aggregation.getGroupOperations());

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
        Map<String, String> fieldMapping = getGroups(data);
        String key = getFieldsAsString(fields, fieldMapping, separator);

        // Set the record and the group values into the container. The metrics are already initialized.
        container.setCachedRecord(data);
        container.setGroupFields(fieldMapping);
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
        Sketch<GroupDataSummary> compactSketch = merge();
        return compactSketch.toByteArray();
    }

    @Override
    public Clip getAggregation() {
        Sketch<GroupDataSummary> result = merge();
        Clip clip = new Clip();

        SketchIterator<GroupDataSummary> iterator = result.iterator();
        while (iterator.next()) {
            GroupData data = iterator.getSummary().getData();
            clip.add(data.getAsBulletRecord(fieldMapping));
        }

        String aggregationMetaKey = getAggregationMetaKey();
        if (aggregationMetaKey == null) {
            return clip;
        }

        return clip.add(new Metadata().add(aggregationMetaKey, getSketchMetadata(result, metadataKeys)));
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
        for (String key : fields) {
            // This explicitly does not do a TypedObject checking. Nulls turn into Strings
            String value = Objects.toString(Specification.extractField(key, record));
            groupMapping.put(key, value);
        }
        return groupMapping;
    }

    private Map<String, Object> getSketchMetadata(Sketch<GroupDataSummary> sketch, Map<String, String> conceptKeys) {
        TupleKMVSketch wrapped = new TupleKMVSketch(sketch);
        Map<String, Object> metadata = super.getSketchMetadata(wrapped, conceptKeys);

        String uniquesEstimate = conceptKeys.get(Concept.UNIQUES_ESTIMATE.getName());
        addIfKeyNonNull(metadata, uniquesEstimate, sketch::getEstimate);

        return metadata;
    }
}
