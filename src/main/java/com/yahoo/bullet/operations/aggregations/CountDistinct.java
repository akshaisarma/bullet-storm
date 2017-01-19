package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.aggregations.sketches.ThetaKMVSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Specification;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CountDistinct extends KMVStrategy {
    private UpdateSketch updateSketch;
    private Union unionSketch;
    private String newName;

    public static final String NEW_NAME_KEY = "newName";
    public static final String DEFAULT_NEW_NAME = "COUNT DISTINCT";


    // Sketch defaults
    // Recommended for real-time systems
    public static final String DEFAULT_UPDATE_SKETCH_FAMILY = Family.ALPHA.getFamilyName();

    // This gives us (Alpha sketches fall back to QuickSelect RSEs after compaction or set operations) a 2.34% error
    // rate at 99.73% confidence (3 Standard Deviations).
    public static final int DEFAULT_NOMINAL_ENTRIES = 16384;

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public CountDistinct(Aggregation aggregation) {
        super(aggregation);
        Map<String, Object> attributes = aggregation.getAttributes();

        newName = attributes == null ? DEFAULT_NEW_NAME : attributes.getOrDefault(NEW_NAME_KEY, DEFAULT_NEW_NAME).toString();

        ResizeFactor resizeFactor = getResizeFactor(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR);
        float samplingProbability = ((Number) config.getOrDefault(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING,
                                                                  DEFAULT_SAMPLING_PROBABILITY)).floatValue();
        Family family = getFamily(config.getOrDefault(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY,
                                                      DEFAULT_UPDATE_SKETCH_FAMILY).toString());
        int nominalEntries = ((Number) config.getOrDefault(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES,
                                                           DEFAULT_NOMINAL_ENTRIES)).intValue();

        updateSketch = UpdateSketch.builder().setFamily(family).setNominalEntries(nominalEntries)
                                             .setP(samplingProbability).setResizeFactor(resizeFactor)
                                             .build();

        unionSketch = SetOperation.builder().setNominalEntries(nominalEntries).setP(samplingProbability)
                                            .setResizeFactor(resizeFactor).buildUnion();
    }

    @Override
    public void consume(BulletRecord data) {
        String field = getFieldsAsString(fields, data, separator);
        updateSketch.update(field);
        consumed = true;
    }

    @Override
    public byte[] getSerializedAggregation() {
        CompactSketch compactSketch = merge();
        return compactSketch.toByteArray();
    }

    @Override
    public void combine(byte[] serializedAggregation) {
        Sketch deserialized = Sketches.wrapSketch(new NativeMemory(serializedAggregation));
        unionSketch.update(deserialized);
        combined = true;
    }

    @Override
    public Clip getAggregation() {
        Sketch result = merge();
        double count = result.getEstimate();

        BulletRecord record = new BulletRecord();
        record.setDouble(newName, count);
        Clip clip = Clip.of(record);

        String metaKey = getAggregationMetaKey();
        return metaKey == null ? clip : clip.add(new Metadata().add(metaKey, getSketchMetadata(result, metadataKeys)));
    }

    private CompactSketch merge() {
        // Merge the updateSketch into the unionSketch. Supporting it for completeness.
        if (consumed && combined) {
            unionSketch.update(updateSketch.compact(false, null));
        }

        if (combined) {
            return unionSketch.getResult(false, null);
        } else {
            return updateSketch.compact(false, null);
        }
    }

    private static String getFieldsAsString(List<String> fields, BulletRecord record, String separator) {
        // This explicitly does not do a TypedObject checking. Nulls turn into a String that will be counted.
        return fields.stream().map(field -> Specification.extractField(field, record))
                              .map(Objects::toString)
                              .collect(Collectors.joining(separator));
    }

    private Map<String, Object> getSketchMetadata(Sketch sketch, Map<String, String> conceptKeys) {
        ThetaKMVSketch wrapped = new ThetaKMVSketch(sketch);
        Map<String, Object> metadata = super.getSketchMetadata(wrapped, conceptKeys);

        String familyKey = conceptKeys.get(Concept.SKETCH_FAMILY.getName());
        String sizeKey = conceptKeys.get(Concept.SKETCH_SIZE.getName());

        addIfKeyNonNull(metadata, familyKey, () -> sketch.getFamily().getFamilyName());
        addIfKeyNonNull(metadata, sizeKey, () -> sketch.getCurrentBytes(true));

        return metadata;
    }

    /**
     * Convert a String family into a {@link Family}. For testing.
     *
     * @param family The string version of the {@link Family}. Currently, QuickSelect and Alpha are supported.
     * @return The Sketch family represented by the string or {@link #DEFAULT_UPDATE_SKETCH_FAMILY} otherwise.
     */
    static Family getFamily(String family) {
        return Family.QUICKSELECT.getFamilyName().equals(family) ? Family.QUICKSELECT : Family.ALPHA;
    }
}
