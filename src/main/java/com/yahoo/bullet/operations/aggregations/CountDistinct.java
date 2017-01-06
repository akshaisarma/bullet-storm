package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Specification;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CountDistinct implements Strategy {
    private UpdateSketch updateSketch;
    private Union unionSketch;
    private Set<String> fields;
    private String newName;

    private boolean consumed = false;
    private boolean combined = false;

    public static final String NEW_NAME_KEY = "newName";
    public static final String DEFAULT_NEW_NAME = "COUNT DISTINCT";

    // TODO: Make the following defaults configurable.

    // Sketch parameters
    private float samplingProbability = DEFAULT_SAMPLING_PROBABILITY;
    private Family sketchFamily = DEFAULT_UPDATE_SKETCH_FAMILY;
    private ResizeFactor resizeFactor = DEFAULT_RESIZE_FACTOR;
    private int nominalEntries = DEFAULT_NOMINAL_ENTRIES;

    // Separator for multiple fields when inserting into the Sketch
    private String separator = DEFAULT_FIELD_SEPARATOR;

    public static final String DEFAULT_FIELD_SEPARATOR = "|";

    // Sketch defaults
    // No sampling
    public static final float DEFAULT_SAMPLING_PROBABILITY = 1.0f;

    // Recommended for real-time systems
    public static final Family DEFAULT_UPDATE_SKETCH_FAMILY = Family.ALPHA;

    // Sketch * 8 its size upto 2 * nominal entries everytime it reaches cap
    public static final ResizeFactor DEFAULT_RESIZE_FACTOR = ResizeFactor.X8;

    // This gives us (Alpha sketches fall back to QuickSelect RSEs after compaction or set operations) a 2.34% error
    // rate at 99.73% confidence (3 SD).
    public static final int DEFAULT_NOMINAL_ENTRIES = 16384;

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    public CountDistinct(Aggregation aggregation) {
        fields = aggregation.getFields().keySet();
        newName = (aggregation.getAttributes().getOrDefault(NEW_NAME_KEY, DEFAULT_NEW_NAME)).toString();

        updateSketch = UpdateSketch.builder().setFamily(sketchFamily).setNominalEntries(nominalEntries)
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
        return null;
    }


    private CompactSketch merge() {
        // Merge the updateSketch into the unionSketch. Does not happen normally but supporting it for completeness
        if (consumed && combined) {
            unionSketch.update(updateSketch.compact(false, null));
        }

        if (combined) {
            return unionSketch.getResult(false, null);
        } else {
            return updateSketch.compact(false, null);
        }
    }

    private static String getFieldsAsString(Set<String> fields, BulletRecord record, String separator) {
        // This explicitly does not do a TypedObject checking
        return fields.stream().map(field -> Specification.extractField(field, record))
                              .map(Objects::toString)
                              .collect(Collectors.joining(separator));
    }
}
