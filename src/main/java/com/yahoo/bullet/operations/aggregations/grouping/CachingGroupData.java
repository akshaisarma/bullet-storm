package com.yahoo.bullet.operations.aggregations.grouping;

import com.yahoo.bullet.record.BulletRecord;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * This class exists to optimize how the {@link GroupDataSummary} updates the summary for each record update. It
 * stores a copy of the {@link BulletRecord} so that the existing {@link GroupData} in the summary can just
 * {@link GroupData#consume(BulletRecord)} it. It also can only be initialized with already created group fields
 * and metrics. This helps us not have to keep recreating group fields and metrics for every single record.
 */
public class CachingGroupData extends GroupData {
    @Getter @Setter
    private transient BulletRecord cachedRecord;

    /**
     * Constructor that initializes the CachingGroupData with an existing {@link Map} of {@link GroupOperation} to values and
     * a {@link Map} of Strings that represent the group fields. These arguments are not copied.
     *
     * @param groupFields The mappings of field names to their values that represent this group.
     * @param metrics the non-null {@link Map} of metrics for this object.
     */
    public CachingGroupData(Map<String, String> groupFields, Map<GroupOperation, Number> metrics) {
        super(groupFields, metrics);
    }
}
