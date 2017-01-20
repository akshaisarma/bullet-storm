package com.yahoo.bullet.operations.aggregations.grouping;

import com.yahoo.sketches.tuple.SummarySetOperations;

/**
 * The stateless implementation of the summary set operations for a {@link GroupDataSummary}. Intersection is not
 * supported.
 */
public class GroupDataSummarySetOperations implements SummarySetOperations<GroupDataSummary> {
    @Override
    public GroupDataSummary union(GroupDataSummary a, GroupDataSummary b) {
        return GroupDataSummary.mergeInPlace(a, b);
    }

    @Override
    public GroupDataSummary intersection(GroupDataSummary a, GroupDataSummary b) {
        throw new UnsupportedOperationException("Intersection is not supported at the moment.");
    }
}
