package com.yahoo.bullet.operations.aggregations.grouping;

import com.yahoo.sketches.tuple.SummarySetOperations;

public class GroupDataSummarySetOperations implements SummarySetOperations<GroupDataSummary> {
    @Override
    public GroupDataSummary union(GroupDataSummary a, GroupDataSummary b) {
        GroupDataSummary result = new GroupDataSummary();
        if (a != null) {
            result.update(a.getData());
        }
        if (b != null) {
            result.update(b.getData());
        }
        return result;
    }

    @Override
    public GroupDataSummary intersection(GroupDataSummary a, GroupDataSummary b) {
        throw new UnsupportedOperationException("Intersection is not supported at the moment.");
    }
}
