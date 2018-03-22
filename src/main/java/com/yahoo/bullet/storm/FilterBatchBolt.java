package com.yahoo.bullet.storm;

import com.yahoo.bullet.record.BulletRecord;
import org.apache.storm.tuple.Tuple;

import java.util.List;

public class FilterBatchBolt extends FilterBolt {
    private static final long serialVersionUID = -4249464558005914458L;

    /**
     * Constructor that accepts the name of the component that the records are coming from and the validated config.
     *
     * @param recordComponent The source component name for records.
     * @param config The validated {@link BulletStormConfig} to use.
     */
    public FilterBatchBolt(String recordComponent, BulletStormConfig config) {
        super(recordComponent, config);
    }

    @Override
    protected void onRecord(Tuple tuple) {
        List<BulletRecord> records = (List<BulletRecord>) tuple.getValue(TopologyConstants.RECORD_POSITION);
        records.forEach(this::handleRecord);
    }
}
