package com.yahoo.bullet.operations.aggregations.sketches;

import com.yahoo.sketches.theta.Sketch;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ThetaKMVSketch implements KMVSketch {
    private final Sketch sketch;

    @Override
    public boolean isEstimationMode() {
        return sketch.isEstimationMode();
    }

    @Override
    public double getTheta() {
        return sketch.getTheta();
    }

    @Override
    public double getLowerBound(int standardDeviation) {
        return sketch.getLowerBound(standardDeviation);
    }

    @Override
    public double getUpperBound(int standardDeviation) {
        return sketch.getUpperBound(standardDeviation);
    }
}
