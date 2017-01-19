package com.yahoo.bullet.operations.aggregations.sketches;

public interface KMVSketch {
    /**
     * Returns whether this sketch was in estimation mode or not.
     * @return A boolean denoting whether this sketch was estimating.
     */
    boolean isEstimationMode();

    /**
     * Gets the theta value for this sketch.
     * @return A double value that is the theta for this sketch.
     */
    double getTheta();

    /**
     * Gets the lower bound at this standard deviation.
     *
     * @param standardDeviation The standard deviation.
     * @return A double representing the maximum value at this standard deviation.
     */
    double getLowerBound(int standardDeviation);

    /**
     * Gets the uppper bound at this standard deviation.
     *
     * @param standardDeviation The standard deviation.
     * @return A double representing the minimum value at this standard deviation.
     */
    double getUpperBound(int standardDeviation);
}
