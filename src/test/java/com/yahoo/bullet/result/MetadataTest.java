/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.result;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.result.Metadata.Concept;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class MetadataTest {
    @SafeVarargs
    public static List<Map<String, String>> asMetadataEntries(Map.Entry<String, String>... pairs) {
        List<Map<String, String>> list = new ArrayList<>();
        for (Map.Entry<String, String> pair : pairs) {
            Map<String, String> map = new HashMap<>();
            map.put(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, pair.getKey());
            map.put(BulletConfig.RESULT_METADATA_METRICS_NAME_KEY, pair.getValue());
            list.add(map);
        }
        return list;
    }

    @Test
    public void testConceptFinding() {
        Assert.assertEquals(Concept.from("Creation Time"), Concept.CREATION_TIME);
        Assert.assertEquals(Concept.from("Rule Identifier"), Concept.RULE_ID);
        Assert.assertEquals(Concept.from("Aggregation Metadata"), Concept.AGGREGATION_METADATA);
        Assert.assertEquals(Concept.from("Standard Deviations"), Concept.STANDARD_DEVIATIONS);
        Assert.assertNull(Concept.from("foo"));
        Assert.assertNull(Concept.from("standard deviations"));
    }

    @Test
    public void testErrorsAddition() {
        Error errorA = Error.of("foo", asList("1", "2"));
        Error errorB = Error.of("bar", asList("3", "4"));
        Metadata meta = Metadata.of(errorA, errorB);

        Map<String, Object> actual = meta.asMap();
        Assert.assertEquals(actual.size(), 1);

        List<Error> actualErrors = (List<Error>) actual.get(Metadata.ERROR_KEY);
        Assert.assertEquals(actualErrors.size(), 2);
        Assert.assertEquals(actualErrors.get(0).getError(), "foo");
        Assert.assertEquals(actualErrors.get(0).getResolutions(), asList("1", "2"));
        Assert.assertEquals(actualErrors.get(1).getError(), "bar");
        Assert.assertEquals(actualErrors.get(1).getResolutions(), asList("3", "4"));

        Error errorC = Error.of("baz", asList("5", "6"));
        Error errorD = Error.of("qux", singletonList("7"));
        meta.addErrors(Arrays.asList(errorC, errorD));

        Assert.assertEquals(actualErrors.size(), 4);
        Assert.assertEquals(actualErrors.get(0).getError(), "foo");
        Assert.assertEquals(actualErrors.get(0).getResolutions(), asList("1", "2"));
        Assert.assertEquals(actualErrors.get(1).getError(), "bar");
        Assert.assertEquals(actualErrors.get(1).getResolutions(), asList("3", "4"));
        Assert.assertEquals(actualErrors.get(2).getError(), "baz");
        Assert.assertEquals(actualErrors.get(2).getResolutions(), asList("5", "6"));
        Assert.assertEquals(actualErrors.get(3).getError(), "qux");
        Assert.assertEquals(actualErrors.get(3).getResolutions(), singletonList("7"));
    }

    @Test
    public void testMerging() {
        Metadata metaA = new Metadata();
        metaA.add("foo", singletonList("bar"));
        metaA.add("bar", 1L);
        Metadata metaB = new Metadata();
        metaB.add("baz", singletonMap("a", 1));
        metaB.add("bar", 0.3);
        Metadata metaC = null;

        metaA.merge(metaB);
        metaA.merge(metaC);

        Map<String, Object> expected = new HashMap<>();
        expected.put("foo", singletonList("bar"));
        expected.put("bar", 0.3);
        expected.put("baz", singletonMap("a", 1));

        Assert.assertEquals(metaA.asMap(), expected);
    }

    @Test
    public void testConceptKeyExtractionWithMetadataNotEnabled() {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put(BulletConfig.RESULT_METADATA_METRICS, asMetadataEntries(Pair.of("Estimated Result", "foo")));

        Set<Concept> concepts = new HashSet<>(singletonList(Concept.ESTIMATED_RESULT));

        Assert.assertEquals(Metadata.getConceptNames(configuration, concepts), Collections.emptyMap());
    }

    @Test
    public void testConceptKeyExtraction() {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put(BulletConfig.RESULT_METADATA_ENABLE, true);
        configuration.put(BulletConfig.RESULT_METADATA_METRICS,
                          asMetadataEntries(Pair.of("Estimated Result", "foo"),
                                            Pair.of("Aggregation Metadata", "bar"),
                                            Pair.of("Standard Deviations", "baz")));

        Set<Concept> concepts = new HashSet<>(asList(Concept.ESTIMATED_RESULT,
                                                     Concept.AGGREGATION_METADATA,
                                                     Concept.STANDARD_DEVIATIONS));

        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put(Concept.ESTIMATED_RESULT.getName(), "foo");
        expectedMap.put(Concept.AGGREGATION_METADATA.getName(), "bar");
        expectedMap.put(Concept.STANDARD_DEVIATIONS.getName(), "baz");

        Assert.assertEquals(Metadata.getConceptNames(configuration, concepts), expectedMap);
    }
}
