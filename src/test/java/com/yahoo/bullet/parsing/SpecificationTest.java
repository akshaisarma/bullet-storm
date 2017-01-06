/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.FilterOperations.FilterType;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpecificationTest {
    public static Stream<BulletRecord> makeStream(int count) {
        return IntStream.range(0, count).mapToObj(x -> RecordBox.get().getRecord());
    }

    public static ArrayList<BulletRecord> makeList(int count) {
        return makeStream(count).collect(Collectors.toCollection(ArrayList::new));
    }

    public static int size(BulletRecord record) {
        int size = 0;
        for (Object ignored : record) {
            size++;
        }
        return size;
    }

    @Test
    public void testDefaults() {
        Specification specification = new Specification();
        specification.configure(emptyMap());

        Assert.assertNull(specification.getProjection());
        Assert.assertNull(specification.getFilters());
        Assert.assertEquals(specification.getDuration(), Specification.DEFAULT_DURATION_MS);
        Assert.assertEquals(specification.getAggregation().getType(), AggregationType.RAW);
        Assert.assertEquals(specification.getAggregation().getSize(), Aggregation.DEFAULT_SIZE);
        Assert.assertTrue(specification.isAcceptingData());
        Assert.assertEquals(specification.getAggregate(), emptyList());
    }


    @Test
    public void testExtractField() {
        BulletRecord record = RecordBox.get().add("field", "foo").add("map_field.foo", "bar")
                                             .addMap("map_field", Pair.of("foo", "baz"))
                                             .addList("list_field", singletonMap("foo", "baz"))
                                             .getRecord();

        Assert.assertNull(Specification.extractField(null, record));
        Assert.assertNull(Specification.extractField("", record));
        Assert.assertNull(Specification.extractField("id", record));
        Assert.assertEquals(Specification.extractField("map_field.foo", record), "baz");
        Assert.assertNull(Specification.extractField("list_field.bar", record));
    }

    @Test
    public void testAggregationForced() {
        Specification specification = new Specification();
        specification.setAggregation(null);
        Assert.assertNull(specification.getProjection());
        Assert.assertNull(specification.getFilters());
        // If you had null for aggregation
        Assert.assertNull(specification.getAggregation());
        specification.configure(Collections.emptyMap());
        Assert.assertTrue(specification.isAcceptingData());
        Assert.assertEquals(specification.getAggregate(), emptyList());
    }

    @Test
    public void testDuration() {
        Specification specification = new Specification();
        specification.configure(emptyMap());
        Assert.assertEquals(specification.getDuration(), specification.DEFAULT_DURATION_MS);

        specification.setDuration(-1000);
        specification.configure(emptyMap());
        Assert.assertEquals(specification.getDuration(), specification.DEFAULT_DURATION_MS);

        specification.setDuration(0);
        specification.configure(emptyMap());
        Assert.assertEquals(specification.getDuration(), (Integer) 0);

        specification.setDuration(1);
        specification.configure(emptyMap());
        Assert.assertEquals(specification.getDuration(), (Integer) 1);

        specification.setDuration(Specification.DEFAULT_DURATION_MS);
        specification.configure(emptyMap());
        Assert.assertEquals(specification.getDuration(), Specification.DEFAULT_DURATION_MS);

        specification.setDuration(Specification.DEFAULT_MAX_DURATION_MS);
        specification.configure(emptyMap());
        Assert.assertEquals(specification.getDuration(), Specification.DEFAULT_MAX_DURATION_MS);

        specification.setDuration(Specification.DEFAULT_MAX_DURATION_MS * 2);
        specification.configure(emptyMap());
        Assert.assertEquals(specification.getDuration(), Specification.DEFAULT_MAX_DURATION_MS);
    }

    @Test
    public void testCustomDuration() {
        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.SPECIFICATION_DEFAULT_DURATION, 200);
        config.put(BulletConfig.SPECIFICATION_MAX_DURATION, 1000);

        Specification specification = new Specification();

        specification.setDuration(null);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 200);

        specification.setDuration(-1000);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 200);

        specification.setDuration(0);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 0);

        specification.setDuration(1);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 1);

        specification.setDuration(200);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 200);

        specification.setDuration(1000);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 1000);

        specification.setDuration(2000);
        specification.configure(config);
        Assert.assertEquals(specification.getDuration(), (Integer) 1000);
    }

    @Test
    public void testFiltering() {
        Specification specification = new Specification();
        specification.setFilters(singletonList(FilterClauseTest.getFieldFilter(FilterType.EQUALS, "foo", "bar")));
        specification.configure(emptyMap());

        Assert.assertTrue(specification.filter(RecordBox.get().add("field", "foo").getRecord()));
        Assert.assertTrue(specification.filter(RecordBox.get().add("field", "bar").getRecord()));
        Assert.assertFalse(specification.filter(RecordBox.get().add("field", "baz").getRecord()));
    }

    @Test
    public void testReceiveTimestampNoProjection() {
        Long start = System.currentTimeMillis();

        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.RECORD_INJECT_TIMESTAMP, true);
        Specification specification = new Specification();
        specification.setProjection(null);
        specification.configure(config);

        BulletRecord input = RecordBox.get().add("field", "foo").add("mid", "123").getRecord();
        BulletRecord actual = specification.project(input);

        Long end = System.currentTimeMillis();

        Assert.assertEquals(size(actual), 3);
        Assert.assertEquals(actual.get("field"), "foo");
        Assert.assertEquals(actual.get("mid"), "123");

        Long recordedTimestamp = (Long) actual.get(Specification.DEFAULT_RECEIVE_TIMESTAMP_KEY);
        Assert.assertTrue(recordedTimestamp >= start);
        Assert.assertTrue(recordedTimestamp <= end);
    }

    @Test
    public void testReceiveTimestamp() {
        Long start = System.currentTimeMillis();

        Map<String, Object> config = new HashMap<>();
        config.put(BulletConfig.RECORD_INJECT_TIMESTAMP, true);
        Specification specification = new Specification();
        Projection projection = new Projection();
        projection.setFields(singletonMap("field", "bid"));
        specification.setProjection(projection);
        specification.configure(config);

        BulletRecord input = RecordBox.get().add("field", "foo").add("mid", "123").getRecord();
        BulletRecord actual = specification.project(input);

        Long end = System.currentTimeMillis();

        Assert.assertEquals(size(actual), 2);
        Assert.assertEquals(actual.get("bid"), "foo");

        Long recordedTimestamp = (Long) actual.get(Specification.DEFAULT_RECEIVE_TIMESTAMP_KEY);
        Assert.assertTrue(recordedTimestamp >= start);
        Assert.assertTrue(recordedTimestamp <= end);
    }

    @Test
    public void testAggregationDefault() {
        Specification specification = new Specification();
        Aggregation aggregation = new Aggregation();
        aggregation.setType(null);
        aggregation.setSize(Aggregation.DEFAULT_MAX_SIZE - 1);
        specification.setAggregation(aggregation);

        Assert.assertNull(aggregation.getType());
        specification.configure(emptyMap());

        // Specification no longer fixes type
        Assert.assertNull(aggregation.getType());
        Assert.assertEquals(aggregation.getSize(), new Integer(Aggregation.DEFAULT_MAX_SIZE - 1));
    }

    @Test
    public void testMeetingDefaultSpecification() {
        Specification specification = new Specification();
        specification.configure(emptyMap());
        Assert.assertTrue(makeStream(Aggregation.DEFAULT_SIZE - 1).map(specification::filter).allMatch(x -> x));
        // Check that we only get the default number out
        makeList(Aggregation.DEFAULT_SIZE + 2).forEach(specification::aggregate);
        Assert.assertEquals((Integer) specification.getAggregate().size(), Aggregation.DEFAULT_SIZE);
    }

    @Test
    public void testValidate() {
        Specification specification = new Specification();
        Aggregation mockAggregation = mock(Aggregation.class);
        Optional<List<Error>> aggregationErrors = Optional.of(asList(Error.of("foo", new ArrayList<>()),
                                                                     Error.of("bar", new ArrayList<>())));
        when(mockAggregation.validate()).thenReturn(aggregationErrors);
        specification.setAggregation(mockAggregation);

        Clause mockClauseA = mock(Clause.class);
        Clause mockClauseB = mock(Clause.class);
        when(mockClauseA.validate()).thenReturn(Optional.of(singletonList(Error.of("baz", new ArrayList<>()))));
        when(mockClauseB.validate()).thenReturn(Optional.of(singletonList(Error.of("qux", new ArrayList<>()))));
        specification.setFilters(asList(mockClauseA, mockClauseB));

        Projection mockProjection = mock(Projection.class);
        when(mockProjection.validate()).thenReturn(Optional.of(singletonList(Error.of("quux", new ArrayList<>()))));
        specification.setProjection(mockProjection);

        Optional<List<Error>> errorList = specification.validate();
        Assert.assertTrue(errorList.isPresent());
        Assert.assertEquals(errorList.get().size(), 5);
    }

    @Test
    public void testValidateNullValues() {
        Specification specification = new Specification();
        specification.setProjection(null);
        specification.setFilters(null);
        specification.setAggregation(null);
        Optional<List<Error>> errorList = specification.validate();
        Assert.assertFalse(errorList.isPresent());
    }
}
