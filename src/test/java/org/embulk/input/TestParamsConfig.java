package org.embulk.input;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TestParamsConfig {

    @Test
    public void testExpandQueriesSinglePair() throws Exception {
        Optional<List<String>> nullValues = Optional.absent();
        QueryConfig q1 = new QueryConfig("test1", Optional.of("awasome1"), nullValues, false);
        QueryConfig q2 = new QueryConfig("test2", Optional.of("awasome2"), nullValues, false);
        ParamsConfig paramsConfig = new ParamsConfig(Lists.newArrayList(q1, q2));
        List<List<QueryConfig.Query>> dest = paramsConfig.expandQueries();
        assertEquals(dest.size(), 1);
        assertEquals(dest.get(0).size(), 2);
        assertEquals(dest.get(0).get(0).getName(), "test1");
        assertEquals(dest.get(0).get(0).getValue(), "awasome1");
        assertEquals(dest.get(0).get(1).getName(), "test2");
        assertEquals(dest.get(0).get(1).getValue(), "awasome2");
    }

    @Test
    public void testExpandQueriesExpandPair() throws Exception {
        Optional<String> nullValue = Optional.absent();
        List<String> values1 = Lists.newArrayList("a", "b");
        List<String> values2 = Lists.newArrayList("c", "d");

        QueryConfig q1 = new QueryConfig("test1", nullValue, Optional.of(values1), false);
        QueryConfig q2 = new QueryConfig("test2", nullValue, Optional.of(values2), false);

        ParamsConfig paramsConfig = new ParamsConfig(Lists.newArrayList(q1, q2));
        List<List<QueryConfig.Query>> dest = paramsConfig.expandQueries();
        assertEquals(dest.size(), 4);

        assertEquals(dest.get(0).size(), 2);
        assertEquals(dest.get(0).get(0).getName(), "test1");
        assertEquals(dest.get(0).get(0).getValue(), "a");
        assertEquals(dest.get(0).get(1).getName(), "test2");
        assertEquals(dest.get(0).get(1).getValue(), "c");

        assertEquals(dest.get(1).size(), 2);
        assertEquals(dest.get(1).get(0).getName(), "test1");
        assertEquals(dest.get(1).get(0).getValue(), "b");
        assertEquals(dest.get(1).get(1).getName(), "test2");
        assertEquals(dest.get(1).get(1).getValue(), "c");

        assertEquals(dest.get(2).size(), 2);
        assertEquals(dest.get(2).get(0).getName(), "test1");
        assertEquals(dest.get(2).get(0).getValue(), "a");
        assertEquals(dest.get(2).get(1).getName(), "test2");
        assertEquals(dest.get(2).get(1).getValue(), "d");

        assertEquals(dest.get(3).size(), 2);
        assertEquals(dest.get(3).get(0).getName(), "test1");
        assertEquals(dest.get(3).get(0).getValue(), "b");
        assertEquals(dest.get(3).get(1).getName(), "test2");
        assertEquals(dest.get(3).get(1).getValue(), "d");
    }
}