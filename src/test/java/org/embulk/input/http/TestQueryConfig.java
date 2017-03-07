package org.embulk.input.http;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestQueryConfig {

    @Test
    public void testUnexpandSingleValue() throws Exception {
        Optional<List<String>> nullValues = Optional.absent();
        QueryConfig config = new QueryConfig("test", Optional.of("awesome"), nullValues, false);
        List<QueryConfig.Query> dest = config.expand();
        assertEquals(dest.size(), 1);
        assertEquals(dest.get(0).getName(), "test");
        assertEquals(dest.get(0).getValues().length, 1);
        assertEquals(dest.get(0).getValues()[0], "awesome");
    }

    @Test
    public void testUnexpandMultiValue() throws Exception {
        Optional<String> nullValue = Optional.absent();
        List<String> values = Lists.newArrayList("a", "b", "c");
        QueryConfig config = new QueryConfig("test", nullValue, Optional.of(values), false);
        List<QueryConfig.Query> dest = config.expand();
        assertEquals(dest.size(), 1);
        assertEquals(dest.get(0).getName(), "test");

        assertEquals(dest.get(0).getValues().length, 3);
        assertEquals(dest.get(0).getValues()[0], "a");
        assertEquals(dest.get(0).getValues()[1], "b");
        assertEquals(dest.get(0).getValues()[2], "c");
    }

    @Test
    public void testExpandSingleValue() throws Exception {
        Optional<List<String>> nullValues = Optional.absent();
        QueryConfig config = new QueryConfig("test", Optional.of("awesome"), nullValues, true);
        List<QueryConfig.Query> dest = config.expand();
        assertEquals(dest.size(), 1);
        assertEquals(dest.get(0).getName(), "test");
        assertEquals(dest.get(0).getValues()[0], "awesome");
    }

    @Test
    public void testExpandMultiValue() throws Exception {
        Optional<String> nullValue = Optional.absent();
        List<String> values = Lists.newArrayList("a", "b", "c");
        QueryConfig config = new QueryConfig("test", nullValue, Optional.of(values), true);
        List<QueryConfig.Query> dest = config.expand();
        assertEquals(dest.size(), 3);
        assertEquals(dest.get(0).getName(), "test");
        assertEquals(dest.get(0).getValues().length, 1);
        assertEquals(dest.get(0).getValues()[0], "a");

        assertEquals(dest.get(1).getValues().length, 1);
        assertEquals(dest.get(1).getName(), "test");
        assertEquals(dest.get(1).getValues()[0], "b");

        assertEquals(dest.get(2).getValues().length, 1);
        assertEquals(dest.get(2).getName(), "test");
        assertEquals(dest.get(2).getValues()[0], "c");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandRaisesExceptionWhenBothValuesAreNull() throws Exception {
        Optional<List<String>> nullValues = Optional.absent();
        Optional<String> nullValue = Optional.absent();
        QueryConfig config = new QueryConfig("test", nullValue, nullValues, false);
        config.expand();
    }

    @Test
    public void testUnExpandBrace() throws Exception {
        Optional<List<String>> nullValues = Optional.absent();
        QueryConfig config = new QueryConfig("test", Optional.of("{awesome1,awesome2,awesome3}"), nullValues, false);
        List<QueryConfig.Query> dest = config.expand();
        assertEquals(dest.size(), 1);
        assertEquals(dest.get(0).getName(), "test");
        assertEquals(dest.get(0).getValues().length, 1);
        assertEquals(dest.get(0).getValues()[0], "{awesome1,awesome2,awesome3}");
    }

    @Test
    public void testExpandBrace() throws Exception {
        Optional<List<String>> nullValues = Optional.absent();
        QueryConfig config = new QueryConfig("test", Optional.of("{awesome1,awesome2,awesome3}"), nullValues, true);
        List<QueryConfig.Query> dest = config.expand();
        assertEquals(dest.size(), 3);
        assertEquals(dest.get(0).getName(), "test");
        assertEquals(dest.get(0).getValues().length, 1);
        assertEquals(dest.get(0).getValues()[0], "awesome1");
        assertEquals(dest.get(1).getName(), "test");

        assertEquals(dest.get(1).getValues().length, 1);
        assertEquals(dest.get(1).getValues()[0], "awesome2");

        assertEquals(dest.get(2).getValues().length, 1);
        assertEquals(dest.get(2).getName(), "test");
        assertEquals(dest.get(2).getValues()[0], "awesome3");
    }

    @Test
    public void testExpandEscapedBrace() throws Exception {
        Optional<List<String>> nullValues = Optional.absent();
        QueryConfig config = new QueryConfig("test", Optional.of("{awe\\,some1,awes\\{ome2,awes\\}ome3}"), nullValues, true);
        List<QueryConfig.Query> dest = config.expand();
        assertEquals(dest.get(0).getName(), "test");
        assertEquals(dest.get(0).getValues().length, 1);
        assertEquals(dest.get(0).getValues()[0], "awe,some1");

        assertEquals(dest.get(1).getName(), "test");
        assertEquals(dest.get(1).getValues().length, 1);
        assertEquals(dest.get(1).getValues()[0], "awes{ome2");

        assertEquals(dest.get(2).getName(), "test");
        assertEquals(dest.get(2).getValues().length, 1);
        assertEquals(dest.get(2).getValues()[0], "awes}ome3");
    }

}