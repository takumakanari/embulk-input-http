package org.embulk.input.http;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestQueryOption {
  @Test
  public void testUnExpandSingleValue() {
    QueryOption config = new QueryOption("test", "awesome", null, false);
    List<QueryOption.Query> dest = config.expand();
    assertEquals(dest.size(), 1);
    assertEquals(dest.get(0).getName(), "test");
    assertEquals(dest.get(0).getValues().length, 1);
    assertEquals(dest.get(0).getValues()[0], "awesome");
  }

  @Test
  public void testUnExpandMultiValue() {
    List<String> values = Lists.newArrayList("a", "b", "c");
    QueryOption config = new QueryOption("test", null, values, false);
    List<QueryOption.Query> dest = config.expand();
    assertEquals(dest.size(), 1);
    assertEquals(dest.get(0).getName(), "test");

    assertEquals(dest.get(0).getValues().length, 3);
    assertEquals(dest.get(0).getValues()[0], "a");
    assertEquals(dest.get(0).getValues()[1], "b");
    assertEquals(dest.get(0).getValues()[2], "c");
  }

  @Test
  public void testExpandSingleValue() {
    QueryOption config = new QueryOption("test", "awesome", null, true);
    List<QueryOption.Query> dest = config.expand();
    assertEquals(dest.size(), 1);
    assertEquals(dest.get(0).getName(), "test");
    assertEquals(dest.get(0).getValues()[0], "awesome");
  }

  @Test
  public void testExpandMultiValue() {
    List<String> values = Lists.newArrayList("a", "b", "c");
    QueryOption config = new QueryOption("test", null, values, true);
    List<QueryOption.Query> dest = config.expand();
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

  @Test(expected = IllegalStateException.class)
  public void testExpandRaisesExceptionWhenBothValuesAreNull() {
    QueryOption config = new QueryOption("test", null, null, false);
    config.expand();
  }

  @Test
  public void testUnExpandBrace() {
    QueryOption config = new QueryOption("test", "{awesome1,awesome2,awesome3}", null, false);
    List<QueryOption.Query> dest = config.expand();
    assertEquals(dest.size(), 1);
    assertEquals(dest.get(0).getName(), "test");
    assertEquals(dest.get(0).getValues().length, 1);
    assertEquals(dest.get(0).getValues()[0], "{awesome1,awesome2,awesome3}");
  }

  @Test
  public void testExpandBrace() {
    QueryOption config = new QueryOption("test", "{awesome1,awesome2,awesome3}", null, true);
    List<QueryOption.Query> dest = config.expand();
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
  public void testExpandEscapedBrace() {
    QueryOption config =
        new QueryOption("test", "{awe\\,some1,awes\\{ome2,awes\\}ome3}", null, true);
    List<QueryOption.Query> dest = config.expand();
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
