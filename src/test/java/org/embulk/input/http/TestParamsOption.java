package org.embulk.input.http;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestParamsOption {
  @Test
  public void testUnExpandQueriesSinglePair() {
    QueryOption q1 = new QueryOption("test1", "awasome1", null, false);
    QueryOption q2 = new QueryOption("test2", "awasome2", null, false);
    ParamsOption paramsOption = new ParamsOption(Lists.newArrayList(q1, q2));
    List<List<QueryOption.Query>> dest = paramsOption.generateQueries(null);
    assertEquals(dest.size(), 1);
    assertEquals(dest.get(0).size(), 2);
    assertEquals(dest.get(0).get(0).getName(), "test1");
    assertEquals(dest.get(0).get(0).getValues()[0], "awasome1");
    assertEquals(dest.get(0).get(1).getName(), "test2");
    assertEquals(dest.get(0).get(1).getValues()[0], "awasome2");
  }

  @Test
  public void testUnExpandQueriesExpandPair() {
    List<String> values1 = Lists.newArrayList("a", "b");
    List<String> values2 = Lists.newArrayList("c", "d");

    QueryOption q1 = new QueryOption("test1", null, values1, false);
    QueryOption q2 = new QueryOption("test2", null, values2, false);

    ParamsOption paramsOption = new ParamsOption(Lists.newArrayList(q1, q2));
    List<List<QueryOption.Query>> dest = paramsOption.generateQueries(null);
    assertEquals(dest.size(), 1);
    assertEquals(dest.get(0).size(), 2);
    assertEquals(dest.get(0).get(0).getName(), "test1");
    assertEquals(dest.get(0).get(0).getValues()[0], "a");
    assertEquals(dest.get(0).get(0).getValues()[1], "b");
    assertEquals(dest.get(0).get(1).getName(), "test2");
    assertEquals(dest.get(0).get(1).getValues()[0], "c");
    assertEquals(dest.get(0).get(1).getValues()[1], "d");
  }

  @Test
  public void testExpandQueriesSinglePair() {
    QueryOption q1 = new QueryOption("test1", "awasome1", null, true);
    QueryOption q2 = new QueryOption("test2", "awasome2", null, true);
    ParamsOption paramsOption = new ParamsOption(Lists.newArrayList(q1, q2));
    List<List<QueryOption.Query>> dest = paramsOption.generateQueries(null);
    assertEquals(dest.size(), 1);
    assertEquals(dest.get(0).size(), 2);
    assertEquals(dest.get(0).get(0).getName(), "test1");
    assertEquals(dest.get(0).get(0).getValues()[0], "awasome1");
    assertEquals(dest.get(0).get(1).getName(), "test2");
    assertEquals(dest.get(0).get(1).getValues()[0], "awasome2");
  }

  @Test
  public void testExpandQueriesExpandPair() {
    List<String> values1 = Lists.newArrayList("a", "b");
    List<String> values2 = Lists.newArrayList("c", "d");

    QueryOption q1 = new QueryOption("test1", null, values1, true);
    QueryOption q2 = new QueryOption("test2", null, values2, true);

    ParamsOption paramsOption = new ParamsOption(Lists.newArrayList(q1, q2));
    List<List<QueryOption.Query>> dest = paramsOption.generateQueries(null);
    assertEquals(dest.size(), 4);

    assertEquals(dest.get(0).size(), 2);
    assertEquals(dest.get(0).get(0).getName(), "test1");
    assertEquals(dest.get(0).get(0).getValues()[0], "a");
    assertEquals(dest.get(0).get(1).getName(), "test2");
    assertEquals(dest.get(0).get(1).getValues()[0], "c");

    assertEquals(dest.get(1).size(), 2);
    assertEquals(dest.get(1).get(0).getName(), "test1");
    assertEquals(dest.get(1).get(0).getValues()[0], "b");
    assertEquals(dest.get(1).get(1).getName(), "test2");
    assertEquals(dest.get(1).get(1).getValues()[0], "c");

    assertEquals(dest.get(2).size(), 2);
    assertEquals(dest.get(2).get(0).getName(), "test1");
    assertEquals(dest.get(2).get(0).getValues()[0], "a");
    assertEquals(dest.get(2).get(1).getName(), "test2");
    assertEquals(dest.get(2).get(1).getValues()[0], "d");

    assertEquals(dest.get(3).size(), 2);
    assertEquals(dest.get(3).get(0).getName(), "test1");
    assertEquals(dest.get(3).get(0).getValues()[0], "b");
    assertEquals(dest.get(3).get(1).getName(), "test2");
    assertEquals(dest.get(3).get(1).getValues()[0], "d");
  }
}
