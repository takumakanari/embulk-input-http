package org.embulk.input.http;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestParamsOption {
  @Test
  public void testUnexpandQueriesSinglePair() throws Exception {
    Optional<List<String>> nullValues = Optional.absent();
    QueryOption q1 = new QueryOption("test1", Optional.of("awasome1"), nullValues, false);
    QueryOption q2 = new QueryOption("test2", Optional.of("awasome2"), nullValues, false);
    ParamsOption paramsOption = new ParamsOption(Lists.newArrayList(q1, q2));
    Optional<PagerOption> pagerOption = Optional.absent();
    List<List<Query>> dest = paramsOption.generateQueries(pagerOption);
    assertEquals(dest.size(), 1);
    assertEquals(dest.get(0).size(), 2);
    assertEquals(dest.get(0).get(0).getName(), "test1");
    assertEquals(dest.get(0).get(0).getValues()[0], "awasome1");
    assertEquals(dest.get(0).get(1).getName(), "test2");
    assertEquals(dest.get(0).get(1).getValues()[0], "awasome2");
  }

  @Test
  public void testUnexpandQueriesExpandPair() throws Exception {
    Optional<String> nullValue = Optional.absent();
    List<String> values1 = Lists.newArrayList("a", "b");
    List<String> values2 = Lists.newArrayList("c", "d");

    QueryOption q1 = new QueryOption("test1", nullValue, Optional.of(values1), false);
    QueryOption q2 = new QueryOption("test2", nullValue, Optional.of(values2), false);

    ParamsOption paramsOption = new ParamsOption(Lists.newArrayList(q1, q2));
    Optional<PagerOption> pagerOption = Optional.absent();
    List<List<Query>> dest = paramsOption.generateQueries(pagerOption);
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
  public void testExpandQueriesSinglePair() throws Exception {
    Optional<List<String>> nullValues = Optional.absent();
    QueryOption q1 = new QueryOption("test1", Optional.of("awasome1"), nullValues, true);
    QueryOption q2 = new QueryOption("test2", Optional.of("awasome2"), nullValues, true);
    ParamsOption paramsOption = new ParamsOption(Lists.newArrayList(q1, q2));
    Optional<PagerOption> pagerOption = Optional.absent();
    List<List<Query>> dest = paramsOption.generateQueries(pagerOption);
    assertEquals(dest.size(), 1);
    assertEquals(dest.get(0).size(), 2);
    assertEquals(dest.get(0).get(0).getName(), "test1");
    assertEquals(dest.get(0).get(0).getValues()[0], "awasome1");
    assertEquals(dest.get(0).get(1).getName(), "test2");
    assertEquals(dest.get(0).get(1).getValues()[0], "awasome2");
  }

  @Test
  public void testExpandQueriesExpandPair() throws Exception {
    Optional<String> nullValue = Optional.absent();
    List<String> values1 = Lists.newArrayList("a", "b");
    List<String> values2 = Lists.newArrayList("c", "d");

    QueryOption q1 = new QueryOption("test1", nullValue, Optional.of(values1), true);
    QueryOption q2 = new QueryOption("test2", nullValue, Optional.of(values2), true);

    ParamsOption paramsOption = new ParamsOption(Lists.newArrayList(q1, q2));
    Optional<PagerOption> pagerOption = Optional.absent();
    List<List<Query>> dest = paramsOption.generateQueries(pagerOption);
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
