package org.embulk.input.http;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestPagerOption {
  @Test
  public void testExpandFromTo() {
    List<List<QueryOption.Query>> dest = new PagerOption("from", "to", 1, 3, 2).expand();
    assertEquals(dest.size(), 3);

    assertEquals(dest.get(0).size(), 2);
    assertEquals(dest.get(0).get(0).getName(), "from");
    assertEquals(dest.get(0).get(0).getValues()[0], "1");
    assertEquals(dest.get(0).get(1).getName(), "to");
    assertEquals(dest.get(0).get(1).getValues()[0], "2");

    assertEquals(dest.get(1).size(), 2);
    assertEquals(dest.get(1).get(0).getName(), "from");
    assertEquals(dest.get(1).get(0).getValues()[0], "3");
    assertEquals(dest.get(1).get(1).getName(), "to");
    assertEquals(dest.get(1).get(1).getValues()[0], "4");

    assertEquals(dest.get(2).size(), 2);
    assertEquals(dest.get(2).get(0).getName(), "from");
    assertEquals(dest.get(2).get(0).getValues()[0], "5");
    assertEquals(dest.get(2).get(1).getName(), "to");
    assertEquals(dest.get(2).get(1).getValues()[0], "6");
  }

  @Test
  public void testExpandFromToWithDefault() {
    List<List<QueryOption.Query>> dest = new PagerOption("from", "to", 0, 2, null).expand();
    assertEquals(dest.size(), 2);

    assertEquals(dest.get(0).size(), 2);
    assertEquals(dest.get(0).get(0).getName(), "from");
    assertEquals(dest.get(0).get(0).getValues()[0], "0");
    assertEquals(dest.get(0).get(1).getName(), "to");
    assertEquals(dest.get(0).get(1).getValues()[0], "0");

    assertEquals(dest.get(1).size(), 2);
    assertEquals(dest.get(1).get(0).getName(), "from");
    assertEquals(dest.get(1).get(0).getValues()[0], "1");
    assertEquals(dest.get(1).get(1).getName(), "to");
    assertEquals(dest.get(1).get(1).getValues()[0], "1");
  }

  @Test
  public void testExpandPagenate() {
    List<List<QueryOption.Query>> dest = new PagerOption("page", null, 1, 3, 1).expand();
    assertEquals(dest.size(), 3);

    assertEquals(dest.get(0).size(), 1);
    assertEquals(dest.get(0).get(0).getName(), "page");
    assertEquals(dest.get(0).get(0).getValues()[0], "1");

    assertEquals(dest.get(1).size(), 1);
    assertEquals(dest.get(1).get(0).getName(), "page");
    assertEquals(dest.get(1).get(0).getValues()[0], "2");

    assertEquals(dest.get(2).size(), 1);
    assertEquals(dest.get(2).get(0).getName(), "page");
    assertEquals(dest.get(2).get(0).getValues()[0], "3");
  }
}
