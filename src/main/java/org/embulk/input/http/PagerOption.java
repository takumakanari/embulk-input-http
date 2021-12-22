package org.embulk.input.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PagerOption {

  private final String fromParam;

  private final String toParam;

  private final int start;

  private final int pages;

  private final int step;

  @JsonCreator
  public PagerOption(
      @JsonProperty("from_param") String fromParam,
      @JsonProperty("to_param") String toParam,
      @JsonProperty("start") int start,
      @JsonProperty("pages") int pages,
      @JsonProperty("step") Integer step) {
    this.fromParam = fromParam;
    this.toParam = toParam;
    this.start = start;
    this.pages = pages;
    this.step = step == null ? 1 : step;
  }

  public List<List<QueryOption.Query>> expand() {
    List<List<QueryOption.Query>> queries = new ArrayList<>();
    int page = 1;
    int index = start;
    while (page <= pages) {
      List<QueryOption.Query> one = new ArrayList<>();
      one.add(new QueryOption.Query(fromParam, Integer.toString(index)));
      if (toParam != null) {
        int to = index + step - 1;
        one.add(new QueryOption.Query(toParam, Integer.toString(to)));
        index = to + 1;
      } else {
        index += step;
      }
      queries.add(one);
      page++;
    }
    return queries;
  }

  @JsonProperty("from_param")
  public String getFromParam() {
    return fromParam;
  }

  @JsonProperty("to_param")
  public String getToParam() {
    return toParam;
  }

  @JsonProperty("start")
  public int getStart() {
    return start;
  }

  @JsonProperty("pages")
  public int getPages() {
    return pages;
  }

  @JsonProperty("step")
  public int getStep() {
    return step;
  }

  @Override
  public String toString() {
    return "PagerOption{"
        + "fromParam='"
        + fromParam
        + '\''
        + ", toParam="
        + toParam
        + ", start="
        + start
        + ", pages="
        + pages
        + ", step="
        + step
        + '}';
  }
}
