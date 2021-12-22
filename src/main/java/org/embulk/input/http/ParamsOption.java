package org.embulk.input.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ParamsOption {

  private final List<QueryOption> queries;

  @JsonCreator
  public ParamsOption(List<QueryOption> queries) {
    this.queries = queries;
  }

  public List<List<QueryOption.Query>> generateQueries(PagerOption pagerOption) {
    List<List<QueryOption.Query>> base =
        queries.stream().map(QueryOption::expand).collect(Collectors.toList());

    int productSize = 1;
    for (List<QueryOption.Query> queryList : base) {
      productSize *= queryList.size();
    }

    List<List<QueryOption.Query>> expands = new ArrayList<>(productSize);
    for (int i = 0; i < productSize; i++) {
      int exp = 1;
      List<QueryOption.Query> one = new ArrayList<>();
      for (List<QueryOption.Query> list : base) {
        QueryOption.Query pc = list.get((i / exp) % list.size());
        one.add(pc);
        exp *= list.size();
      }
      if (pagerOption != null) {
        for (List<QueryOption.Query> q : pagerOption.expand()) {
          expands.add(flattenMerge(Arrays.asList(one, q)));
        }
      } else {
        expands.add(one);
      }
    }

    return Collections.unmodifiableList(expands);
  }

  @JsonValue
  public List<QueryOption> getQueries() {
    return queries;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ParamsOption)) {
      return false;
    }
    ParamsOption other = (ParamsOption) obj;
    return Objects.equals(queries, other.queries);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(queries);
  }

  private List<QueryOption.Query> flattenMerge(List<List<QueryOption.Query>> sources) {
    return sources.stream()
        .flatMap(s -> s.stream().map(QueryOption.Query::copy))
        .collect(Collectors.toList());
  }
}
