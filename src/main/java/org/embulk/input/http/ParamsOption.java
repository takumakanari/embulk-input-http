package org.embulk.input.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.List;

public class ParamsOption {
  private final List<QueryOption> queries;

  @JsonCreator
  public ParamsOption(List<QueryOption> queries) {
    this.queries = queries;
  }

  @JsonValue
  public List<QueryOption> getQueries() {
    return queries;
  }

  public List<List<Query>> generateQueries(Optional<PagerOption> pagerOption) {
    List<List<Query>> base = new ArrayList<>(queries.size());
    for (QueryOption p : queries) {
      base.add(p.expand());
    }

    int productSize = 1;
    int baseSize = base.size();
    for (int i = 0; i < baseSize; productSize *= base.get(i).size(), i++) {}

    List<List<Query>> expands = new ArrayList<>(productSize);
    for (int i = 0; i < productSize; i++) {
      int j = 1;
      List<Query> one = new ArrayList<>();
      for (List<Query> list : base) {
        Query pc = list.get((i / j) % list.size());
        one.add(pc);
        j *= list.size();
      }
      if (pagerOption.isPresent()) {
        for (List<Query> q : pagerOption.get().expand()) {
          expands.add(copyAndConcat(one, q));
        }
      } else {
        expands.add(one);
      }
    }

    return expands;
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
    return Objects.equal(queries, other.queries);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(queries);
  }

  private List<Query> copyAndConcat(List<Query>... srcs) {
    List<Query> dest = new ArrayList<>();
    for (List<Query> src : srcs) {
      for (Query q : src) {
        dest.add(q.copy());
      }
    }
    return dest;
  }
}
