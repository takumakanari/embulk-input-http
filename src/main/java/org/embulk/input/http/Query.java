package org.embulk.input.http;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

public class Query {

  private final String name;

  private final String[] values;

  public Query(@JsonProperty("name") String name, @JsonProperty("values") String... values) {
    this.name = name;
    this.values = values;
  }

  public String getName() {
    return name;
  }

  public String[] getValues() {
    return values;
  }

  public Query copy() {
    return new Query(this.name, Arrays.copyOf(this.values, this.values.length));
  }
}
