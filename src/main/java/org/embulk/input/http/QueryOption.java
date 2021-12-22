package org.embulk.input.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryOption {

  private final String name;

  private final String value;

  private final List<String> values;

  private final boolean expand;

  @JsonCreator
  public QueryOption(
      @JsonProperty("name") String name,
      @JsonProperty("value") String value,
      @JsonProperty("values") List<String> values,
      @JsonProperty("expand") boolean expand) {
    this.name = name;
    this.value = value;
    this.values = values;
    this.expand = expand;
  }

  public List<Query> expand() {
    if (value != null) {
      return expand
          ? Collections.unmodifiableList(
              BraceExpansion.expand(value).stream()
                  .map(e -> new Query(name, e))
                  .collect(Collectors.toList()))
          : Collections.singletonList(new Query(name, value));
    }
    if (values != null) {
      return expand
          ? Collections.unmodifiableList(
              values.stream().map(e -> new Query(name, e)).collect(Collectors.toList()))
          : Collections.singletonList(new Query(name, values.toArray(new String[values.size()])));
    }
    throw new IllegalStateException("params: value or values is required.");
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("value")
  public String getValue() {
    return value;
  }

  @JsonProperty("expand")
  public boolean isExpand() {
    return expand;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof QueryOption)) {
      return false;
    }
    QueryOption other = (QueryOption) obj;
    return Objects.equals(this.name, other.name)
        && Objects.equals(value, other.value)
        && Objects.equals(expand, other.expand);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value, expand);
  }

  @Override
  public String toString() {
    return String.format("ParameterConfig[%s, %s, %s]", getName(), getValue(), isExpand());
  }

  public static class Query {

    private final String name;

    private final String[] values;

    public Query(@JsonProperty("name") String name, @JsonProperty("values") String... values) {
      this.name = name;
      this.values = values;
    }

    public Query copy() {
      return new Query(this.name, Arrays.copyOf(this.values, this.values.length));
    }

    public String getName() {
      return name;
    }

    public String[] getValues() {
      return values;
    }
  }

  private static class BraceExpansion {

    private static List<String> expand(String source) {
      return expandRecursive("", source, "", new ArrayList<>());
    }

    private static List<String> expandRecursive(
        String prefix, String source, String suffix, List<String> dest) {
      // used the code below as reference.
      //  http://rosettacode.org/wiki/Brace_expansion#Java
      int i1 = -1;
      int i2 = 0;
      String noEscape = source.replaceAll("([\\\\]{2}|[\\\\][,}{])", "  ");
      StringBuilder sb = null;

      outer:
      while ((i1 = noEscape.indexOf('{', i1 + 1)) != -1) {
        i2 = i1 + 1;
        sb = new StringBuilder(source);
        for (int depth = 1; i2 < source.length() && depth > 0; i2++) {
          char ch = noEscape.charAt(i2);
          depth = (ch == '{') ? ++depth : depth;
          depth = (ch == '}') ? --depth : depth;
          if (ch == ',' && depth == 1) {
            sb.setCharAt(i2, '\u0000');
          } else if (ch == '}' && depth == 0 && sb.indexOf("\u0000") != -1) {
            break outer;
          }
        }
      }

      if (i1 == -1) {
        if (suffix.length() > 0) {
          expandRecursive(prefix + source, suffix, "", dest);
        } else {
          final String out =
              String.format("%s%s%s", prefix, source, suffix)
                  .replaceAll("[\\\\]{2}", "\\")
                  .replaceAll("[\\\\]([,}{])", "$1");
          dest.add(out);
        }
      } else {
        for (String m : sb.substring(i1 + 1, i2).split("\u0000", -1)) {
          expandRecursive(
              prefix + source.substring(0, i1), m, source.substring(i2 + 1) + suffix, dest);
        }
      }

      return Collections.unmodifiableList(dest);
    }
  }
}
