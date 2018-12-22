package org.embulk.input.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class BasicAuthOption {
  private final String user;
  private final String password;

  @JsonCreator
  public BasicAuthOption(
      @JsonProperty("user") String user, @JsonProperty("password") String password) {
    this.user = user;
    this.password = password;
  }

  @JsonProperty("user")
  public String getUser() {
    return user;
  }

  @JsonProperty("password")
  public String getPassword() {
    return password;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(user, password);
  }

  @Override
  public String toString() {
    return String.format("BasicAuthOption[%s, %s]", getUser(), getPassword());
  }
}
