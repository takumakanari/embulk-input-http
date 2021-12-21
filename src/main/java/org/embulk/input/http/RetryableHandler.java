package org.embulk.input.http;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.util.EntityUtils;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RetryableHandler implements Retryable<HttpResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetryableHandler.class);

  private static final List<Class<? extends IOException>> NOT_RETRYABLE_CLASSES =
      Collections.unmodifiableList(
          Arrays.asList(
              UnknownHostException.class, InterruptedIOException.class, SSLException.class));

  private final HttpClient client;

  private final HttpRequestBase request;

  public RetryableHandler(HttpClient client, HttpRequestBase request) {
    this.client = client;
    this.request = request;
  }

  @Override
  public HttpResponse call() throws Exception {
    HttpResponse response = client.execute(request);
    statusIsOkOrThrow(response);
    return response;
  }

  @Override
  public boolean isRetryableException(Exception exception) {
    if (NOT_RETRYABLE_CLASSES.contains(exception.getClass())) {
      LOGGER.error(String.format("'%s' is not retryable", exception.getClass()));
      return false;
    }
    return true;
  }

  @Override
  public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) {
    LOGGER.warn(
        "Retrying {}/{} after {} seconds. Message: {}",
        retryCount,
        retryLimit,
        retryWait / 1000,
        exception.getMessage());
  }

  @Override
  public void onGiveup(Exception firstException, Exception lastException) {
    LOGGER.warn(
        "Giving up on retrying: first exception is [{}], last exception is [{}]",
        firstException.getMessage(),
        lastException.getMessage());
  }

  protected void statusIsOkOrThrow(HttpResponse response) throws HttpException, IOException {
    int code = response.getStatusLine().getStatusCode();
    if (response.getStatusLine().getStatusCode() == 200) {
      return;
    }
    throw new HttpException(
        String.format(
            "Request is not successful, code=%d, body=%s",
            code, EntityUtils.toString(response.getEntity())));
  }
}
