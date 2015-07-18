package org.embulk.input;

import com.google.common.collect.ImmutableList;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.util.EntityUtils;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.List;

public class RetryableHandler implements RetryExecutor.Retryable {

    protected final Logger logger = Exec.getLogger(getClass());

    private static List<Class<? extends IOException>> NOT_RETRIABLE_CLAASSES;

    private final HttpClient client;
    private final HttpRequestBase request;
    private HttpResponse response;

    static {
        ImmutableList.Builder<Class<? extends IOException>> classes = ImmutableList.builder();
        classes.add(UnknownHostException.class).
                add(InterruptedIOException.class).
                add(SSLException.class);
        NOT_RETRIABLE_CLAASSES = classes.build();
    }

    public RetryableHandler(HttpClient client, HttpRequestBase request) {
        this.client = client;
        this.request = request;
    }

    public HttpResponse getResponse() {
        return response;
    }

    @Override
    public Object call() throws Exception {
        if (response != null) throw new IllegalStateException("response is already set");
        HttpResponse response = client.execute(request);
        statusIsOkOrThrow(response);
        this.response = response;
        return null;
    }

    @Override
    public boolean isRetryableException(Exception exception) {
        if (NOT_RETRIABLE_CLAASSES.contains(exception.getClass())) {
            logger.error(String.format("'%s' is not retriable", exception.getClass()));
            return false;
        }
        return true;
    }

    @Override
    public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
            throws RetryExecutor.RetryGiveupException {
        logger.warn("retrying {}/{} after {} seconds. Message: {}",
                retryCount, retryLimit, retryWait / 1000,
                exception.getMessage());
    }

    @Override
    public void onGiveup(Exception firstException, Exception lastException)
            throws RetryExecutor.RetryGiveupException {
        logger.error("giveup {}", lastException.getMessage());
    }

    protected void statusIsOkOrThrow(HttpResponse response)
            throws HttpException, IOException {
        int code = response.getStatusLine().getStatusCode();
        switch (response.getStatusLine().getStatusCode()) {
            case 200:
                return;
            default:
                throw new HttpException(String.format("Request is not successful, code=%d, body=%s",
                        code, EntityUtils.toString(response.getEntity())));
        }
    }

}
