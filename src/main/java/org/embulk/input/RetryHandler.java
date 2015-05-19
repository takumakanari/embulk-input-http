package org.embulk.input;

import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Arrays;

public class RetryHandler extends DefaultHttpRequestRetryHandler
{

    private final Logger logger = Exec.getLogger(getClass());

    private int interval = 0;

    public RetryHandler(int retry, int interval)
    {
        super(retry, true, Arrays.asList(
                UnknownHostException.class,
                ConnectException.class,
                SSLException.class));
        this.interval = interval;
    }

    @Override
    public boolean retryRequest(final IOException exception,
                                final int executionCount, final HttpContext context)
    {
        final boolean isRetriable = super.retryRequest(exception, executionCount, context);
        if (isRetriable) {
            try {
                logger.info(String.format("Sleep %d msec before retry", interval));
                Thread.sleep(interval);
            } catch (InterruptedException e) {}
        }
        return isRetriable;
    }

}
