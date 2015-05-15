package org.embulk.input;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.embulk.config.*;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class HttpInputPlugin implements FileInputPlugin {

    private final Logger logger = Exec.getLogger(getClass());

    public interface PluginTask extends Task {
        @Config("url")
        public String getUrl();

        @Config("charset")
        @ConfigDefault("\"utf-8\"")
        public String getCharset();

        @Config("method")
        @ConfigDefault("\"get\"")
        public String getMethod();

        @Config("user_agent")
        @ConfigDefault("\"Embulk::Input::HttpInputPlugin\"")
        public String getUserAgent();

        @Config("open_timeout")
        @ConfigDefault("2000")
        public int getOpenTimeout();

        @Config("read_timeout")
        @ConfigDefault("10000")
        public int getReadTimeout();

        @Config("max_retries")
        @ConfigDefault("5")
        public int getMaxRetries();

        @Config("retry_interval")
        @ConfigDefault("10000")
        public int getRetryInterval();

        @Config("sleep_before_request")
        @ConfigDefault("0")
        public int getSleepBeforeRequest();
        public void setSleepBeforeRequest(int sleepBeforeRequest);

        @Config("params")
        @ConfigDefault("null")
        public Optional<ParamsConfig> getParams();

        @Config("basic_auth")
        @ConfigDefault("null")
        public Optional<BasicAuthConfig> getBasicAuth();

        @ConfigInject
        public BufferAllocator getBufferAllocator();

        public List<ParamsConfig> getQueries();
        public void setQueries(List<ParamsConfig> queries);

        public HttpMethod getHttpMethod();
        public void setHttpMethod(HttpMethod httpMethod);
    }

    public enum HttpMethod {
        POST,
        GET
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        int numOfThreads = 1;
        if (task.getParams().isPresent()) {
            List<ParamsConfig> expandedQueries = task.getParams().get().expandQueries();
            task.setQueries(expandedQueries);
            numOfThreads = expandedQueries.size();
        } else {
            task.setQueries(new ArrayList<ParamsConfig>());
        }

        if (numOfThreads == 1) {
            task.setSleepBeforeRequest(0);
        }

        switch (task.getMethod().toUpperCase()) {
            case "GET":
                task.setHttpMethod(HttpMethod.GET);
                break;
            case "POST":
                task.setHttpMethod(HttpMethod.POST);
                break;
            default:
                throw new ConfigException(String.format("Unsupported http method %s", task.getMethod()));
        }

        return resume(task.dump(), numOfThreads, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control) {
        control.run(taskSource, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<CommitReport> successCommitReports) {
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex) {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        HttpRequestBase request;
        try {
            request = makeRequest(task, taskIndex);
        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }

        HttpClientBuilder builder = HttpClientBuilder.create()
                .setDefaultRequestConfig(makeRequestConfig(task))
                .setDefaultHeaders(makeHeaders(task));

        if (task.getMaxRetries() > 0) {
            final int retry = task.getMaxRetries();
            final int interval = task.getRetryInterval();
            HttpRequestRetryHandler retryHandler = new RetryHandler(retry, interval);
            builder.setRetryHandler(retryHandler);
        }

        if (task.getBasicAuth().isPresent()) {
            builder.setDefaultCredentialsProvider(makeCredentialsProvider(task.getBasicAuth().get(),
                    request));
        }

        HttpClient client = builder.build();

        if (task.getSleepBeforeRequest() > 0) {
            try {
                logger.info(String.format("Waiting %d msec ...", task.getSleepBeforeRequest()));
                Thread.sleep(task.getSleepBeforeRequest());
            } catch (InterruptedException e) {
            }
        }

        logger.info(String.format("%s \"%s\"", task.getMethod().toUpperCase(),
                request.getURI().toString()));
        try {
            HttpResponse response = client.execute(request);
            statusIsOkOrThrow(response);
            //final String body = EntityUtils.toString(response.getEntity());
            //InputStream stream = new ByteArrayInputStream(body.getBytes());
            InputStream stream = response.getEntity().getContent();
            PluginFileInput input = new PluginFileInput(task, stream);
            stream = null;
            return input;
        } catch (IOException | HttpException e) {
            throw Throwables.propagate(e);
        }
    }

    private CredentialsProvider makeCredentialsProvider(BasicAuthConfig config, HttpRequestBase scopeRequest) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        final AuthScope authScope = new AuthScope(scopeRequest.getURI().getHost(),
                scopeRequest.getURI().getPort());
        credentialsProvider.setCredentials(authScope,
                new UsernamePasswordCredentials(config.getUser(), config.getPassword()));
        return credentialsProvider;
    }

    private HttpRequestBase makeRequest(PluginTask task, int taskIndex)
            throws URISyntaxException, UnsupportedEncodingException {
        final ParamsConfig paramsConfig = (task.getQueries().isEmpty()) ?
                null : task.getQueries().get(taskIndex);
        if (task.getHttpMethod() == HttpMethod.GET) {
            HttpGet request = new HttpGet(task.getUrl());
            if (paramsConfig != null) {
                URIBuilder builder = new URIBuilder(request.getURI());
                for (QueryConfig p : paramsConfig.getQueries()) {
                    builder.addParameter(p.getName(), p.getValue());
                }
                request.setURI(builder.build());
            }
            return request;
        } else if (task.getHttpMethod() == HttpMethod.POST) {
            HttpPost request = new HttpPost(task.getUrl());
            if (paramsConfig != null) {
                List<NameValuePair> pairs = new ArrayList<>();
                for (QueryConfig p : paramsConfig.getQueries()) {
                    pairs.add(new BasicNameValuePair(p.getName(), p.getValue()));
                }
                request.setEntity(new UrlEncodedFormEntity(pairs));
            }
            return request;
        }
        throw new IllegalArgumentException(String.format("Unsupported http method %s", task.getMethod()));
    }

    private List<Header> makeHeaders(PluginTask task) {
        List<Header> headers = new ArrayList<>();
        headers.add(new BasicHeader("Accept", "*/*"));
        headers.add(new BasicHeader("Accept-Charset", task.getCharset()));
        headers.add(new BasicHeader("Accept-Encoding", "gzip, deflate"));
        headers.add(new BasicHeader("Accept-Language", "en-us,en;q=0.5"));
        headers.add(new BasicHeader("User-Agent", task.getUserAgent()));
        return headers;
    }

    private RequestConfig makeRequestConfig(PluginTask task) {
        return RequestConfig.custom()
                .setCircularRedirectsAllowed(true)
                .setMaxRedirects(10)
                .setRedirectsEnabled(true)
                .setConnectTimeout(task.getOpenTimeout())
                .setSocketTimeout(task.getReadTimeout())
                .build();
    }

    private void statusIsOkOrThrow(HttpResponse response)
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

    public static class PluginFileInput extends InputStreamFileInput
            implements TransactionalFileInput {

        private static class SingleFileProvider
                implements InputStreamFileInput.Provider {

            private InputStream stream;
            private boolean opened = false;

            public SingleFileProvider(InputStream stream) {
                this.stream = stream;
            }

            @Override
            public InputStream openNext() throws IOException {
                if (opened) {
                    return null;
                }
                opened = true;
                return stream;
            }

            @Override
            public void close() throws IOException {
                if (!opened) {
                    stream.close();
                }
            }
        }

        public PluginFileInput(PluginTask task, InputStream stream) {
            super(task.getBufferAllocator(), new SingleFileProvider(stream));
        }

        public void abort() {
        }

        public CommitReport commit() {
            return Exec.newCommitReport();
        }

        @Override
        public void close() {
        }
    }

}
