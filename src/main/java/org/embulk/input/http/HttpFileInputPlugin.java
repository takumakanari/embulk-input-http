package org.embulk.input.http;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.lang.String.format;

public class HttpFileInputPlugin implements FileInputPlugin
{
    private final Logger logger = Exec.getLogger(getClass());

    public interface PluginTask extends Task
    {
        @Config("url")
        String getUrl();

        @Config("charset")
        @ConfigDefault("\"utf-8\"")
        String getCharset();

        @Config("method")
        @ConfigDefault("\"get\"")
        String getMethod();

        @Config("user_agent")
        @ConfigDefault("\"Embulk::Input::HttpFileInputPlugin\"")
        String getUserAgent();

        @Config("open_timeout")
        @ConfigDefault("2000")
        int getOpenTimeout();

        @Config("read_timeout")
        @ConfigDefault("10000")
        int getReadTimeout();

        @Config("max_retries")
        @ConfigDefault("5")
        int getMaxRetries();

        @Config("retry_interval")
        @ConfigDefault("10000")
        int getRetryInterval();

        @Config("request_interval")
        @ConfigDefault("0")
        int getRequestInterval();

        void setRequestInterval(int requestInterval);

        @Config("interval_includes_response_time")
        @ConfigDefault("null")
        boolean getIntervalIncludesResponseTime();

        @Config("input_direct")
        @ConfigDefault("true")
        boolean getInputDirect();

        @Config("params")
        @ConfigDefault("null")
        Optional<ParamsOption> getParams();

        @Config("basic_auth")
        @ConfigDefault("null")
        Optional<BasicAuthOption> getBasicAuth();

        @Config("pager")
        @ConfigDefault("null")
        Optional<PagerOption> getPager();

        @Config("request_headers")
        @ConfigDefault("{}")
        Map<String, String> getRequestHeaders();

        @ConfigInject
        BufferAllocator getBufferAllocator();

        List<List<QueryOption.Query>> getQueries();

        void setQueries(List<List<QueryOption.Query>> queries);

        HttpMethod getHttpMethod();

        void setHttpMethod(HttpMethod httpMethod);
    }

    public enum HttpMethod
    {
        POST,
        GET
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        final int tasks;
        if (task.getParams().isPresent()) {
            List<List<QueryOption.Query>> queries = task.getParams().get().generateQueries(task.getPager());
            task.setQueries(queries);
            tasks = queries.size();
        }
        else if (task.getPager().isPresent()) {
            List<List<QueryOption.Query>> queries = task.getPager().get().expand();
            task.setQueries(queries);
            tasks = queries.size();
        }
        else {
            task.setQueries(Lists.<List<QueryOption.Query>>newArrayList());
            task.setRequestInterval(0);
            tasks = 1;
        }

        task.setHttpMethod(HttpMethod.valueOf(task.getMethod().toUpperCase()));

        return resume(task.dump(), tasks, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, FileInputPlugin.Control control)
    {
        control.run(taskSource, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        HttpRequestBase request;
        try {
            request = makeRequest(task, taskIndex);
        }
        catch (URISyntaxException | UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }

        HttpClientBuilder builder = HttpClientBuilder.create()
                .disableAutomaticRetries()
                .setDefaultRequestConfig(makeRequestConfig(task))
                .setDefaultHeaders(makeHeaders(task));

        if (task.getBasicAuth().isPresent()) {
            builder.setDefaultCredentialsProvider(makeCredentialsProvider(task.getBasicAuth().get(), request));
        }

        HttpClient client = builder.build();

        logger.info(format(Locale.ENGLISH, "%s \"%s\"", task.getMethod().toUpperCase(), request.getURI().toString()));

        RetryableHandler retryable = new RetryableHandler(client, request);
        long startTimeMills = System.currentTimeMillis();
        try {
            RetryExecutor.retryExecutor().
                    withRetryLimit(task.getMaxRetries()).
                    withInitialRetryWait(task.getRetryInterval()).
                    withMaxRetryWait(30 * 60 * 1000).
                    runInterruptible(retryable);

            InputStream stream = retryable.getResponse().getEntity().getContent();
            if (!task.getInputDirect()) {
                stream = copyToFile(stream);
            }

            PluginFileInput input = new PluginFileInput(task, stream, startTimeMills);
            stream = null;
            return input;
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private InputStream copyToFile(InputStream input)
            throws IOException
    {
        File tmpfile = Files.createTempFile("embulk-input-http.", ".tmp").toFile();
        tmpfile.deleteOnExit();

        try (FileOutputStream output = new FileOutputStream(tmpfile)) {
            logger.info(format(Locale.ENGLISH, "Writing response to %s", tmpfile));
            IOUtils.copy(input, output);
        } finally {
            input.close();
        }

        return new FileInputStream(tmpfile);
    }

    private CredentialsProvider makeCredentialsProvider(BasicAuthOption basicAuth, HttpRequestBase request)
    {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        final AuthScope authScope = new AuthScope(request.getURI().getHost(),
                request.getURI().getPort());
        credentialsProvider.setCredentials(authScope,
                new UsernamePasswordCredentials(basicAuth.getUser(), basicAuth.getPassword()));
        return credentialsProvider;
    }

    private HttpRequestBase makeRequest(PluginTask task, int taskIndex)
            throws URISyntaxException, UnsupportedEncodingException
    {
        final List<QueryOption.Query> queries = (task.getQueries().isEmpty()) ?
                null : task.getQueries().get(taskIndex);
        if (task.getHttpMethod() == HttpMethod.GET) {
            HttpGet request = new HttpGet(task.getUrl());
            if (queries != null) {
                URIBuilder builder = new URIBuilder(request.getURI());
                for (QueryOption.Query q : queries) {
                    for (String v : q.getValues()) {
                        builder.addParameter(q.getName(), v);
                    }
                }
                request.setURI(builder.build());
            }
            return request;
        }
        else if (task.getHttpMethod() == HttpMethod.POST) {
            HttpPost request = new HttpPost(task.getUrl());
            if (queries != null) {
                List<NameValuePair> pairs = new ArrayList<>();
                for (QueryOption.Query q : queries) {
                    for (String v : q.getValues()) {
                        pairs.add(new BasicNameValuePair(q.getName(), v));
                    }
                }
                request.setEntity(new UrlEncodedFormEntity(pairs));
            }
            return request;
        }
        throw new IllegalArgumentException(String.format("Unsupported http method %s", task.getMethod()));
    }

    private List<Header> makeHeaders(PluginTask task)
    {
        List<Header> headers = new ArrayList<>();
        headers.add(new BasicHeader("Accept", "*/*"));
        headers.add(new BasicHeader("Accept-Charset", task.getCharset()));
        headers.add(new BasicHeader("Accept-Encoding", "gzip, deflate"));
        headers.add(new BasicHeader("Accept-Language", "en-us,en;q=0.5"));
        headers.add(new BasicHeader("User-Agent", task.getUserAgent()));
        for (Map.Entry<String, String> entry : task.getRequestHeaders().entrySet()) {
            headers.add(new BasicHeader(entry.getKey(), entry.getValue()));
        }
        return headers;
    }

    private RequestConfig makeRequestConfig(PluginTask task)
    {
        return RequestConfig.custom()
                .setCircularRedirectsAllowed(true)
                .setMaxRedirects(10)
                .setRedirectsEnabled(true)
                .setConnectTimeout(task.getOpenTimeout())
                .setSocketTimeout(task.getReadTimeout())
                .build();
    }

    public static class PluginFileInput extends InputStreamFileInput
            implements TransactionalFileInput
    {
        private final Logger logger = Exec.getLogger(getClass());

        private final long startTimeMills;
        private final PluginTask task;

        public PluginFileInput(PluginTask task, InputStream stream, long startTimeMills)
        {
            super(task.getBufferAllocator(), new SingleFileProvider(stream));
            this.startTimeMills = startTimeMills;
            this.task = task;
        }

        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }

        @Override
        public void close()
        {
            super.close();
            handleInterval();
        }

        @Override
        public void abort()
        {
        }

        protected void handleInterval()
        {
            if (task.getRequestInterval() <= 0) {
                return;
            }
            long interval = task.getRequestInterval();
            if (task.getIntervalIncludesResponseTime()) {
                interval = interval - (System.currentTimeMillis() - startTimeMills);
            }
            if (interval > 0) {
                logger.info(String.format("waiting %d msec ...", interval));
                try {
                    Thread.sleep(interval);
                }
                catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
            }
        }

        private static class SingleFileProvider
                implements InputStreamFileInput.Provider
        {
            private final InputStream stream;
            private boolean opened = false;

            public SingleFileProvider(InputStream stream)
            {
                this.stream = stream;
            }

            @Override
            public InputStream openNext() throws IOException
            {
                if (opened) {
                    return null;
                }
                opened = true;
                return stream;
            }

            @Override
            public void close() throws IOException
            {
                if (!opened) {
                    stream.close();
                }
            }
        }
    }
}
