package org.embulk.input.http;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.http.HttpRequestBuilder.GetHttpRequestBuilder;
import org.embulk.input.http.HttpRequestBuilder.PostHttpRequestBuilder;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.file.InputStreamFileInput;
import org.embulk.util.retryhelper.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class HttpFileInputPlugin implements FileInputPlugin {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpFileInputPlugin.class);

  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  private static final Map<HttpMethod, HttpRequestBuilder> HTTP_REQUEST_BUILDERS =
      Collections.unmodifiableMap(
          new HashMap<HttpMethod, HttpRequestBuilder>() {
            {
              put(HttpMethod.GET, new GetHttpRequestBuilder());
              put(HttpMethod.POST, new PostHttpRequestBuilder());
            }
          });

  @Override
  public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control) {
    final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
    final PluginTask task = configMapper.map(config, PluginTask.class);

    final List<List<QueryOption.Query>> queries;
    if (task.getParams().isPresent()) {
      queries = task.getParams().get().generateQueries(task.getPager().orElse(null));
    } else if (task.getPager().isPresent()) {
      queries = task.getPager().get().expand();
    } else {
      queries = Collections.emptyList();
    }
    task.setQueries(queries);
    task.setHttpMethod(HttpMethod.valueOf(task.getMethod().toUpperCase()));

    return resume(task.toTaskSource(), queries.isEmpty() ? 1 : queries.size(), control);
  }

  @Override
  public ConfigDiff resume(TaskSource taskSource, int taskCount, FileInputPlugin.Control control) {
    control.run(taskSource, taskCount);
    return CONFIG_MAPPER_FACTORY.newConfigDiff();
  }

  @Override
  public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports) {}

  @Override
  public TransactionalFileInput open(TaskSource taskSource, int taskIndex) {
    final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
    PluginTask task = taskMapper.map(taskSource, PluginTask.class);

    HttpRequestBase request;
    try {
      request = httpRequestFrom(task, taskIndex);
    } catch (URISyntaxException | UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

    HttpClientBuilder builder =
        HttpClientBuilder.create()
            .disableAutomaticRetries()
            .setDefaultRequestConfig(requestConfigFrom(task))
            .setDefaultHeaders(requestHeadersFrom(task));
    if (task.getBasicAuth().isPresent()) {
      builder.setDefaultCredentialsProvider(
          makeCredentialsProvider(task.getBasicAuth().get(), request));
    }

    LOGGER.info(
        format(
            Locale.ENGLISH,
            "%s \"%s\"",
            task.getMethod().toUpperCase(),
            request.getURI().toString()));

    long startTimeMills = System.currentTimeMillis();
    try {
      InputStream stream =
          retryExecutorFrom(task)
              .runInterruptible(new RetryableHandler(builder.build(), request))
              .getEntity()
              .getContent();
      if (!task.getInputDirect()) {
        stream = copyToFile(stream);
      }
      return new PluginFileInput(task, stream, startTimeMills);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private InputStream copyToFile(InputStream input) throws IOException {
    File tmpfile = Files.createTempFile("embulk-input-http.", ".tmp").toFile();
    tmpfile.deleteOnExit();

    try (FileOutputStream output = new FileOutputStream(tmpfile)) {
      LOGGER.info(format(Locale.ENGLISH, "Writing response to %s", tmpfile));
      IOUtils.copy(input, output);
    } finally {
      input.close();
    }

    return new FileInputStream(tmpfile);
  }

  private CredentialsProvider makeCredentialsProvider(
      BasicAuthOption basicAuth, HttpRequestBase request) {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    final AuthScope authScope =
        new AuthScope(request.getURI().getHost(), request.getURI().getPort());
    credentialsProvider.setCredentials(
        authScope, new UsernamePasswordCredentials(basicAuth.getUser(), basicAuth.getPassword()));
    return credentialsProvider;
  }

  private static HttpRequestBase httpRequestFrom(PluginTask task, int taskIndex)
      throws URISyntaxException, UnsupportedEncodingException {
    HttpRequestBuilder builder =
        Optional.ofNullable(HTTP_REQUEST_BUILDERS.get(task.getHttpMethod()))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Unsupported http method %s", task.getMethod())));

    return builder.build(
        task, (task.getQueries().isEmpty()) ? null : task.getQueries().get(taskIndex));
  }

  private static List<Header> requestHeadersFrom(PluginTask task) {
    Map<String, String> map =
        new HashMap<String, String>() {
          {
            put("Accept", "*/*");
            put("Accept-Charset", task.getCharset());
            put("Accept-Encoding", "gzip, deflate");
            put("Accept-Language", "en-us,en;q=0.5");
            put("User-Agent", task.getUserAgent());
          }
        };

    // Overwrite default headers by user defined headers
    task.getRequestHeaders().forEach(map::put);

    return Collections.unmodifiableList(
        map.entrySet().stream()
            .map(e -> new BasicHeader(e.getKey(), e.getValue()))
            .collect(Collectors.toList()));
  }

  private static RequestConfig requestConfigFrom(PluginTask task) {
    return RequestConfig.custom()
        .setCircularRedirectsAllowed(true)
        .setMaxRedirects(10)
        .setRedirectsEnabled(true)
        .setConnectTimeout(task.getOpenTimeout())
        .setSocketTimeout(task.getReadTimeout())
        .build();
  }

  private static RetryExecutor retryExecutorFrom(PluginTask task) {
    return RetryExecutor.builder()
        .withRetryLimit(task.getMaxRetries())
        .withInitialRetryWaitMillis(task.getRetryInterval())
        .withMaxRetryWaitMillis(30 * 60 * 1000) // TODO be configurable
        .build();
  }

  public enum HttpMethod {
    POST,
    GET
  }

  public interface PluginTask extends Task {

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

    @Config("interval_includes_response_time")
    @ConfigDefault("null")
    boolean getIntervalIncludesResponseTime();

    @Config("input_direct")
    @ConfigDefault("true")
    boolean getInputDirect();

    @Config("params")
    @ConfigDefault("null")
    Optional<ParamsOption> getParams();

    @Config("request_body")
    @ConfigDefault("null")
    Optional<String> getRequestBody();

    @Config("basic_auth")
    @ConfigDefault("null")
    Optional<BasicAuthOption> getBasicAuth();

    @Config("pager")
    @ConfigDefault("null")
    Optional<PagerOption> getPager();

    @Config("request_headers")
    @ConfigDefault("{}")
    Map<String, String> getRequestHeaders();

    List<List<QueryOption.Query>> getQueries();

    void setQueries(List<List<QueryOption.Query>> queries);

    HttpMethod getHttpMethod();

    void setHttpMethod(HttpMethod httpMethod);
  }

  public static class PluginFileInput extends InputStreamFileInput
      implements TransactionalFileInput {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpFileInputPlugin.class);

    private final long startTimeMills;

    private final PluginTask task;

    public PluginFileInput(PluginTask task, InputStream stream, long startTimeMills) {
      super(Exec.getBufferAllocator(), new SingleFileProvider(stream));
      this.startTimeMills = startTimeMills;
      this.task = task;
    }

    public TaskReport commit() {
      return CONFIG_MAPPER_FACTORY.newTaskReport();
    }

    @Override
    public void close() {
      super.close();
      handleInterval();
    }

    @Override
    public void abort() {}

    protected void handleInterval() {
      if (task.getRequestInterval() <= 0) {
        return;
      }
      long interval = task.getRequestInterval();
      if (task.getIntervalIncludesResponseTime()) {
        interval = interval - (System.currentTimeMillis() - startTimeMills);
      }
      if (interval > 0) {
        LOGGER.info(String.format("waiting %d msec ...", interval));
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    private static class SingleFileProvider implements InputStreamFileInput.Provider {

      private final InputStream stream;

      private boolean opened = false;

      public SingleFileProvider(InputStream stream) {
        this.stream = stream;
      }

      @Override
      public InputStream openNext() {
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
  }
}
