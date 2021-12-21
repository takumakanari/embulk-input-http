package org.embulk.input.http;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

interface HttpRequestBuilder {

  HttpRequestBase build(HttpFileInputPlugin.PluginTask task, List<QueryOption.Query> queries)
      throws URISyntaxException, UnsupportedEncodingException;

  class GetHttpRequestBuilder implements HttpRequestBuilder {

    @Override
    public HttpRequestBase build(
        HttpFileInputPlugin.PluginTask task, List<QueryOption.Query> queries)
        throws URISyntaxException {
      HttpGet request = new HttpGet(task.getUrl());
      if (queries == null) {
        return request;
      }

      URIBuilder builder = new URIBuilder(request.getURI());
      for (QueryOption.Query q : queries) {
        for (String v : q.getValues()) {
          builder.addParameter(q.getName(), v);
        }
      }
      request.setURI(builder.build());
      return request;
    }
  }

  class PostHttpRequestBuilder implements HttpRequestBuilder {

    @Override
    public HttpRequestBase build(
        HttpFileInputPlugin.PluginTask task, List<QueryOption.Query> queries)
        throws UnsupportedEncodingException {
      HttpPost request = new HttpPost(task.getUrl());
      if (queries != null) {
        List<NameValuePair> pairs = new ArrayList<>();
        for (QueryOption.Query q : queries) {
          for (String v : q.getValues()) {
            pairs.add(new BasicNameValuePair(q.getName(), v));
          }
        }
        request.setEntity(new UrlEncodedFormEntity(pairs));
      } else if (task.getRequestBody().isPresent()) {
        request.setEntity(new StringEntity(task.getRequestBody().get()));
      }
      return request;
    }
  }
}
