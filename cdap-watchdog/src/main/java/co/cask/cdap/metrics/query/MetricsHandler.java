/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.metrics.query;

import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.metrics.process.MetricsConsumerMetaTable;
import co.cask.cdap.metrics.process.MetricsMetaKey;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.MetricQueryRequest;
import co.cask.cdap.proto.MetricTagValue;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Search metrics handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/metrics")
public class MetricsHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsHandler.class);
  private static final Gson GSON = new Gson();
  private static final Map<String, String> METRICS_PROCESSOR_TAGS_MAP =
    ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
                    Constants.Metrics.Tag.COMPONENT, Constants.Service.METRICS_PROCESSOR);
  private static final List<MetricTagValue> METRICS_PROCESSOR_TAGS_LIST =
    ImmutableList.of(new MetricTagValue(Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace()),
                     new MetricTagValue(Constants.Metrics.Tag.COMPONENT, Constants.Service.METRICS_PROCESSOR));

  private final MetricsQueryHelper metricsQueryHelper;
  private final MetricDatasetFactory metricDatasetFactory;
  private final List<TopicId> metricsTopics;

  private MetricsConsumerMetaTable metaTable;

  @Inject
  public MetricsHandler(MetricsQueryHelper metricsQueryHelper, MetricDatasetFactory metricDatasetFactory,
                        @Named(Constants.Metrics.TOPIC_PREFIX) String topicPrefix,
                        @Named(Constants.Metrics.MESSAGING_TOPIC_NUM) Integer topicNumbers) {
    this.metricsQueryHelper = metricsQueryHelper;
    this.metricDatasetFactory = metricDatasetFactory;
    this.metricsTopics = new ArrayList<>();
    for (int i = 0; i < topicNumbers; i++) {
      this.metricsTopics.add(NamespaceId.SYSTEM.topic(topicPrefix + i));
    }
  }

  @POST
  @Path("/search")
  public void search(HttpRequest request, HttpResponder responder,
                     @QueryParam("target") String target,
                     @QueryParam("tag") List<String> tags) throws Exception {
    if (target == null) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Required target param is missing");
      return;
    }
    try {
      switch (target) {
        case "tag":
          responder.sendJson(HttpResponseStatus.OK, metricsQueryHelper.searchTags(tags));
          break;
        case "metric":
          responder.sendJson(HttpResponseStatus.OK, metricsQueryHelper.searchMetric(tags));
          break;
        default:
          responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Unknown target param value: " + target);
          break;
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
  }

  @POST
  @Path("/query")
  public void query(HttpRequest request, HttpResponder responder,
                    @QueryParam("metric") List<String> metrics,
                    @QueryParam("groupBy") List<String> groupBy,
                    @QueryParam("tag") List<String> tags) throws Exception {
    try {
      if (new QueryStringDecoder(request.getUri()).getParameters().isEmpty()) {
        if (HttpHeaders.getContentLength(request) > 0) {
          Map<String, MetricsQueryHelper.QueryRequestFormat> queries =
            GSON.fromJson(request.getContent().toString(Charsets.UTF_8),
                          new TypeToken<Map<String, MetricsQueryHelper.QueryRequestFormat>>() { }.getType());
          responder.sendJson(HttpResponseStatus.OK, metricsQueryHelper.executeBatchQueries(queries));
          return;
        }
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Batch request with empty content");
      }
      responder.sendJson(HttpResponseStatus.OK,
                         metricsQueryHelper.executeTagQuery(tags, metrics, groupBy,
                                                            new QueryStringDecoder(request.getUri()).getParameters()));
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
  }

  @GET
  @Path("/processor/status")
  public void processorStatus(HttpRequest request, HttpResponder responder) throws Exception {
    if (metaTable == null) {
      metaTable = metricDatasetFactory.createConsumerMeta();
    }
    Map<TopicId, Long> processMap = new HashMap<>();
    for (TopicId topicId : metricsTopics) {
      processMap.put(topicId, metaTable.getProcessTotal(new TopicIdMetaKey(topicId)));
    }
    responder.sendJson(HttpResponseStatus.OK, processMap);
  }

  private final class TopicIdMetaKey implements MetricsMetaKey {

    private final TopicId topicId;
    private final byte[] key;

    TopicIdMetaKey(TopicId topicId) {
      this.topicId = topicId;
      this.key = MessagingUtils.toMetadataRowKey(topicId);
    }

    @Override
    public byte[] getKey() {
      return key;
    }

    TopicId getTopicId() {
      return topicId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TopicIdMetaKey that = (TopicIdMetaKey) o;
      // Comparing the key is enough because key and topicId have one-to-one relationship
      return Arrays.equals(getKey(), that.getKey());
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(getKey());
    }
  }
}
