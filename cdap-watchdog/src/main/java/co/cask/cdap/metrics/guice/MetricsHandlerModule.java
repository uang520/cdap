/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.metrics.guice;

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.logging.gateway.handlers.LogHandler;
import co.cask.cdap.metrics.query.MetricsHandler;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.http.HttpHandler;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

/**
 * Metrics http handlers.
 */
public class MetricsHandlerModule extends PrivateModule {
  @Override
  protected void configure() {
    bind(MetricDatasetFactory.class).to(DefaultMetricDatasetFactory.class).in(Scopes.SINGLETON);
    bind(MetricStore.class).to(DefaultMetricStore.class);

    bind(MetricsQueryService.class).in(Scopes.SINGLETON);
    expose(MetricsQueryService.class);

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class,
                                                                      Names.named(Constants.Service.METRICS));
    handlerBinder.addBinding().to(MetricsHandler.class);
    handlerBinder.addBinding().to(LogHandler.class);
    CommonHandlers.add(handlerBinder);
    bind(MetricsHandler.class);
    expose(MetricsHandler.class);
    expose(MetricStore.class);
  }

  @SuppressWarnings("unused")
  @Provides
  @Named(Constants.Metrics.TOPIC_PREFIX)
  public String providesTopicPrefix(CConfiguration cConf) {
    return cConf.get(Constants.Metrics.TOPIC_PREFIX);
  }

  @SuppressWarnings("unused")
  @Provides
  @Named(Constants.Metrics.MESSAGING_TOPIC_NUM)
  public Integer providesTopicNum(CConfiguration cConf) {
    return cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM);
  }

}
