/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.spark.function;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.StageStatisticsCollector;
import co.cask.cdap.etl.common.plugin.PipelinePluginContext;
import co.cask.cdap.etl.spark.batch.SparkBatchRuntimeContext;
import co.cask.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import co.cask.cdap.etl.spec.StageSpec;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializable collection of objects that can be used in Spark closures to instantiate plugins.
 */
public class PluginFunctionContext implements Serializable {
  private static final long serialVersionUID = -7897960584858589314L;

  private final String namespace;
  private final String pipelineName;
  private final long logicalStartTime;
  private final Map<String, String> arguments;
  private final PluginContext pluginContext;
  private final ServiceDiscoverer serviceDiscoverer;
  private final Metrics metrics;
  private final SecureStore secureStore;
  private final DataTracer dataTracer;
  private final StageSpec stageSpec;
  private final StageStatisticsCollector collector;
  private transient PipelinePluginContext pipelinePluginContext;

  public PluginFunctionContext(StageSpec stageSpec, JavaSparkExecutionContext sec, StageStatisticsCollector collector) {
    this(stageSpec, sec, new BasicArguments(sec).asMap(), sec.getLogicalStartTime(), collector);
  }

  // used in spark streaming, where each batch has a different batch time, and prepareRun is run per batch
  public PluginFunctionContext(StageSpec stageSpec, JavaSparkExecutionContext sec, Map<String, String> arguments,
                               long logicalStartTime, StageStatisticsCollector collector) {
    this.namespace = sec.getNamespace();
    this.pipelineName = sec.getApplicationSpecification().getName();
    this.stageSpec = stageSpec;
    this.logicalStartTime = logicalStartTime;
    this.arguments = new HashMap<>(arguments);
    this.pluginContext = sec.getPluginContext();
    this.serviceDiscoverer = sec.getServiceDiscoverer();
    this.metrics = sec.getMetrics();
    this.secureStore = sec.getSecureStore();
    this.dataTracer = sec.getDataTracer(stageSpec.getName());
    this.pipelinePluginContext = getPluginContext();
    this.collector = collector;
  }

  public <T> T createPlugin() throws Exception {
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(arguments, logicalStartTime, secureStore, namespace);
    return getPluginContext().newPluginInstance(stageSpec.getName(), macroEvaluator);
  }

  public String getStageName() {
    return stageSpec.getName();
  }

  public StageSpec getStageSpec() {
    return stageSpec;
  }

  public StageMetrics createStageMetrics() {
    return new DefaultStageMetrics(metrics, stageSpec.getName());
  }

  public StageStatisticsCollector getStageStatisticsCollector() {
    return collector;
  }

  public SparkBatchRuntimeContext createBatchRuntimeContext() {
    PipelineRuntime pipelineRuntime = new PipelineRuntime(namespace, pipelineName, logicalStartTime,
                                                          new BasicArguments(arguments), metrics, pluginContext,
                                                          serviceDiscoverer);
    return new SparkBatchRuntimeContext(pipelineRuntime, stageSpec);
  }

  public DataTracer getDataTracer() {
    return dataTracer;
  }

  private PipelinePluginContext getPluginContext() {
    if (pipelinePluginContext == null) {
      pipelinePluginContext = new SparkPipelinePluginContext(pluginContext, metrics,
                                                             stageSpec.isStageLoggingEnabled(),
                                                             stageSpec.isProcessTimingEnabled());
    }
    return pipelinePluginContext;
  }
}
