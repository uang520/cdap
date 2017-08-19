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

package co.cask.cdap.etl.spark.streaming.function;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.NoopStageStatisticsCollector;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.StageStatisticsCollector;
import co.cask.cdap.etl.spark.Compat;
import co.cask.cdap.etl.spark.SparkPipelineRuntime;
import co.cask.cdap.etl.spark.batch.SparkBatchSinkContext;
import co.cask.cdap.etl.spark.batch.SparkBatchSinkFactory;
import co.cask.cdap.etl.spark.function.BatchSinkFunction;
import co.cask.cdap.etl.spark.function.PairFlatMapFunc;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import co.cask.cdap.etl.spec.StageSpec;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function used to write a batch of data to a batch sink for use with a JavaDStream.
 * note: not using foreachRDD(VoidFunction2) method, because spark 1.3 doesn't have VoidFunction2.
 *
 * @param <T> type of object in the rdd
 */
public class StreamingBatchSinkFunction<T> implements Function2<JavaRDD<T>, Time, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingBatchSinkFunction.class);
  private final JavaSparkExecutionContext sec;
  private final StageSpec stageSpec;

  public StreamingBatchSinkFunction(PairFlatMapFunction<T, Object, Object> sinkFunction,
                                    JavaSparkExecutionContext sec, StageSpec stageSpec) {
    this.sec = sec;
    this.stageSpec = stageSpec;
  }

  @Override
  public Void call(JavaRDD<T> data, Time batchTime) throws Exception {
    if (data.isEmpty()) {
      return null;
    }

    final long logicalStartTime = batchTime.milliseconds();
    MacroEvaluator evaluator = new DefaultMacroEvaluator(sec.getWorkflowToken(),
                                                         sec.getRuntimeArguments(),
                                                         logicalStartTime,
                                                         sec.getSecureStore(),
                                                         sec.getNamespace());
    PluginContext pluginContext = new SparkPipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                 stageSpec.isStageLoggingEnabled(),
                                                                 stageSpec.isProcessTimingEnabled());
    final SparkBatchSinkFactory sinkFactory = new SparkBatchSinkFactory();
    final String stageName = stageSpec.getName();
    final BatchSink<Object, Object, Object> batchSink = pluginContext.newPluginInstance(stageName, evaluator);
    final PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec, logicalStartTime);
    boolean isPrepared = false;
    boolean isDone = false;

    try {
      sec.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          SparkBatchSinkContext sinkContext =
            new SparkBatchSinkContext(sinkFactory, sec, datasetContext, pipelineRuntime, stageSpec);
          batchSink.prepareRun(sinkContext);
        }
      });
      isPrepared = true;

      PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec,
                                                                              pipelineRuntime.getArguments().asMap(),
                                                                              batchTime.milliseconds(),
                                                                              new NoopStageStatisticsCollector());
      PairFlatMapFunc<T, Object, Object> sinkFunction = new BatchSinkFunction<T, Object, Object>(pluginFunctionContext);

      sinkFactory.writeFromRDD(data.flatMapToPair(Compat.convert(sinkFunction)), sec, stageName,
                               Object.class, Object.class);
      isDone = true;
      sec.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          SparkBatchSinkContext sinkContext =
            new SparkBatchSinkContext(sinkFactory, sec, datasetContext, pipelineRuntime, stageSpec);
          batchSink.onRunFinish(true, sinkContext);
        }
      });
    } catch (Exception e) {
      LOG.error("Error writing to sink {} for the batch for time {}.", stageName, logicalStartTime, e);
    } finally {
      if (isPrepared && !isDone) {
        sec.execute(new TxRunnable() {
          @Override
          public void run(DatasetContext datasetContext) throws Exception {
            SparkBatchSinkContext sinkContext =
              new SparkBatchSinkContext(sinkFactory, sec, datasetContext, pipelineRuntime, stageSpec);
            batchSink.onRunFinish(false, sinkContext);
          }
        });
      }
    }
    return null;
  }
}
