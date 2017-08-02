/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.sql.datasources.dataset

import java.util

import co.cask.cdap.api.data.DatasetInstantiationException
import co.cask.cdap.api.data.batch.{RecordScannable, Split, Splits}
import co.cask.cdap.api.data.format.StructuredRecord
import co.cask.cdap.api.data.schema.UnsupportedTypeException
import co.cask.cdap.api.spark.sql.DataFrames
import co.cask.cdap.app.runtime.spark.SparkTransactional.TransactionType
import co.cask.cdap.app.runtime.spark.data.RecordScannableRDD
import co.cask.cdap.app.runtime.spark.sql.ObjectRowConverter
import co.cask.cdap.app.runtime.spark.{SparkClassLoader, SparkTxRunnable}
import co.cask.cdap.data.LineageDatasetContext
import co.cask.cdap.data2.transaction.Transactions
import co.cask.cdap.proto.id.DatasetId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.tephra.TransactionFailureException

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  *
  */
private[dataset] class RecordScannableRelation(override val sqlContext: SQLContext,
                                               override val schema: StructType,
                                               datasetId: DatasetId,
                                               parameters: Map[String, String])
  extends BaseRelation with Serializable with PrunedFilteredScan {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val sc = sqlContext.sparkContext

    // Create a target schema based on the query columns
    val rowSchema = StructType(requiredColumns.map(col => schema.fields(schema.fieldIndex(col))))

    val sparkClassLoader = SparkClassLoader.findFromContext()
    val sec = sparkClassLoader.getSparkExecutionContext(false)

    // User may provide a custom set of splits from the query parameters
    val inputSplits = parameters.get("input.splits")
      .map(Splits.decode(_, new util.ArrayList[Split](), sparkClassLoader))

    // Creates the RDD[Row] based on the RecordScannable
    // For RDD to compute partitions, a transaction is needed in order to gain access to dataset instance.
    // It should either be using the active transaction (explicit transaction), or create a new transaction
    // but leave it open so that it will be used for all stages in same job execution and get committed when
    // the job ended.
    var result: RDD[Row] = null
    try {
      sec.getSparkTransactional().execute(new SparkTxRunnable {
        override def run(context: LineageDatasetContext): Unit = {
          val dataset = context.getDataset(datasetId.getNamespace, datasetId.getDataset, parameters)
          result = dataset.asInstanceOf[RecordScannable[_]].getRecordType match {
            case recordType if classOf[StructuredRecord] == recordType => {
              val recordScannable = dataset.asInstanceOf[RecordScannable[StructuredRecord]]
              new RecordScannableRDD[StructuredRecord](sc, datasetId.getNamespace, datasetId.getDataset, parameters,
                                                       inputSplits.getOrElse(recordScannable.getSplits),
                                                       sec.getDriveHttpServiceBaseURI(sc))
                .map(DataFrames.toRow(_, rowSchema))
            }
            case beanType: Class[_] => {
              val recordScannable = dataset.asInstanceOf[RecordScannable[_]]
              new RecordScannableRDD(sc, datasetId.getNamespace, datasetId.getDataset, parameters,
                                     inputSplits.getOrElse(recordScannable.getSplits),
                                     sec.getDriveHttpServiceBaseURI(sc))(ClassTag(beanType))
                .asInstanceOf[RDD[AnyRef]]
                .mapPartitions(it => new Iterator[Row]() {
                  val converter = new ObjectRowConverter

                  override def hasNext: Boolean = it.hasNext

                  override def next(): Row = converter.toRow(it.next(), rowSchema)
                })
            }
            case anyType =>
              throw new UnsupportedTypeException(s"Dataset $datasetId has record type $anyType is not supported")
          }
        }
      }, TransactionType.IMPLICIT_COMMIT_ON_JOB_END)

      result
    } catch {
      case t: TransactionFailureException => throw Transactions.propagate(t, classOf[UnsupportedTypeException],
                                                                          classOf[DatasetInstantiationException])
    }
  }
}
