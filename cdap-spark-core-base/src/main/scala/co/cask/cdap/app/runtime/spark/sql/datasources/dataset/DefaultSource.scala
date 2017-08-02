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

import java.lang.reflect.Type
import javax.annotation.Nullable

import co.cask.cdap.api.data.batch.RecordScannable
import co.cask.cdap.api.data.format.StructuredRecord
import co.cask.cdap.api.data.schema.{Schema, UnsupportedTypeException}
import co.cask.cdap.api.dataset.{Dataset, DatasetProperties, DatasetSpecification}
import co.cask.cdap.api.spark.sql.DataFrames
import co.cask.cdap.app.runtime.spark.{SparkClassLoader, SparkRuntimeContext, SparkRuntimeContextProvider}
import co.cask.cdap.data2.metadata.lineage.AccessType
import co.cask.cdap.internal.io.ReflectionSchemaGenerator
import co.cask.cdap.proto.id.{DatasetId, NamespaceId}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.JavaConversions._

/**
  * The Spark data source for Dataset.
  */
class DefaultSource extends RelationProvider
                    with SchemaRelationProvider with CreatableRelationProvider with DataSourceRegister {

  override def shortName(): String = {
    "cdap.dataset"
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    return createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              @Nullable userSchema: StructType): BaseRelation = {
    val runtimeContext = SparkRuntimeContextProvider.get()
    val datasetId = getDatasetId(parameters, runtimeContext.getProgramRunId.getNamespaceId)
    val datasetSpec = runtimeContext.getDatasetFramework.getDatasetSpec(datasetId)

    // Determine the dataset schema from specification or from the RecordScannable interface
    val schema = getSchema(datasetId, datasetSpec, parameters, Option(userSchema), runtimeContext,
                           (dataset: Dataset) => dataset.asInstanceOf[RecordScannable[_]].getRecordType)

    // Should be able to load the type through the SparkClassLoader
    val sparkClassLoader = SparkClassLoader.findFromContext()
    sparkClassLoader.loadClass(datasetSpec.getType) match {
      // RecordScannable Dataset
      case cls if classOf[RecordScannable[_]].isAssignableFrom(cls) =>
        new RecordScannableRelation(sqlContext, schema, datasetId, parameters)

//      case cls if classOf[FileSet].isAssignableFrom(cls) =>

      // TODO: Handling of FileSet and PartitionedFileSet
      case _ => throw new IllegalArgumentException("Unsupport type " + datasetSpec.getType)
    }
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
//    val runtimeContext = SparkRuntimeContextProvider.get()
//    val datasetId = getDatasetId(parameters, runtimeContext.getProgramRunId.getNamespaceId)
//    val datasetSpec = runtimeContext.getDatasetFramework.getDatasetSpec(datasetId)
//    val schema = getSchema(datasetId, datasetSpec, parameters, runtimeContext)
//
//    // The dataframe should have the same schema as needed by the dataset
//    require(schema == data.schema,
//            s"DataFrame schema doesn't match the schema of dataset $datasetId. " +
//              s"Dataset schema is $schema and dataframe schema is ${data.schema}")
//
//
    null
  }

  /**
    * Creates a [[co.cask.cdap.proto.id.DatasetId]] from the parameters.
    *
    * @param parameters the parameters provided to the query. The dataset name is from the `path` parameter
    * @param namespaceId the namespace of the current program execution
    * @return the `DatasetId` of the query
    */
  private def getDatasetId(parameters: Map[String, String], namespaceId: NamespaceId): DatasetId = {
    parameters.get("path") match {
      case Some(datasetName) =>
        parameters.get("namespace").map(new NamespaceId(_)).getOrElse(namespaceId).dataset(datasetName)
      case _ =>
        throw new IllegalArgumentException("Missing dataset name, which is derived from the 'path' parameter")
    }
  }

  /**
    * Gets the schema based on the dataset properties.
    *
    * @param datasetId the dataset id
    * @param datasetSpec the dataset specification of the dataset instance
    * @param parameters the query parameters provided by user
    * @param runtimeContext the Spark runtime context
    * @param recordTypeFunc a function to determine the record type from a given dataset
    */
  private def getSchema(datasetId: DatasetId,
                        datasetSpec: DatasetSpecification,
                        parameters: Map[String, String],
                        userSchema: Option[StructType],
                        runtimeContext: SparkRuntimeContext,
                        recordTypeFunc: (Dataset) => Type) : StructType = {
    // Try to see if there is schema from the dataset properties
    val schema = Option(datasetSpec.getProperty(DatasetProperties.SCHEMA))
      // If there is one, parse it
      .map(Schema.parseJson(_))
      // If no schema property, try to derive it from the record type exposed by the dataset
      .orElse({
        val dataset = runtimeContext.getDataset(datasetId.getNamespace, datasetId.getDataset,
                                                parameters, AccessType.UNKNOWN)
        try {
          recordTypeFunc(dataset) match {
            // The recordType shouldn't be StructuredRecord, otherwise the SCHEMA property should exists
            case recordType if classOf[StructuredRecord] == recordType => None
            // For non-StructuredRecord type, generate the schema from the type
            case recordType => Some(new ReflectionSchemaGenerator().generate(recordType, false))
          }
        } finally {
          runtimeContext.releaseDataset(dataset)
        }
      })
      // Convert it to Spark SQL DataType
      .map(DataFrames.toDataType[DataType](_))
      // Default to user provided schema if there no schema can be derived from the dataset
      .orElse(userSchema)
      // Get the DataType or throw exception if missing
      .getOrElse(throw new IllegalArgumentException(
        s"The dataset $datasetId does not have schema and no schema is provided via the SQL query. " +
          s"The dataset must either has the '${DatasetProperties.SCHEMA}' property " +
          s"or implements RecordScannable/RecordWritable interfaces"))

    schema match {
      case s: StructType => s
      case t => throw new UnsupportedTypeException(
        s"Dataset $datasetId with schema of type ${t} is not supported. It must be of StructType.")
    }
  }
}
