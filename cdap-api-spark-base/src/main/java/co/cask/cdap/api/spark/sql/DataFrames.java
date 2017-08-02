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

package co.cask.cdap.api.spark.sql;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.runtime.AbstractFunction1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class for conversions between {@link DataType} and {@link Schema}.
 */
public final class DataFrames {

  // A default union selector that will return the non-nullable part of it if the schema is nullable.
  // Otherwise exception will be thrown.
  private static final Function1<Schema, DataType> DEFAULT_UNION_SELECTOR = new AbstractFunction1<Schema, DataType>() {
    @Override
    public DataType apply(Schema schema) {
      if (schema.isNullable()) {
        return schemaToDataType(schema.getNonNullable(), this);
      }
      throw new IllegalArgumentException("Union schema is not support: " + schema);
    }
  };

  /**
   * Converts a {@link Schema} to Spark {@link DataType}.
   *
   * @param schema the schema to convert
   * @return The corresponding {@link DataType}
   */
  public static <T extends DataType> T toDataType(Schema schema) {
    return toDataType(schema, DEFAULT_UNION_SELECTOR);
  }

  /**
   * Converts a {@link Schema} to Spark {@link DataType}.
   *
   * @param schema the schema to convert
   * @param unionSelector the function to pick which schema to use when a {@link Schema.Type#UNION} type schema
   *                      is encountered
   * @return The corresponding {@link DataType}
   */
  public static <T extends DataType> T toDataType(Schema schema, Function1<Schema, DataType> unionSelector) {
    return (T) schemaToDataType(schema, unionSelector);
  }

  /**
   * Converts a Spark {@link DataType} to a {@link Schema} object.
   *
   * @param dataType the data type to convert from
   * @return The corresponding {@link Schema}
   */
  public static Schema toSchema(DataType dataType) {
    return dataTypeToSchema(dataType, new int[] { 0 });
  }

  /**
   * Creates a {@link Row} object that represents data in the given {@link StructuredRecord}.
   *
   * @param record contains the record data
   * @param structType a {@link StructType} representing the data type in the resulting {@link Row}.
   * @return a new {@link Row} instance
   */
  public static Row toRow(StructuredRecord record, StructType structType) {
    return new StructuredRecordRowConverter().toRow(record, structType);
  }

  /**
   * Actual method to convert {@link Schema} to Spark {@link DataType}. It is separated out for the generic casting.
   *
   * @param schema the schema to convert
   * @param unionSelector the function to pick which schema to use when a {@link Schema.Type#UNION} type schema
   *                      is encountered
   * @return The corresponding {@link DataType}
   */
  private static DataType schemaToDataType(Schema schema, Function1<Schema, DataType> unionSelector) {
    switch (schema.getType()) {
      case NULL:
        return DataTypes.NullType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case BYTES:
        return DataTypes.BinaryType;
      case STRING:
        return DataTypes.StringType;
      case ENUM:
        return DataTypes.StringType;
      case ARRAY:
        Schema componentSchema = schema.getComponentSchema();
        return DataTypes.createArrayType(schemaToDataType(componentSchema, unionSelector),
                                         componentSchema.isNullable());
      case MAP:
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        return DataTypes.createMapType(schemaToDataType(mapSchema.getKey(), unionSelector),
                                       schemaToDataType(mapSchema.getValue(), unionSelector),
                                       mapSchema.getValue().isNullable());
      case RECORD:
        List<StructField> structFields = new ArrayList<>(schema.getFields().size());
        for (Schema.Field field : schema.getFields()) {
          Schema fieldSchema = field.getSchema();
          DataType fieldType = schemaToDataType(fieldSchema, unionSelector);
          structFields.add(DataTypes.createStructField(field.getName(), fieldType, fieldSchema.isNullable()));
        }
        return DataTypes.createStructType(structFields);
      case UNION:
        return unionSelector.apply(schema);
    }
    // Should not happen
    throw new IllegalArgumentException("Unsupported schema: " + schema);
  }


  /**
   * Converts a Spark {@link DataType} to a {@link Schema} object.
   *
   * @param dataType the data type to convert from
   * @param recordCounter tracks number of record schema becoming created; used for record name generation only
   * @return a new {@link Schema}.
   */
  private static Schema dataTypeToSchema(DataType dataType, int[] recordCounter) {
    if (dataType.equals(DataTypes.NullType)) {
      return Schema.of(Schema.Type.NULL);
    }
    if (dataType.equals(DataTypes.BooleanType)) {
      return Schema.of(Schema.Type.BOOLEAN);
    }
    if (dataType.equals(DataTypes.ByteType)) {
      return Schema.of(Schema.Type.INT);
    }
    if (dataType.equals(DataTypes.ShortType)) {
      return Schema.of(Schema.Type.INT);
    }
    if (dataType.equals(DataTypes.IntegerType)) {
      return Schema.of(Schema.Type.INT);
    }
    if (dataType.equals(DataTypes.LongType)) {
      return Schema.of(Schema.Type.LONG);
    }
    if (dataType.equals(DataTypes.FloatType)) {
      return Schema.of(Schema.Type.FLOAT);
    }
    if (dataType.equals(DataTypes.DoubleType)) {
      return Schema.of(Schema.Type.DOUBLE);
    }
    if (dataType.equals(DataTypes.BinaryType)) {
      return Schema.of(Schema.Type.BYTES);
    }
    if (dataType.equals(DataTypes.StringType)) {
      return Schema.of(Schema.Type.STRING);
    }
    if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;

      // Special case for byte array
      if (arrayType.elementType() == DataTypes.ByteType) {
        return Schema.of(Schema.Type.BYTES);
      }

      Schema componentSchema = dataTypeToSchema(arrayType.elementType(), recordCounter);
      return Schema.arrayOf(arrayType.containsNull() ? Schema.nullableOf(componentSchema) : componentSchema);
    }
    if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      Schema valueSchema = dataTypeToSchema(mapType.valueType(), recordCounter);
      return Schema.mapOf(dataTypeToSchema(mapType.keyType(), recordCounter),
                          mapType.valueContainsNull() ? Schema.nullableOf(valueSchema) : valueSchema);
    }
    if (dataType instanceof StructType) {
      List<Schema.Field> fields = new ArrayList<>();
      for (StructField structField : ((StructType) dataType).fields()) {
        Schema fieldSchema = dataTypeToSchema(structField.dataType(), recordCounter);
        fields.add(Schema.Field.of(structField.name(),
                                   structField.nullable() ? Schema.nullableOf(fieldSchema) : fieldSchema));
      }
      return Schema.recordOf("Record" + recordCounter[0]++, fields);
    }

    // Some special types in Spark SQL
    if (dataType.equals(DataTypes.TimestampType)) {
      return Schema.of(Schema.Type.LONG);
    }
    if (dataType.equals(DataTypes.DateType)) {
      return Schema.of(Schema.Type.LONG);
    }

    // Not support the CalendarInterval type for now, as there is no equivalent in Schema
    throw new IllegalArgumentException("Unsupported data type: " + dataType.typeName());
  }

  private DataFrames() {
  }
}
