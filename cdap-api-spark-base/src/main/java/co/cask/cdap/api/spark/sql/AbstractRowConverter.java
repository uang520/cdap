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

import co.cask.cdap.api.common.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Abstract base class for converting object into Spark {@link Row} object.
 *
 * @param <T> type of record
 */
public abstract class AbstractRowConverter<T> {

  /**
   * Creates a {@link Row} object that represents data in the given record.
   *
   * @param record contains the record data
   * @param rowType a {@link StructType} representing the data type in the resulting {@link Row}.
   * @return a new {@link Row} instance
   */
  public Row toRow(T record, StructType rowType) {
    return (Row) toRowValue(record, rowType, "");
  }

  /**
   * Gets the field value from a field in the given record.
   *
   * @param record the record
   * @param fieldName field name in the record
   * @return the value of the field
   */
  @Nullable
  protected abstract Object getObjectFieldValue(T record, String fieldName);

  /**
   * Converts an object value to a value type acceptable by {@link Row}
   *
   * @param value the value to convert
   * @param dataType the target {@link DataType} of the value
   * @param path the current field path from the top. It is just for error message purpose.
   * @return an object that is compatible with Spark {@link Row}.
   */
  private Object toRowValue(@Nullable Object value, DataType dataType, String path) {
    if (value == null) {
      return null;
    }
    if (dataType.equals(DataTypes.NullType)) {
      return null;
    }
    if (dataType.equals(DataTypes.BooleanType)) {
      return value;
    }
    if (dataType.equals(DataTypes.ByteType)) {
      return value;
    }
    if (dataType.equals(DataTypes.ShortType)) {
      return value;
    }
    if (dataType.equals(DataTypes.IntegerType)) {
      return value;
    }
    if (dataType.equals(DataTypes.LongType)) {
      return value;
    }
    if (dataType.equals(DataTypes.FloatType)) {
      return value;
    }
    if (dataType.equals(DataTypes.DoubleType)) {
      return value;
    }
    if (dataType.equals(DataTypes.BinaryType)) {
      if (value instanceof ByteBuffer) {
        return Bytes.toBytes((ByteBuffer) value);
      }
      return value;
    }
    if (dataType.equals(DataTypes.StringType)) {
      return value;
    }
    if (dataType instanceof ArrayType) {
      @SuppressWarnings("unchecked")
      Collection<Object> collection;
      int size;
      if (value instanceof Collection) {
        collection = (Collection<Object>) value;
      } else if (value.getClass().isArray()) {
        collection = Arrays.asList((Object[]) value);
      } else {
        throw new IllegalArgumentException(
          "Value type " + value.getClass() +
            " is not supported as array type value. It must either be a Collection or an array");
      }

      List<Object> result = new ArrayList<>(collection.size());
      String elementPath = path + "[]";
      ArrayType arrayType = (ArrayType) dataType;

      for (Object obj : collection) {
        Object elementValue = toRowValue(obj, arrayType.elementType(), elementPath);
        if (elementValue == null && !arrayType.containsNull()) {
          throw new IllegalArgumentException("Null value is not allowed for array element at " + elementPath);
        }
        result.add(elementValue);
      }
      return JavaConversions.asScalaBuffer(result).toSeq();
    }
    if (dataType instanceof MapType) {
      @SuppressWarnings("unchecked")
      Map<Object, Object> map = (Map<Object, Object>) value;
      Map<Object, Object> result = new LinkedHashMap<>(map.size());
      String mapPath = path + "<>";
      MapType mapType = (MapType) dataType;

      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object mapKey = toRowValue(entry.getKey(), mapType.keyType(), mapPath);
        if (mapKey == null) {
          throw new IllegalArgumentException("Null key is not allowed for map at " + mapPath);
        }
        Object mapValue = toRowValue(entry.getValue(), mapType.valueType(), mapPath);
        if (mapValue == null && !mapType.valueContainsNull()) {
          throw new IllegalArgumentException("Null value is not allowed for map at " + mapPath);
        }
        result.put(mapKey, mapValue);
      }
      return JavaConversions.mapAsScalaMap(result);
    }
    if (dataType instanceof StructType) {
      StructField[] fields = ((StructType) dataType).fields();
      Object[] fieldValues = new Object[fields.length];
      for (int i = 0; i < fields.length; i++) {
        String fieldName = fields[i].name();
        String fieldPath = path + "/" + fieldName;
        Object fieldValue = toRowValue(getObjectFieldValue((T) value, fieldName), fields[i].dataType(), fieldPath);

        if (fieldValue == null && !fields[i].nullable()) {
          throw new IllegalArgumentException("Null value is not allowed for row field at " + fieldPath);
        }
        fieldValues[i] = fieldValue;
      }
      return RowFactory.create(fieldValues);
    }

    // Some special types in Spark SQL
    if (dataType.equals(DataTypes.TimestampType)) {
      return new Timestamp((long) value);
    }
    if (dataType.equals(DataTypes.DateType)) {
      return new Date((long) value);
    }

    // Not support the CalendarInterval type for now, as there is no equivalent in Schema
    throw new IllegalArgumentException("Unsupported data type: " + dataType.typeName());
  }
}
