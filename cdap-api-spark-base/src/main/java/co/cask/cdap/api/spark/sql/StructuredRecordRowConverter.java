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
import org.apache.spark.sql.types.StructType;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link AbstractRowConverter} to that uses {@link StructuredRecord} as record type.
 */
public final class StructuredRecordRowConverter extends AbstractRowConverter<StructuredRecord> {

  @Nullable
  @Override
  protected Object getObjectFieldValue(StructuredRecord record, String fieldName) {
    return record.get(fieldName);
  }

  public StructuredRecord fromRow(Row row, StructType schema) {
    Schema recordSchema = DataFrames.toSchema(schema);
    return (StructuredRecord) fromRowValue(row, recordSchema);
  }

  /**
   * Converts a value from Spark {@link Row} into value acceptable for {@link StructuredRecord}.
   *
   * @param value the value to convert from
   * @param schema the target {@link Schema} of the value
   * @return a value object acceptable to be used in {@link StructuredRecord}.
   */
  private static Object fromRowValue(Object value, Schema schema) {
    switch (schema.getType()) {
      // For all simple types, return as is.
      case NULL:
        return null;
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
        return value;
      case BYTES:
        return ByteBuffer.wrap((byte[]) value);
      case ARRAY: {
        // Value must be a collection
        @SuppressWarnings("unchecked")
        Collection<Object> collection = (Collection<Object>) value;
        List<Object> result = new ArrayList<>(collection.size());
        for (Object element : collection) {
          result.add(fromRowValue(element, schema.getComponentSchema()));
        }
        return result;
      }
      case MAP: {
        // Value must be a Map
        Map<?, ?> map = (Map<?, ?>) value;
        Map<Object, Object> result = new LinkedHashMap<>(map.size());
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          result.put(fromRowValue(entry.getKey(), mapSchema.getKey()),
                     fromRowValue(entry.getValue(), mapSchema.getValue()));
        }
        return result;
      }
      case RECORD: {
        // Value must be a Row
        Row row = (Row) value;
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        int idx = 0;
        for (Schema.Field field : schema.getFields()) {
          Schema fieldSchema = field.getSchema();

          if (row.isNullAt(idx) && !fieldSchema.isNullable()) {
            throw new NullPointerException("Null value is not allowed in record field "
                                             + schema.getRecordName() + "." + field.getName());
          }

          fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;

          // If the value is null for the field, just continue without setting anything to the StructuredRecord
          if (row.isNullAt(idx)) {
            idx++;
            continue;
          }

          // Special case handling for ARRAY and MAP in order to get the Java type
          if (fieldSchema.getType() == Schema.Type.ARRAY) {
            builder.set(field.getName(), fromRowValue(row.getList(idx), fieldSchema));
          } else if (fieldSchema.getType() == Schema.Type.MAP) {
            builder.set(field.getName(), fromRowValue(row.getJavaMap(idx), fieldSchema));
          } else {
            Object fieldValue = row.get(idx);

            // Date and timestamp special return type handling
            if (fieldValue instanceof Date) {
              fieldValue = ((Date) fieldValue).getTime();
            } else if (fieldValue instanceof Timestamp) {
              fieldValue = ((Timestamp) fieldValue).getTime();
            }
            builder.set(field.getName(), fromRowValue(fieldValue, fieldSchema));
          }

          idx++;
        }
        return builder.build();
      }
    }

    throw new IllegalArgumentException("Unsupported schema: " + schema);
  }
}
