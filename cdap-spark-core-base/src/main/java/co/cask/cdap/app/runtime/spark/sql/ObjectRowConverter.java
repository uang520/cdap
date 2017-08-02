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

package co.cask.cdap.app.runtime.spark.sql;

import co.cask.cdap.api.spark.sql.AbstractRowConverter;
import co.cask.cdap.internal.io.ASMFieldAccessorFactory;
import co.cask.cdap.internal.io.FieldAccessorFactory;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;

import javax.annotation.Nullable;

/**
 * A {@link AbstractRowConverter} that converts java object into row, based on object field name.
 */
public final class ObjectRowConverter extends AbstractRowConverter<Object> {

  private final FieldAccessorFactory fieldAccessorFactory = new ASMFieldAccessorFactory();

  @Nullable
  @Override
  protected Object getObjectFieldValue(Object record, String fieldName) {
    try {
      return fieldAccessorFactory.getFieldAccessor(TypeToken.of(record.getClass()), fieldName).get(record);
    } catch (Exception e) {
      try {
        throw Throwables.getRootCause(e);
      } catch (NoSuchFieldException | IllegalAccessException ex) {
        // If there no such field or cannot get the field value, just treat it as null.
        return null;
      } catch (RuntimeException ex) {
        throw ex;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }
}
