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

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.app.runtime.spark.sql.ObjectRowConverter;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit test for {@link ObjectRowConverter}.
 */
public class ObjectRowConverterTest {

  @Test
  public void testObjectConvertion() throws UnsupportedTypeException {
    StructType structType = DataFrames.toDataType(new ReflectionSchemaGenerator(true).generate(Person.class));

    Person person = new Person("Dummy", 20, "d", "u", "m");
    Row row = new ObjectRowConverter().toRow(person, structType);

    Assert.assertEquals("Dummy", row.get(structType.fieldIndex("name")));
    Assert.assertEquals(Integer.valueOf(20), row.get(structType.fieldIndex("age")));
    Assert.assertEquals(Arrays.asList("d", "u", "m"), row.getList(structType.fieldIndex("nicknames")));
  }

  @Test
  public void testMissingField() throws UnsupportedTypeException {
    StructType structType = DataFrames.toDataType(new ReflectionSchemaGenerator(true).generate(Person.class));
    structType = structType.add("horoscope", DataTypes.StringType);

    Person person = new Person("Dummy", 20, "d", "u", "m");
    Row row = new ObjectRowConverter().toRow(person, structType);

    Assert.assertEquals("Dummy", row.get(structType.fieldIndex("name")));
    Assert.assertEquals(Integer.valueOf(20), row.get(structType.fieldIndex("age")));
    Assert.assertEquals(Arrays.asList("d", "u", "m"), row.getList(structType.fieldIndex("nicknames")));
    Assert.assertNull(row.get(structType.fieldIndex("horoscope")));
  }

  /**
   * Testing object class
   */
  public static final class Person {
    final String name;
    final int age;
    final List<String> nicknames;

    public Person(String name, int age, String...nicknames) {
      this.name = name;
      this.age = age;
      this.nicknames = new ArrayList<>(Arrays.asList(nicknames));
    }
  }
}
