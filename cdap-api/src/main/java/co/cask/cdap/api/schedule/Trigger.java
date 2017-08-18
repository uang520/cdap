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

package co.cask.cdap.api.schedule;

import java.util.HashMap;
import java.util.Map;

/**
 * A condition must be satisfied in order to trigger a schedule.
 */
public interface Trigger {
  /**
   * Represents all known trigger types.
   */
  enum Type {
    TIME("time"),
    PARTITION("partition"),
    STREAM_SIZE("stream-size"),
    PROGRAM_STATUS("program-status"),
    AND("and", true),
    OR("or", true);

    private static final Map<String, Type> CATEGORY_MAP;

    static {
      CATEGORY_MAP = new HashMap<>();
      for (Type type : Type.values()) {
        CATEGORY_MAP.put(type.getCategoryName(), type);
      }
    }

    private final String categoryName;
    private final boolean isComposite;

    Type(String categoryName) {
      this(categoryName, false);
    }

    Type(String categoryName, boolean isComposite) {
      this.categoryName = categoryName;
      this.isComposite = isComposite;
    }

    /**
     * @return The category name of the type.
     */
    public String getCategoryName() {
      return categoryName;
    }

    /**
     * Get the corresponding type with the given category name of the type
     *
     * @param categoryName the category name to get the type for
     * @return the corresponding type of the given category name
     */
    public static Type valueOfCategoryName(String categoryName) {
      Type type = CATEGORY_MAP.get(categoryName);
      if (type == null) {
        throw new IllegalArgumentException("Unknown category name " + categoryName);
      }
      return type;
    }

    /**
     * Whether the trigger type represents a composite trigger, i.e. a trigger that can
     * contains multiple triggers internally.
     *
     * @return {@code true} if the trigger type represents a composite trigger, {@code false} otherwise
     */
    public boolean isComposite() {
      return isComposite;
    }
  }

  /**
   * @return The type of this trigger.
   */
  Type getType();
}
