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

import java.util.Map;

/**
 * The information of a schedule to be passed to the program launched by it
 */
public class TriggeringScheduleInfo {

  private final String name;
  private final String description;
  private final TriggerInfo triggerInfo;
  private final Map<String, String> properties;

  public TriggeringScheduleInfo(String name, String description, TriggerInfo triggerInfo,
                                Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.properties = properties;
    this.triggerInfo = triggerInfo;
  }

  /**
   * @return Schedule's name, which is unique in an application.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Description of the schedule.
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return Information of the trigger contained in this schedule.
   */
  public TriggerInfo getTriggerInfo() {
    return triggerInfo;
  }

  /**
   * @return Properties of the schedule.
   */
  public Map<String, String> getProperties() {
    return properties;
  }
}
