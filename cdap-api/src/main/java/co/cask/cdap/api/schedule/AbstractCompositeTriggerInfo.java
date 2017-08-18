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

import java.util.List;

/**
 * Base class for the composite trigger information to be passed to the triggered program.
 */
public abstract class AbstractCompositeTriggerInfo extends TriggerInfo {
  List<TriggerInfo> unitTriggerInfos;

  public AbstractCompositeTriggerInfo(Trigger.Type type, List<TriggerInfo> unitTriggerInfos) {
    super(type);
    this.unitTriggerInfos = unitTriggerInfos;
  }

  /**
   * @return A list of unique {@link TriggerInfo}'s of the unit triggers in this composite trigger.
   *         Unit triggers are the triggers which are not composite triggers.
   */
  public List<TriggerInfo> getUnitTriggerInfos() {
    return unitTriggerInfos;
  }
}
