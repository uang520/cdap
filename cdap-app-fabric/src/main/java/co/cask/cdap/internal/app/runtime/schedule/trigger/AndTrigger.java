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

package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.api.schedule.AndTriggerInfo;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.proto.Notification;

import java.util.List;

/**
 * A Trigger that schedules a ProgramSchedule, when all internal triggers are satisfied.
 */
public class AndTrigger extends AbstractCompositeTrigger implements SatisfiableTrigger {

  public AndTrigger(SatisfiableTrigger... triggers) {
    super(Type.AND, triggers);
  }

  @Override
  public boolean isSatisfied(List<Notification> notifications) {
    for (Trigger trigger : triggers) {
      if (!((SatisfiableTrigger) trigger).isSatisfied(notifications)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public TriggerInfo getTriggerInfo(TriggerInfoContext context) {
    return new AndTriggerInfo(getUnitTriggerInfos(context));
  }
}
