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

import co.cask.cdap.api.schedule.TimeTriggerInfo;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProtoTrigger;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Trigger that schedules a ProgramSchedule, based upon a particular cron expression.
 */
public class TimeTrigger extends ProtoTrigger.TimeTrigger implements SatisfiableTrigger {
  private static final Logger LOG = LoggerFactory.getLogger(TimeTrigger.class);
  private static final Gson GSON = new Gson();
  private static final java.lang.reflect.Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  public TimeTrigger(String cronExpression) {
    super(cronExpression);
    validate();
  }

  @Override
  public void validate() {
    Schedulers.validateCronExpression(cronExpression);
  }

  @Override
  public boolean isSatisfied(List<Notification> notifications) {
    for (Notification notification : notifications) {
      if (!notification.getNotificationType().equals(Notification.Type.TIME)) {
        continue;
      }
      String systemOverridesString = notification.getProperties().get(ProgramOptionConstants.SYSTEM_OVERRIDES);
      if (systemOverridesString != null) {
        Map<String, String> systemOverrides = GSON.fromJson(systemOverridesString, STRING_STRING_MAP);
        return cronExpression.equals(systemOverrides.get(ProgramOptionConstants.CRON_EXPRESSION));
      }
    }
    return false;
  }

  @Override
  public Set<String> getTriggerKeys() {
    return ImmutableSet.of();
  }

  @Override
  public TimeTriggerInfo getTriggerInfo(TriggerInfoContext context) {
    for (Notification notification : context.getNotifications()) {
      if (!notification.getNotificationType().equals(Notification.Type.TIME)) {
        continue;
      }
      String systemOverridesString = notification.getProperties().get(ProgramOptionConstants.SYSTEM_OVERRIDES);
      String userOverridesString = notification.getProperties().get(ProgramOptionConstants.USER_OVERRIDES);
      if (systemOverridesString == null || userOverridesString == null) {
        continue;
      }
      Map<String, String> systemOverrides = GSON.fromJson(systemOverridesString, STRING_STRING_MAP);
      Map<String, String> userOverrides = GSON.fromJson(userOverridesString, STRING_STRING_MAP);

      if (cronExpression.equals(systemOverrides.get(ProgramOptionConstants.CRON_EXPRESSION))) {

        String logicalStartTime = userOverrides.get(ProgramOptionConstants.LOGICAL_START_TIME);
        return new TimeTriggerInfo(cronExpression, logicalStartTime);
      }
    }
    LOG.debug("No logical start time found from notifications {} for TimeTrigger with cron expression '{}' " +
                "in schedule '{}'", context.getNotifications(), cronExpression, context.getSchedule());
    return new TimeTriggerInfo(cronExpression, null);
  }

  public TimeTriggerInfo getTriggerInfoFromSingleNotification(TriggerInfoContext context) throws SchedulerException {
    // TimeTrigger is satisfied as soon as the first Notification arrives,
    // due to how the Notification is initially created and processed.
    if (context.getNotifications().size() != 1) {
      throw new SchedulerException(String.format("There should be only 1 notification in the job of schedule '%s' " +
                                                   "with satisfied TimeTrigger, but there are notifications '%s'",
                                                 context.getSchedule(), context.getNotifications()));
    }
    Notification notification = context.getNotifications().get(0);
    if (!notification.getNotificationType().equals(Notification.Type.TIME)) {
      throw new SchedulerException(String.format("The notification '%s' in the job of schedule '%s' " +
                                                   "is expected with type '%s' but it is not.",
                                                 notification, context.getSchedule(), Notification.Type.TIME.name()));
    }
    String userOverridesString = notification.getProperties().get(ProgramOptionConstants.USER_OVERRIDES);
    if (userOverridesString == null) {
      LOG.debug("No user overrides found from notification {} for TimeTrigger with cron expression '{}' " +
                  "in schedule '{}'", notification, cronExpression, context.getSchedule());
      return new TimeTriggerInfo(cronExpression, null);
    }
    Map<String, String> userOverrides = GSON.fromJson(userOverridesString, STRING_STRING_MAP);
    String logicalStartTime = userOverrides.get(ProgramOptionConstants.LOGICAL_START_TIME);
    if (logicalStartTime == null) {
      LOG.debug("No logical start time found from notification {} for TimeTrigger with cron expression '{}' " +
                  "in schedule '{}'", notification, cronExpression, context.getSchedule());
      return new TimeTriggerInfo(cronExpression, null);
    }
    return new TimeTriggerInfo(cronExpression, logicalStartTime);
  }
}
