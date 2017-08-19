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


import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.schedule.ProgramStatusTriggerInfo;
import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A Trigger that schedules a ProgramSchedule, when a certain status of a program has been achieved.
 */
public class ProgramStatusTrigger extends ProtoTrigger.ProgramStatusTrigger implements SatisfiableTrigger {
  private static final Gson GSON = new Gson();

  public ProgramStatusTrigger(ProgramId programId, Set<ProgramStatus> programStatuses) {
    super(programId, programStatuses);
  }

  @VisibleForTesting
  public ProgramStatusTrigger(ProgramId programId, ProgramStatus... programStatuses) {
    super(programId, new HashSet<>(Arrays.asList(programStatuses)));
  }

  @Override
  public boolean isSatisfied(List<Notification> notifications) {
    for (Notification notification : notifications) {
      if (!Notification.Type.PROGRAM_STATUS.equals(notification.getNotificationType())) {
        continue;
      }
      String programRunIdString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String programRunStatusString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);
      // Ignore notifications which specify an invalid programRunId or programStatus
      if (programRunIdString == null || programRunStatusString == null) {
        continue;
      }
      ProgramStatus programStatus;
      try {
        programStatus = ProgramRunStatus.toProgramStatus(ProgramRunStatus.valueOf(programRunStatusString));
      } catch (IllegalArgumentException e) {
        // Return silently, this happens for statuses that are not meant to be scheduled
        continue;
      }
      ProgramRunId programRunId = GSON.fromJson(programRunIdString, ProgramRunId.class);
      ProgramId triggeringProgramId = programRunId.getParent();
      if (this.programId.equals(triggeringProgramId) && programStatuses.contains(programStatus)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Set<String> getTriggerKeys() {
    return Schedulers.triggerKeysForProgramStatus(programId, programStatuses);
  }

  @Override
  public TriggerInfo getTriggerInfo(TriggerInfoContext context) {
    for (Notification notification : context.getNotifications()) {
      if (!Notification.Type.PROGRAM_STATUS.equals(notification.getNotificationType())) {
        continue;
      }
      String programRunIdString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String programRunStatusString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);
      // Ignore notifications which specify an invalid programRunId or programStatus
      if (programRunIdString == null || programRunStatusString == null) {
        continue;
      }
      ProgramStatus programStatus;
      try {
        programStatus = ProgramRunStatus.toProgramStatus(ProgramRunStatus.valueOf(programRunStatusString));
      } catch (IllegalArgumentException e) {
        // Return silently, this happens for statuses that are not meant to be scheduled
        continue;
      }
      ProgramRunId programRunId = GSON.fromJson(programRunIdString, ProgramRunId.class);
      ProgramId triggeringProgramId = programRunId.getParent();

      if (this.programId.equals(triggeringProgramId) && programStatuses.contains(programStatus)) {
        return new ProgramStatusTriggerInfo(programId.getNamespace(),
                                            context.getApplicationSpecification(programId.getParent()),
                                            ProgramType.valueOf(programId.getType().name()), programId.getProgram(),
                                            programStatuses, programRunId.getRun(), programStatus,
                                            context.getWorkflowToken(programId, programRunId.getRun()),
                                            context.getProgramRuntimeArguments(programId, programRunId.getRun()));
      }
    }
    return new ProgramStatusTriggerInfo(programId.getNamespace(),
                                        context.getApplicationSpecification(programId.getParent()),
                                        ProgramType.valueOf(programId.getType().name()), programId.getProgram(),
                                        programStatuses, null, null, null, null);
  }
}
