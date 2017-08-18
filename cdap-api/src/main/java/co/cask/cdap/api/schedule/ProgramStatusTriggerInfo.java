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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The program status trigger information to be passed to the triggered program.
 */
public class ProgramStatusTriggerInfo extends TriggerInfo {

  private final String namespace;
  private final ApplicationSpecification applicationSpecification;
  private final String program;
  private final String programRunId;
  private final ProgramStatus programStatus;
  private final Set<ProgramStatus> triggerStatues;
  private final WorkflowToken userWorkflowToken;
  private final Map<String, String> runtimeArguments;

  public ProgramStatusTriggerInfo(String namespace, ApplicationSpecification applicationSpecification,
                                  String program, String programRunId, ProgramStatus programStatus,
                                  Set<ProgramStatus> triggerStatues,
                                  WorkflowToken userWorkflowToken, Map<String, String> runtimeArguments) {
    super(Trigger.Type.PROGRAM_STATUS);
    this.namespace = namespace;
    this.applicationSpecification = applicationSpecification;
    this.program = program;
    this.programRunId = programRunId;
    this.programStatus = programStatus;
    this.triggerStatues = triggerStatues;
    this.userWorkflowToken = userWorkflowToken;
    this.runtimeArguments = runtimeArguments;
  }

  /**
   * @return The namespace of the triggering program.
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return The application specification of the application that contains the triggering program.
   */
  ApplicationSpecification getApplicationSpecifications() {
    return applicationSpecification;
  }

  /**
   * @return The program name of the triggering program.
   */
  public String getProgram() {
    return program;
  }

  /**
   * @return The program run Id of the triggering program run.
   */
  public String getProgramRunId() {
    return programRunId;
  }

  /**
   * @return The program status of the triggering program.
   */
  public ProgramStatus getProgramStatus() {
    return programStatus;
  }

  /**
   * @return All the program statuses that can satisfy the program status trigger.
   */
  public Set<ProgramStatus> getTriggerStatues() {
    return triggerStatues;
  }

  /**
   * @return The application specification of the parent application which contains
   *         the program that triggers the schedule.
   */
  public ApplicationSpecification getApplicationSpecification() {
    return applicationSpecification;
  }

  /**
   * @return The user-scope workflow token if the program is a workflow, or {@code null} otherwise.
   */
  @Nullable
  public WorkflowToken getUserWorkflowToken() {
    return userWorkflowToken;
  }

  /**
   * @return The runtime arguments key-value pairs as values.
   */
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }
}
