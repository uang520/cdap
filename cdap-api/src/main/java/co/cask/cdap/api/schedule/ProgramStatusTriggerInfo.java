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
import co.cask.cdap.api.app.ProgramType;
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
  private final ProgramType programType;
  private final String program;
  private final Set<ProgramStatus> triggerStatues;
  @Nullable
  private final String runId;
  @Nullable
  private final ProgramStatus programStatus;
  @Nullable
  private final WorkflowToken workflowToken;
  @Nullable
  private final Map<String, String> runtimeArguments;

  public ProgramStatusTriggerInfo(String namespace, ApplicationSpecification applicationSpecification,
                                  ProgramType programType, String program, Set<ProgramStatus> triggerStatues,
                                  @Nullable String runId, @Nullable ProgramStatus programStatus,
                                  @Nullable WorkflowToken workflowToken,
                                  @Nullable Map<String, String> runtimeArguments) {
    super(Trigger.Type.PROGRAM_STATUS);
    this.namespace = namespace;
    this.applicationSpecification = applicationSpecification;
    this.programType = programType;
    this.program = program;
    this.triggerStatues = triggerStatues;
    this.runId = runId;
    this.programStatus = programStatus;
    this.workflowToken = workflowToken;
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
  public ApplicationSpecification getApplicationSpecification() {
    return applicationSpecification;
  }

  /**
   * @return The program type of the triggering program.
   */
  public ProgramType getProgramType() {
    return programType;
  }

  /**
   * @return The program name of the triggering program.
   */
  public String getProgram() {
    return program;
  }

  /**
   * @return All the program statuses that can satisfy the program status trigger.
   */
  public Set<ProgramStatus> getTriggerStatues() {
    return triggerStatues;
  }

  /**
   * @return The program run Id of the triggering program run that can satisfy the program status trigger,
   *         or {@code null} if there is no such run.
   */
  @Nullable
  public String getRunId() {
    return runId;
  }

  /**
   * @return The program status of the triggering program run that can satisfy the program status trigger,
   *         or {@code null} if there is no such run.
   */
  @Nullable
  public ProgramStatus getProgramStatus() {
    return programStatus;
  }

  /**
   * @return The workflow token if the program is a workflow with a run that can
   *         satisfy the program status trigger, or an empty workflow token if there's no such run.
   *         Return {@code null} if the program is not a workflow.
   */
  @Nullable
  public WorkflowToken getWorkflowToken() {
    return workflowToken;
  }

  /**
   * @return The runtime arguments of the triggering program run that can satisfy the program status trigger,
   *         or {@code null} if there is no such run.
   */
  @Nullable
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }
}
