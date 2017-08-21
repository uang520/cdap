/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.planner;

import co.cask.cdap.etl.api.condition.Condition;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.spec.PipelineSpec;
import co.cask.cdap.etl.spec.PluginSpec;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Takes a {@link PipelineSpec} and creates an execution plan from it.
 */
public class PipelinePlanner {
  private final Set<String> reduceTypes;
  private final Set<String> isolationTypes;
  private final Set<String> supportedPluginTypes;
  private final Set<String> actionTypes;

  public PipelinePlanner(Set<String> supportedPluginTypes, Set<String> reduceTypes, Set<String> isolationTypes,
                         Set<String> actionTypes) {
    this.reduceTypes = ImmutableSet.copyOf(reduceTypes);
    this.isolationTypes = ImmutableSet.copyOf(isolationTypes);
    this.supportedPluginTypes = ImmutableSet.copyOf(supportedPluginTypes);
    this.actionTypes = ImmutableSet.copyOf(actionTypes);
  }

  /**
   * Create an execution plan for the given logical pipeline. This is used for batch pipelines.
   * Though it may eventually be useful to mark windowing points for realtime pipelines.
   *
   * A plan consists of one or more phases, with connections between phases.
   * A connection between a phase indicates control flow, and not necessarily
   * data flow. This class assumes that it receives a valid pipeline spec.
   * That is, the pipeline has no cycles, all its nodes have unique names,
   * sources don't have any input, sinks don't have any output,
   * everything else has both an input and an output, etc.
   *
   * We start by inserting connector nodes into the logical dag,
   * which are used to mark boundaries between mapreduce jobs.
   * Each connector represents a node where we will need to write to a local dataset.
   *
   * Next, the logical pipeline is broken up into phases,
   * using the connectors as sinks in one phase, and a source in another.
   * After this point, connections between phases do not indicate data flow, but control flow.
   *
   * @param spec the pipeline spec, representing a logical pipeline
   * @return the execution plan
   */
  public PipelinePlan plan(PipelineSpec spec) {
    // go through the stages and examine their plugin type to determine which stages are reduce stages
    Set<String> reduceNodes = new HashSet<>();
    Set<String> isolationNodes = new HashSet<>();
    Set<String> actionNodes = new HashSet<>();
    Map<String, StageSpec> specs = new HashMap<>();

    // Map to hold the connection information from condition nodes to the first stage
    // they connect to. Condition information also includes whether the stage is connected
    // on the 'true' branch or the 'false' branch
    Map<String, ConditionBranches> conditionBranches = new HashMap<>();

    for (StageSpec stage : spec.getStages()) {
      String pluginType = stage.getPlugin().getType();
      if (reduceTypes.contains(pluginType)) {
        reduceNodes.add(stage.getName());
      }
      if (isolationTypes.contains(pluginType)) {
        isolationNodes.add(stage.getName());
      }
      if (actionTypes.contains(pluginType)) {
        // Collect all Action nodes from spec
        actionNodes.add(stage.getName());
      }
      if (Condition.PLUGIN_TYPE.equals(pluginType)) {
        conditionBranches.put(stage.getName(), new ConditionBranches(null, null));
      }
      specs.put(stage.getName(), stage);
    }

    // Set represents the control nodes in the pipeline - Action and Condition
    Set<String> controlNodes = Sets.union(actionNodes, conditionBranches.keySet());

    // Map to hold chained control nodes information. This is used to reuse the connector datasets.
    // For example if the dag is file-->condition-->action--->parse-->sink. In this case connector need
    // to be inserted to hold the information from file. However same connector would act as a source
    // for parse. The following map will contain entry <action, condition> indicating that for action,
    // we need to use the connector for condition.
    Map<String, String> controlChildToParent = new HashMap<>();

    for (Connection connection : spec.getConnections()) {
      if (controlNodes.contains(connection.getFrom()) && controlNodes.contains(connection.getTo())) {
        // control nodes are chained together
        controlChildToParent.put(connection.getTo(), connection.getFrom());
      }

      if (conditionBranches.containsKey(connection.getFrom())) {
        // Outgoing connection from condition
        ConditionBranches branches = conditionBranches.get(connection.getFrom());
        String trueOutput;
        String falseOutput;
        if (connection.getCondition()) {
          trueOutput = connection.getTo();
          falseOutput = branches.getFalseOutput();
        } else {
          trueOutput = branches.getTrueOutput();
          falseOutput = connection.getTo();
        }
        conditionBranches.put(connection.getFrom(), new ConditionBranches(trueOutput, falseOutput));
      }
    }

    Map<String, String> connectorNodes = new HashMap<>();
    Set<Dag> splittedDags = split(spec.getConnections(), conditionBranches.keySet(), reduceNodes, isolationNodes,
                                  actionNodes, connectorNodes);

    // convert to objects the programs expect.
    Map<String, PipelinePhase> phases = new HashMap<>();

    // now split the logical pipeline into pipeline phases, using the connectors as split points
    Map<String, Dag> subdags = new HashMap<>();
    // assign some name to each subdag
    for (Dag subdag : splittedDags) {
      String name = getPhaseName(subdag.getSources(), subdag.getSinks());
      subdags.put(name, subdag);
      addControlPhasesIfRequired(controlNodes, subdag, specs, phases);
    }

    Set<String> processControlNodes = new HashSet<>();
    // build connections between phases
    Set<Connection> phaseConnections = new HashSet<>();
    for (Map.Entry<String, Dag> subdagEntry1 : subdags.entrySet()) {
      String dag1Name = subdagEntry1.getKey();
      Dag dag1 = subdagEntry1.getValue();

      for (Map.Entry<String, Dag> subdagEntry2: subdags.entrySet()) {
        String dag2Name = subdagEntry2.getKey();
        Dag dag2 = subdagEntry2.getValue();
        if (dag1Name.equals(dag2Name)) {
          continue;
        }

        // if dag1 has any sinks that are a source in dag2, add a connection between the dags
        if (Sets.intersection(dag1.getSinks(), dag2.getSources()).size() > 0) {
          Set<String> controlAsSink = Sets.intersection(dag1.getSinks(), controlNodes);
          if (!controlAsSink.isEmpty()) {
            String controlNodeName = controlAsSink.iterator().next();

            if (!controlNodes.containsAll(dag1.getNodes())) {
              // dag1 does not only contain control nodes in it
              phaseConnections.add(new Connection(dag1Name, controlNodeName));
            }

            ConditionBranches branches = conditionBranches.get(controlNodeName);
            if (branches != null) {
              for (Boolean condition : Arrays.asList(true, false)) {
                String stage = condition ? branches.getTrueOutput() : branches.getFalseOutput();
                if (stage == null) {
                  continue;
                }

                if (!dag2.getNodes().contains(stage)) {
                  continue;
                }

                if (controlNodes.containsAll(dag2.getNodes())) {
                  // dag 2 only contains condition stages. dag1 sink is same as dag 2 source condition node.
                  // so we added condition phase corresponding to the dag1 sink. Now phase connection should be
                  // added from newly created condition phase to the dag 2 sink condition, rather than using dag2name
                  // here. The scenario here is n1-c1-c2-c3-n2. dag1 is <n1, c1> and dag 2 is <c1, c2>. We just created
                  // c1 as new phase. since dag2 satisfies the above if condition (as it only contains conditions) we add
                  // phase connection from c1->c2, rather than c1->c1.to.c2
                  phaseConnections.add(new Connection(controlNodeName, dag2.getSinks().iterator().next(),
                                                      condition));
                } else {
                  // scenario here is n1-c1-c2-n2. dag 1 is <c1, c2> and dag 2 is <c2, n2>. We just created condition phase
                  // for c2 and the phase connection should be c2->c2.to.n2
                  phaseConnections.add(new Connection(controlNodeName, dag2Name, condition));
                }
                break;
              }
            } else {
              if (controlNodes.containsAll(dag2.getNodes())) {
                phaseConnections.add(new Connection(controlNodeName, dag2.getSinks().iterator().next()));
              } else {
                phaseConnections.add(new Connection(controlNodeName, dag2Name));
              }
            }
            processControlNodes.add(controlNodeName);
          } else {
            phaseConnections.add(new Connection(dag1Name, dag2Name));
          }
        } else {
          if (Sets.intersection(dag1.getSources(), dag2.getSources()).size() > 0) {
            Set<String> controlAsSource = Sets.intersection(dag1.getSources(), controlNodes);
            if (!controlAsSource.isEmpty()) {
              String controlNodeName = controlAsSource.iterator().next();

              ConditionBranches branches = conditionBranches.get(controlNodeName);
              if (branches != null) {
                for (Boolean condition : Arrays.asList(true, false)) {
                  String stage = condition ? branches.getTrueOutput() : branches.getFalseOutput();
                  if (stage == null) {
                    continue;
                  }

                  if (!dag2.getNodes().contains(stage)) {
                    continue;
                  }

                  if (controlNodes.containsAll(dag2.getNodes())) {
                    // dag 2 only contains condition stages. dag1 sink is same as dag 2 source condition node.
                    // so we added condition phase corresponding to the dag1 sink. Now phase connection should be
                    // added from newly created condition phase to the dag 2 sink condition, rather than using dag2name
                    // here. The scenario here is n1-c1-c2-c3-n2. dag1 is <n1, c1> and dag 2 is <c1, c2>. We just created
                    // c1 as new phase. since dag2 satisfies the above if condition (as it only contains conditions) we add
                    // phase connection from c1->c2, rather than c1->c1.to.c2
                    phaseConnections.add(new Connection(controlNodeName, dag2.getSinks().iterator().next(),
                                                        condition));
                  } else {
                    // scenario here is n1-c1-c2-n2. dag 1 is <c1, c2> and dag 2 is <c2, n2>. We just created condition phase
                    // for c2 and the phase connection should be c2->c2.to.n2
                    phaseConnections.add(new Connection(controlNodeName, dag2Name, condition));
                  }
                  break;
                }
              } else {
                if (controlNodes.containsAll(dag2.getNodes())) {
                  phaseConnections.add(new Connection(controlNodeName, dag2.getSinks().iterator().next()));
                } else {
                  phaseConnections.add(new Connection(controlNodeName, dag2Name));
                }
              }
              processControlNodes.add(controlNodeName);
            } else {
              phaseConnections.add(new Connection(dag1Name, dag2Name));
            }
          }
        }
      }
    }

    Map<String, String> controlConnectors = getConnectorsAssociatedWithControlNodes(controlNodes, controlChildToParent);
    for (Map.Entry<String, Dag> dagEntry : subdags.entrySet()) {
      // If dag only contains control nodes then ignore it
      if (controlNodes.containsAll(dagEntry.getValue().getNodes())) {
        continue;
      }

      Dag dag = dagEntry.getValue();
      if (controlNodes.containsAll(dag.getSources())) {
        String source = dag.getSources().iterator().next();
        if (!processControlNodes.contains(source)) {
          // This control node is not processed yet, because it is first in the pipeline.
          ConditionBranches branches = conditionBranches.get(source);
          if (branches != null) {
            // this is condition node
            boolean condition = false;
            if (branches.getTrueOutput() != null && dag.getNodes().contains(branches.getTrueOutput())) {
              condition = true;
            }
            phaseConnections.add(new Connection(source, dagEntry.getKey(), condition));
          } else {
            phaseConnections.add(new Connection(source, dagEntry.getKey()));
          }
          processControlNodes.add(source);
        }
      }

      if (controlNodes.containsAll(dag.getSinks())) {
        String sink = dag.getSinks().iterator().next();
        if (!processControlNodes.contains(sink)) {
          // This control node is not processed yet, because it is last in the pipeline.
          // Conditions cannot be last however action nodes can be
          phaseConnections.add(new Connection(dagEntry.getKey(), sink));
          processControlNodes.add(sink);
        }
      }

      Dag updatedDag = getUpdatedDag(dagEntry.getValue(), controlConnectors);
      phases.put(dagEntry.getKey(), dagToPipeline(updatedDag, connectorNodes, specs, controlConnectors));
    }

    return new PipelinePlan(phases, phaseConnections);
  }

  private void addControlPhasesIfRequired(Set<String> controlNodes, Dag dag, Map<String, StageSpec> stageSpecs,
                                          Map<String, PipelinePhase> phases) {
    // Add control phases corresponding to the subdag source
    if (controlNodes.containsAll(dag.getSources())) {
      if (dag.getSources().size() != 1) {
        // sources should only have a single stage if its control
        throw new IllegalStateException(String.format("Dag '%s' to '%s' has control as well " +
                                                        "as non-control stages in source which is not " +
                                                        "allowed.", dag.getSources(), dag.getSinks()));
      }

      String controlNode = dag.getSources().iterator().next();
      if (!phases.containsKey(controlNode)) {
        PipelinePhase.Builder phaseBuilder = PipelinePhase.builder(supportedPluginTypes);
        PipelinePhase controlPhase = phaseBuilder
          .addStage(stageSpecs.get(controlNode))
          .build();
        phases.put(controlNode, controlPhase);
      }
    }

    // Add control phases corresponding to the subdag sink
    if (controlNodes.containsAll(dag.getSinks())) {
      if (dag.getSinks().size() != 1) {
        // sinks should only have a single stage if its control
        throw new IllegalStateException(String.format("Dag '%s' to '%s' has control as well " +
                                                        "as non-control stages in sinks which is not " +
                                                        "allowed.", dag.getSources(), dag.getSinks()));
      }

      String controlNode = dag.getSinks().iterator().next();
      if (!phases.containsKey(controlNode)) {
        PipelinePhase.Builder phaseBuilder = PipelinePhase.builder(supportedPluginTypes);
        PipelinePhase controlPhase = phaseBuilder
          .addStage(stageSpecs.get(controlNode))
          .build();
        phases.put(controlNode, controlPhase);
      }
    }
  }

  /**
   * This method is responsible for returning {@link Map} of condition and associated connector name.
   * By default each condition will have associated connector named as conditionname.connector. This connector
   * will hold the data from the previous phase. However it is possible that multiple conditions are
   * chained together. In this case we do not need to create individual connector for each of the child condition
   * but they should have connector same as parent condition.
   * @param controlNodes Set of condition nodes in the spec
   * @param childToParent Map contains only conditions as keys. The corresponding value represents its immediate parent
   * @return the resolved connectors for each condition
   */
  private Map<String, String> getConnectorsAssociatedWithControlNodes(Set<String> controlNodes,
                                                                      Map<String, String> childToParent) {
    Map<String, String> controlConnectors = new HashMap<>();
    for (String controlNode : controlNodes) {
      // Put the default connector for the control node
      controlConnectors.put(controlNode, controlNode + ".connector");
    }

    for (Map.Entry<String, String> entry : childToParent.entrySet()) {
      String parent = childToParent.get(entry.getValue());
      controlConnectors.put(entry.getKey(), entry.getValue() + ".connector");
      while (parent != null) {
        controlConnectors.put(entry.getKey(), parent + ".connector");
        parent = childToParent.get(parent);
      }
    }
    return controlConnectors;
  }

  /**
   * Update the current dag by replacing conditions in the dag with the corresponding condition connectors
   */
  private Dag getUpdatedDag(Dag dag, Map<String, String> controlConnectors) {
    Set<String> controlAsSources = Sets.intersection(controlConnectors.keySet(), dag.getSources());
    Set<String> controlAsSink =  Sets.intersection(controlConnectors.keySet(), dag.getSinks());
    if (controlAsSources.isEmpty() && controlAsSink.isEmpty()) {
      return dag;
    }

    Set<Connection> newConnections = new HashSet<>();

    for (String node : dag.getNodes()) {
      String newNode = controlConnectors.get(node) == null ? node : controlConnectors.get(node);
      for (String inputNode : dag.getNodeInputs(node)) {
        newConnections.add(new Connection(controlConnectors.get(inputNode) == null ? inputNode
                                            : controlConnectors.get(inputNode), newNode));
      }

      for (String outputNode : dag.getNodeOutputs(node)) {
        newConnections.add(new Connection(newNode, controlConnectors.get(outputNode) == null ? outputNode
          : controlConnectors.get(outputNode)));
      }
    }

    return new Dag(newConnections);
  }

  /**
   * Converts a Dag into a PipelinePhase, using what we know about the plugin type of each node in the dag.
   * The PipelinePhase is what programs will take as input, and keeps track of sources, transforms, sinks, etc.
   *
   * @param dag the dag to convert
   * @param connectors connector nodes across all dags
   * @param specs specifications for every stage
   * @return the converted dag
   */
  private PipelinePhase dagToPipeline(Dag dag, Map<String, String> connectors, Map<String, StageSpec> specs,
                                      Map<String, String> conditionConnectors) {
    PipelinePhase.Builder phaseBuilder = PipelinePhase.builder(supportedPluginTypes);

    for (String stageName : dag.getTopologicalOrder()) {
      Set<String> outputs = dag.getNodeOutputs(stageName);
      if (!outputs.isEmpty()) {
        phaseBuilder.addConnections(stageName, outputs);
      }

      // add connectors
      String originalName = connectors.get(stageName);
      if (originalName != null || conditionConnectors.values().contains(stageName)) {
        PluginSpec connectorSpec =
          new PluginSpec(Constants.CONNECTOR_TYPE, "connector",
                         ImmutableMap.of(Constants.CONNECTOR_ORIGINAL_NAME, originalName != null
                           ? originalName : stageName), null);
        phaseBuilder.addStage(StageSpec.builder(stageName, connectorSpec).build());
        continue;
      }

      // add other plugin types
      StageSpec spec = specs.get(stageName);
      phaseBuilder.addStage(spec);
    }

    return phaseBuilder.build();
  }

  @VisibleForTesting
  static String getPhaseName(Set<String> sources, Set<String> sinks) {
    // using sorted sets to guarantee the name is deterministic
    return Joiner.on('.').join(new TreeSet<>(sources)) +
      ".to." +
      Joiner.on('.').join(new TreeSet<>(sinks));
  }

  @VisibleForTesting
  static Set<Dag> split(Set<Connection> connections, Set<String> conditions, Set<String> reduceNodes,
                        Set<String> isolationNodes, Set<String> actionNodes, Map<String, String> connectorNodes) {
    Dag dag = new Dag(connections);
    Set<Dag> subdags = dag.splitByControlNodes(Sets.union(conditions, actionNodes));

    Set<Dag> result = new HashSet<>();
    for (Dag subdag : subdags) {
      if (Sets.union(conditions, actionNodes).containsAll(subdag.getNodes())) {
        result.add(subdag);
        continue;
      }
      Set<String> subdagReduceNodes = Sets.intersection(reduceNodes, subdag.getNodes());
      Set<String> subdagIsolationNodes = Sets.intersection(isolationNodes, subdag.getNodes());

      ConnectorDag cdag = ConnectorDag.builder()
        .addDag(subdag)
        .addReduceNodes(subdagReduceNodes)
        .addIsolationNodes(subdagIsolationNodes)
        .build();

      cdag.insertConnectors();
      connectorNodes.putAll(cdag.getConnectors());
      result.addAll(cdag.split());
    }
    return result;
  }
}
