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

import co.cask.cdap.etl.proto.Connection;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 */
public class ConnectorDagTest {

  private static final Map<String, String> EMPTY_CONNECTORS = new HashMap<>();
  private static final Set<String> EMPTY_ACTIONS = new HashSet<>();

  @Test
  public void testMultipleSourcesIsNoOp() {
    /*
        n1 --|
             |--- n3
        n2 --|
     */
    ConnectorDag cdag = ConnectorDag.builder()
      .addConnection("n1", "n3")
      .addConnection("n2", "n3")
      .build();
    cdag.insertConnectors();
    // no connector should be inserted since we support multiple inputs in a dag
    ConnectorDag expected = ConnectorDag.builder()
      .addConnection("n1", "n3")
      .addConnection("n2", "n3")
      .build();
    Assert.assertEquals(expected, cdag);

    /*
        n1 --|
             |--- n3 --- n4
        n2 --|
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n3")
      .addConnection("n2", "n3")
      .addConnection("n3", "n4")
      .build();
    cdag.insertConnectors();
    // no connector should have been inserted
    expected = ConnectorDag.builder()
      .addConnection("n1", "n3")
      .addConnection("n2", "n3")
      .addConnection("n3", "n4")
      .build();
    Assert.assertEquals(expected, cdag);


    /*
        n1 --|
             |--- n4 ---|
        n2 --|          |-- n6 --- n9
             |--- n5 ---|
        n3 --|     |
                   |------ n7 ---- n10
                   |        |
                   |------------ n8 -- n11
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n4")
      .addConnection("n1", "n5")
      .addConnection("n2", "n4")
      .addConnection("n2", "n5")
      .addConnection("n3", "n4")
      .addConnection("n3", "n5")
      .addConnection("n4", "n6")
      .addConnection("n5", "n6")
      .addConnection("n5", "n7")
      .addConnection("n5", "n8")
      .addConnection("n6", "n9")
      .addConnection("n7", "n8")
      .addConnection("n7", "n10")
      .addConnection("n8", "n11")
      .build();
    cdag.insertConnectors();
    // since we support multiple inputs, no connectors should be inserted
    expected = ConnectorDag.builder()
      .addConnection("n1", "n4")
      .addConnection("n1", "n5")
      .addConnection("n2", "n4")
      .addConnection("n2", "n5")
      .addConnection("n3", "n4")
      .addConnection("n3", "n5")
      .addConnection("n4", "n6")
      .addConnection("n5", "n6")
      .addConnection("n5", "n7")
      .addConnection("n5", "n8")
      .addConnection("n6", "n9")
      .addConnection("n7", "n8")
      .addConnection("n7", "n10")
      .addConnection("n8", "n11")
      .build();
    Assert.assertEquals(expected, cdag);
  }

  @Test
  public void testReduceNodeConnectors() {
    /*
             |--- n2
        n1 --|
             |--- n3(r) --- n4
     */
    // n3 is a reduce node, which means n1 is writing to a sink (n2) and a stop node (n3),
    // so n3 should have a connector inserted in front of it
    ConnectorDag cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n1", "n3")
      .addConnection("n3", "n4")
      .addReduceNodes("n3")
      .build();
    cdag.insertConnectors();
    ConnectorDag expected = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n1", "n3.connector")
      .addConnection("n3", "n4")
      .addConnection("n3.connector", "n3")
      .addReduceNodes("n3")
      .addConnectors("n3.connector", "n3")
      .build();
    Assert.assertEquals(expected, cdag);

    /*
             |--- n2
        n1 --|
             |--- n3 --- n4
     */
    // the same graph as above, but n3 is not a reduce node. In this scenario, nothing is a connector
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n1", "n3")
      .addConnection("n3", "n4")
      .build();
    cdag.insertConnectors();
    Assert.assertTrue(cdag.getConnectors().isEmpty());

    /*
        n1 --- n2(r) --- n3(r) --- n4(r) --- n5

        in this example, n3 and n4 should have connectors
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n2", "n3")
      .addConnection("n3", "n4")
      .addConnection("n4", "n5")
      .addReduceNodes("n2", "n3", "n4")
      .build();
    cdag.insertConnectors();
    expected = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n2", "n3.connector")
      .addConnection("n3", "n4.connector")
      .addConnection("n4.connector", "n4")
      .addConnection("n4", "n5")
      .addConnection("n3.connector", "n3")
      .addReduceNodes("n2", "n3", "n4")
      .addConnectors("n3.connector", "n3", "n4.connector", "n4")
      .build();
    Assert.assertEquals(expected, cdag);

    /*
                       |-- n3(r) -- n4
        n1 --- n2(r) --|
                       |-- n5 -- n6(r) -- n7

        in this example, n3 and n6 should have connectors
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n2", "n3")
      .addConnection("n2", "n5")
      .addConnection("n3", "n4")
      .addConnection("n5", "n6")
      .addConnection("n6", "n7")
      .addReduceNodes("n2", "n3", "n6")
      .build();
    cdag.insertConnectors();
    expected = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n2", "n3.connector")
      .addConnection("n2", "n5")
      .addConnection("n3", "n4")
      .addConnection("n5", "n6.connector")
      .addConnection("n6", "n7")
      .addConnection("n3.connector", "n3")
      .addConnection("n6.connector", "n6")
      .addReduceNodes("n2", "n3", "n6")
      .addConnectors("n3.connector", "n3", "n6.connector", "n6")
      .build();
    Assert.assertEquals(expected, cdag);

    /*
             |--- n2(r) ----------|
             |                    |                                    |-- n10
        n1 --|--- n3(r) --- n5 ---|--- n6 --- n7(r) --- n8 --- n9(r) --|
             |                    |                                    |-- n11
             |--- n4(r) ----------|

        in this example, n2, n3, n4, and n9 should all have connectors
        n2, n3, n4 all have connectors because n1 is writing to multiple reduce nodes
        n9 has a connector because it is connected to reduce node n7
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n1", "n3")
      .addConnection("n1", "n4")
      .addConnection("n2", "n6")
      .addConnection("n3", "n5")
      .addConnection("n4", "n6")
      .addConnection("n5", "n6")
      .addConnection("n6", "n7")
      .addConnection("n7", "n8")
      .addConnection("n8", "n9")
      .addConnection("n9", "n10")
      .addConnection("n9", "n11")
      .addReduceNodes("n2", "n3", "n4", "n7", "n9")
      .build();
    cdag.insertConnectors();
    expected = ConnectorDag.builder()
      .addConnection("n1", "n2.connector")
      .addConnection("n1", "n3.connector")
      .addConnection("n1", "n4.connector")
      .addConnection("n2", "n6")
      .addConnection("n3", "n5")
      .addConnection("n4", "n6")
      .addConnection("n5", "n6")
      .addConnection("n6", "n7.connector")
      .addConnection("n7.connector", "n7")
      .addConnection("n7", "n8")
      .addConnection("n8", "n9.connector")
      .addConnection("n9", "n10")
      .addConnection("n9", "n11")
      .addConnection("n2.connector", "n2")
      .addConnection("n3.connector", "n3")
      .addConnection("n4.connector", "n4")
      .addConnection("n9.connector", "n9")
      .addReduceNodes("n2", "n3", "n4", "n7", "n9")
      .addConnectors("n2.connector", "n2", "n3.connector", "n3", "n4.connector", "n4", "n7.connector", "n7",
                     "n9.connector", "n9")
      .build();
    Assert.assertEquals(expected, cdag);
  }

  @Test
  public void testSplitDag() {
    /*
             |--- n2(r) ----------|
             |                    |                                    |-- n10
        n1 --|--- n3(r) --- n5 ---|--- n6 --- n7(r) --- n8 --- n9(r) --|
             |                    |                                    |-- n11
             |--- n4(r) ----------|

        in this example, n2, n3, n4, n7, and n9 should all have connectors
        n2, n3, n4 all have connectors because n1 is writing to multiple reduce nodes
        n7 has a connector because reduce nodes n2, n3, and n4 are connected to it
        n9 has a connector because reduce node n7 is connected to it
     */
    ConnectorDag cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n1", "n3")
      .addConnection("n1", "n4")
      .addConnection("n2", "n6")
      .addConnection("n3", "n5")
      .addConnection("n4", "n6")
      .addConnection("n5", "n6")
      .addConnection("n6", "n7")
      .addConnection("n7", "n8")
      .addConnection("n8", "n9")
      .addConnection("n9", "n10")
      .addConnection("n9", "n11")
      .addReduceNodes("n2", "n3", "n4", "n7", "n9")
      .build();
    cdag.insertConnectors();
    Set<Dag> actual = new HashSet<>(cdag.split());
    // dag_source_sink(s)
    Dag dag1 = new Dag(
      ImmutableSet.of(
        new Connection("n1", "n2.connector"),
        new Connection("n1", "n3.connector"),
        new Connection("n1", "n4.connector")));
    Dag dag2 = new Dag(
      ImmutableSet.of(
        new Connection("n2.connector", "n2"),
        new Connection("n2", "n6"),
        new Connection("n6", "n7.connector")));
    Dag dag3 = new Dag(
      ImmutableSet.of(
        new Connection("n3.connector", "n3"),
        new Connection("n3", "n5"),
        new Connection("n5", "n6"),
        new Connection("n6", "n7.connector")));
    Dag dag4 = new Dag(
      ImmutableSet.of(
        new Connection("n4.connector", "n4"),
        new Connection("n4", "n6"),
        new Connection("n6", "n7.connector")));
    Dag dag5 = new Dag(
      ImmutableSet.of(
        new Connection("n7.connector", "n7"),
        new Connection("n7", "n8"),
        new Connection("n8", "n9.connector")));
    Dag dag6 = new Dag(
      ImmutableSet.of(
        new Connection("n9.connector", "n9"),
        new Connection("n9", "n10"),
        new Connection("n9", "n11")));
    Set<Dag> expected = ImmutableSet.of(dag1, dag2, dag3, dag4, dag5, dag6);
    Assert.assertEquals(expected, actual);


    /*
             |---> n2(r)
             |      |
        n1 --|      |
             |      v
             |---> n3(r) ---> n4

        n2 and n3 should have connectors inserted in front of them to become:

             |---> n2.connector ---> n2(r)
             |                        |
        n1 --|                        |
             |                        v
             |-------------------> n3.connector ---> n3(r) ---> n4
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n1", "n3")
      .addConnection("n2", "n3")
      .addConnection("n3", "n4")
      .addReduceNodes("n2", "n3")
      .build();
    cdag.insertConnectors();
    actual = new HashSet<>(cdag.split());

    /*
             |--> n2.connector
        n1 --|
             |--> n3.connector
     */
    dag1 = new Dag(ImmutableSet.of(
      new Connection("n1", "n2.connector"),
      new Connection("n1", "n3.connector")));
    /*
        n2.connector --> n2 --> n3.connector
     */
    dag2 = new Dag(ImmutableSet.of(
      new Connection("n2.connector", "n2"),
      new Connection("n2", "n3.connector")));
    /*
        n3.connector --> n3 --> n4
     */
    dag3 = new Dag(ImmutableSet.of(
      new Connection("n3.connector", "n3"),
      new Connection("n3", "n4")));
    expected = ImmutableSet.of(dag1, dag2, dag3);
    Assert.assertEquals(expected, actual);


    /*
         n1 --> n2 --|
                     |--> n3(r) --> n4 --|
         n7 --> n8 --|                   |--> n5(r) --> n6
                                         |
         n9 -----------------------------|

        only n5 should have a connector inserted in front of it to become:

         n1 --> n2 --|
                     |--> n3(r) --> n4 --|
         n7 --> n8 --|                   |--> n5.connector --> n5(r) --> n6
                                         |
         n9 -----------------------------|
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n2", "n3")
      .addConnection("n3", "n4")
      .addConnection("n4", "n5")
      .addConnection("n5", "n6")
      .addConnection("n7", "n8")
      .addConnection("n8", "n3")
      .addConnection("n9", "n5")
      .addReduceNodes("n3", "n5")
      .build();
    cdag.insertConnectors();
    actual = new HashSet<>(cdag.split());

    /*
         n1 --> n2 --|
                     |--> n3(r) --> n4 --|
         n7 --> n8 --|                   |--> n5.connector
     */
    dag1 = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n5.connector"),
      new Connection("n7", "n8"),
      new Connection("n8", "n3")));
    /*
                                         |--> n5.connector
                                         |
         n9 -----------------------------|
     */
    dag2 = new Dag(ImmutableSet.of(new Connection("n9", "n5.connector")));
    /*
         n5.connector --> n5(r) --> n6
     */
    dag3 = new Dag(ImmutableSet.of(
      new Connection("n5.connector", "n5"),
      new Connection("n5", "n6")));
    expected = ImmutableSet.of(dag1, dag2, dag3);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSplitDagWithMultiReduces() {
    /*
   n1 --|
        |--- n3(r) ---|
   n2 --|             |-- n5(r) --|
          n4 ---------|           |
                                  |-- n7(r) -- n8
                                  |
                      n6 ---------|
                                  |
                      n9 ---------|
     */
    ConnectorDag cdag = ConnectorDag.builder()
      .addConnection("n1", "n3")
      .addConnection("n2", "n3")
      .addConnection("n3", "n5")
      .addConnection("n4", "n5")
      .addConnection("n5", "n7")
      .addConnection("n6", "n7")
      .addConnection("n9", "n7")
      .addConnection("n7", "n8")
      .addReduceNodes("n3", "n5", "n7")
      .build();
    cdag.insertConnectors();
    Set<Dag> actual = new HashSet<>(cdag.split());
    Dag dag1 = new Dag(
      ImmutableSet.of(
        new Connection("n1", "n3"),
        new Connection("n2", "n3"),
        new Connection("n3", "n5.connector")));
    Dag dag2 = new Dag(
      ImmutableSet.of(
        new Connection("n4", "n5.connector")));
    Dag dag3 = new Dag(
      ImmutableSet.of(
        new Connection("n5.connector", "n5"),
        new Connection("n5", "n7.connector")));
    Dag dag4 = new Dag(
      ImmutableSet.of(
        new Connection("n6", "n7.connector"),
        new Connection("n9", "n7.connector")));
    Dag dag5 = new Dag(
      ImmutableSet.of(
        new Connection("n7.connector", "n7"),
        new Connection("n7", "n8")));
    Set<Dag> expected = ImmutableSet.of(dag1, dag2, dag3, dag4, dag5);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testIsolateNoOp() {
    /*
        n1(i) --> n2(i)
     */
    ConnectorDag cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addIsolationNodes("n1", "n2")
      .build();
    Assert.assertTrue(cdag.insertConnectors().isEmpty());

    /*
        n1 --> n2(i) --> n3
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n2", "n3")
      .addIsolationNodes("n2")
      .build();
    Assert.assertTrue(cdag.insertConnectors().isEmpty());

    /*
                       |--> n3
        n1 --> n2(i) --|
                       |--> n4
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n2", "n3")
      .addConnection("n2", "n4")
      .addIsolationNodes("n2")
      .build();
    Assert.assertTrue(cdag.insertConnectors().isEmpty());
  }

  @Test
  public void testIsolate() {
    /*
             |--> n2(i) --|
        n1 --|            |--> n4
             |--> n3(i) --|
     */
    ConnectorDag cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n1", "n3")
      .addConnection("n2", "n4")
      .addConnection("n3", "n4")
      .addIsolationNodes("n2", "n3")
      .build();
    cdag.insertConnectors();
    /*
             |--> n2.connector --> n2(i) --|
        n1 --|                             |--> n4.connector --> n4
             |--> n3.connector --> n3(i) --|
     */
    ConnectorDag expected = ConnectorDag.builder()
      .addConnection("n1", "n2.connector")
      .addConnection("n1", "n3.connector")
      .addConnection("n2.connector", "n2")
      .addConnection("n2", "n4.connector")
      .addConnection("n3.connector", "n3")
      .addConnection("n3", "n4.connector")
      .addConnection("n4.connector", "n4")
      .addIsolationNodes("n2", "n3")
      .addConnectors("n2.connector", "n2", "n3.connector", "n3", "n4.connector", "n4")
      .build();
    Assert.assertEquals(expected, cdag);


    /*
             |--> n2(i) --> n3 --|
        n1 --|                   |--> n4
             |-------------------|
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n1", "n4")
      .addConnection("n2", "n3")
      .addConnection("n3", "n4")
      .addIsolationNodes("n2")
      .build();
    cdag.insertConnectors();
    /*
             |--> n2.connector --> n2(i) --> n3.connector --> n3 --|
        n1 --|                                                     |----> n4
             |-----------------------------------------------------|
     */
    expected = ConnectorDag.builder()
      .addConnection("n1", "n2.connector")
      .addConnection("n1", "n4")
      .addConnection("n2.connector", "n2")
      .addConnection("n2", "n3.connector")
      .addConnection("n3.connector", "n3")
      .addConnection("n3", "n4")
      .addIsolationNodes("n2")
      .addConnectors("n2.connector", "n2", "n3.connector", "n3")
      .build();
    Assert.assertEquals(expected, cdag);

    /*
        n1 --> n2(i) --> n3(i) --> n4
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n2", "n3")
      .addConnection("n3", "n4")
      .addIsolationNodes("n2", "n3")
      .build();
    cdag.insertConnectors();
    /*
        n1 --> n2(i) --> n3.connector --> n3(i) --> n4
     */
    expected = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n2", "n3.connector")
      .addConnection("n3.connector", "n3")
      .addConnection("n3", "n4")
      .addIsolationNodes("n2", "n3")
      .addConnectors("n3.connector", "n3")
      .build();
    Assert.assertEquals(expected, cdag);

    /*
             |--> n2(r) --> n3(i) --|
        n1 --|                      |--> n6(r) --> n7
             |--> n4(i) --> n5(r) --|
     */
    cdag = ConnectorDag.builder()
      .addConnection("n1", "n2")
      .addConnection("n1", "n4")
      .addConnection("n2", "n3")
      .addConnection("n3", "n6")
      .addConnection("n4", "n5")
      .addConnection("n5", "n6")
      .addConnection("n6", "n7")
      .addReduceNodes("n2", "n5", "n6")
      .addIsolationNodes("n3", "n4")
      .build();
    cdag.insertConnectors();
    /*
             |--> n2.connector --> n2(r) --> n3.connector --> n3(i) --|
        n1 --|                                                        |--> n6.connector --> n6(r) --> n7
             |--> n4.connector --> n4(i) --> n5.connector --> n5(r) --|
     */
    expected = ConnectorDag.builder()
      .addConnection("n1", "n2.connector")
      .addConnection("n1", "n4.connector")
      .addConnection("n2.connector", "n2")
      .addConnection("n2", "n3.connector")
      .addConnection("n3.connector", "n3")
      .addConnection("n3", "n6.connector")
      .addConnection("n4.connector", "n4")
      .addConnection("n4", "n5.connector")
      .addConnection("n5.connector", "n5")
      .addConnection("n5", "n6.connector")
      .addConnection("n6.connector", "n6")
      .addConnection("n6", "n7")
      .addReduceNodes("n2", "n5", "n6")
      .addIsolationNodes("n3", "n4")
      .addConnectors("n2.connector", "n2", "n3.connector", "n3", "n4.connector", "n4",
                     "n5.connector", "n5", "n6.connector", "n6")
      .build();
    Assert.assertEquals(expected, cdag);
  }

  @Test
  public void testSubdagMerge() throws Exception {
    /*
        n1 -----|
                |-- n4(r) -- n6
             |--|
        n2 --|
             |--|
                |-- n5(r) -- n7
        n3 -----|
     */
    ConnectorDag cdag = ConnectorDag.builder()
      .addConnection("n1", "n4")
      .addConnection("n2", "n4")
      .addConnection("n2", "n5")
      .addConnection("n3", "n5")
      .addConnection("n4", "n6")
      .addConnection("n5", "n7")
      .addReduceNodes("n4", "n5")
      .build();
    Assert.assertEquals(ImmutableSet.of("n4", "n5"), cdag.insertConnectors());

    /*
        n1 -----|
                |-- n4.connector
             |--|
        n2 --|
             |--|
                |-- n5.connector
        n3 -----|
     */
    Dag dag1 = new Dag(ImmutableSet.of(
      new Connection("n1", "n4.connector"),
      new Connection("n2", "n4.connector"),
      new Connection("n2", "n5.connector"),
      new Connection("n3", "n5.connector")));

    /*
       n4.connector -- n4 -- n6
     */
    Dag dag2 = new Dag(ImmutableSet.of(new Connection("n4.connector", "n4"), new Connection("n4", "n6")));

    /*
       n5.connector -- n5 -- n7
     */
    Dag dag3 = new Dag(ImmutableSet.of(new Connection("n5.connector", "n5"), new Connection("n5", "n7")));

    Set<Dag> expected = ImmutableSet.of(dag1, dag2, dag3);
    Set<Dag> actual = new HashSet<>(cdag.split());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleCondition() throws Exception {

    /*
      file - csv - condition - sink1
                      |
                      |-------sink2
     */

    Set<Connection> connections = ImmutableSet.of(
      new Connection("file", "csv"),
      new Connection("csv", "condition"),
      new Connection("condition", "sink1"),
      new Connection("condition", "sink2")
    );

    Set<String> conditions = new HashSet<>(Arrays.asList("condition"));
    Set<String> reduceNodes = new HashSet<>();
    Set<String> isolationNodes = new HashSet<>();
    Set<Dag> actual = PipelinePlanner.split(connections, conditions, reduceNodes, isolationNodes, EMPTY_ACTIONS,
                                            EMPTY_CONNECTORS);

    Dag dag1 = new Dag(ImmutableSet.of(
      new Connection("file", "csv"),
      new Connection("csv", "condition")));
    Dag dag2 = new Dag(ImmutableSet.of(
      new Connection("condition", "sink1")));
    Dag dag3 = new Dag(ImmutableSet.of(
      new Connection("condition", "sink2")));

    Set<Dag> expected = ImmutableSet.of(dag1, dag2, dag3);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testSimpleConditionWithReducers() throws Exception {
    /*
             |--- n2
        n1 --|
             |--- n3(r) --- n4---condition----n5
                                    |
                                    |---------n6
    */

    Set<Connection> connections = ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "condition"),
      new Connection("condition", "n5"),
      new Connection("condition", "n6")
    );

    Set<String> conditions = new HashSet<>(Arrays.asList("condition"));
    Set<String> reduceNodes = new HashSet<>(Arrays.asList("n3"));
    Set<String> isolationNodes = new HashSet<>();
    Set<Dag> actual = PipelinePlanner.split(connections, conditions, reduceNodes, isolationNodes, EMPTY_ACTIONS,
                                            EMPTY_CONNECTORS);

    Dag dag1 = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n3.connector")));
    Dag dag2 = new Dag(ImmutableSet.of(
      new Connection("n3.connector", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "condition")));
    Dag dag3 = new Dag(ImmutableSet.of(
      new Connection("condition", "n5")));
    Dag dag4 = new Dag(ImmutableSet.of(
      new Connection("condition", "n6")));
    Set<Dag> expected = ImmutableSet.of(dag1, dag2, dag3, dag4);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testSimpleConditionWithMultipleSources() throws Exception {
    /*
             |--- n2
        n1 --|
             |--- n3(r) --- n4---condition----n5
                    |                |
        n11---------|                |---------n6
    */

    Set<Connection> connections = ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "condition"),
      new Connection("condition", "n5"),
      new Connection("condition", "n6"),
      new Connection("n11", "n3")
    );

    Set<String> conditions = new HashSet<>(Arrays.asList("condition"));
    Set<String> reduceNodes = new HashSet<>(Arrays.asList("n3"));
    Set<String> isolationNodes = new HashSet<>();
    Set<Dag> actual = PipelinePlanner.split(connections, conditions, reduceNodes, isolationNodes, EMPTY_ACTIONS,
                                            EMPTY_CONNECTORS);

    Dag dag1 = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n3.connector"),
      new Connection("n11", "n3.connector")));
    Dag dag2 = new Dag(ImmutableSet.of(
      new Connection("n3.connector", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "condition")));
    Dag dag3 = new Dag(ImmutableSet.of(
      new Connection("condition", "n5")));
    Dag dag4 = new Dag(ImmutableSet.of(
      new Connection("condition", "n6")));
    Set<Dag> expected = ImmutableSet.of(dag1, dag2, dag3, dag4);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testConditionDag() throws Exception {

    /*
          file - csv - c1 - t1---agg1--agg2---sink1
                        |
                        ----c2 - sink2
                             |
                              ------c3 - sink3

     */

    Set<Connection> connections = ImmutableSet.of(
      new Connection("file", "csv"),
      new Connection("csv", "c1"),
      new Connection("c1", "t1"),
      new Connection("t1", "agg1"),
      new Connection("agg1", "agg2"),
      new Connection("agg2", "sink1"),
      new Connection("c1", "c2"),
      new Connection("c2", "sink2"),
      new Connection("c2", "c3"),
      new Connection("c3", "sink3")
    );

    Set<String> conditions = new HashSet<>(Arrays.asList("c1", "c2", "c3"));
    Set<String> reduceNodes = new HashSet<>(Arrays.asList("agg1", "agg2"));
    Set<String> isolationNodes = new HashSet<>();
    Set<Dag> actual = PipelinePlanner.split(connections, conditions, reduceNodes, isolationNodes, EMPTY_ACTIONS,
                                            EMPTY_CONNECTORS);

    Dag dag1 = new Dag(
      ImmutableSet.of(
        new Connection("file", "csv"),
        new Connection("csv", "c1")));
    Dag dag2 = new Dag(
      ImmutableSet.of(
        new Connection("c1", "t1"),
        new Connection("t1", "agg1"),
        new Connection("agg1", "agg2.connector")));
    Dag dag3 = new Dag(
      ImmutableSet.of(
        new Connection("agg2.connector", "agg2"),
        new Connection("agg2", "sink1")));
    Dag dag4 = new Dag(
      ImmutableSet.of(
        new Connection("c1", "c2")));
    Dag dag5 = new Dag(
      ImmutableSet.of(
        new Connection("c2", "sink2")));
    Dag dag6 = new Dag(
      ImmutableSet.of(
        new Connection("c2", "c3")));
    Dag dag7 = new Dag(
      ImmutableSet.of(
        new Connection("c3", "sink3")));

    Set<Dag> expected = ImmutableSet.of(dag1, dag2, dag3, dag4, dag5, dag6, dag7);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testMultipleNonNestedConditions() throws Exception {
    /*
       n1-c1-n2-n3-c2-n4
     */
    Set<Connection> connections = ImmutableSet.of(
      new Connection("n1", "c1"),
      new Connection("c1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "c2"),
      new Connection("c2", "n4")
    );
    Set<String> conditions = new HashSet<>(Arrays.asList("c1", "c2"));
    Set<String> reduceNodes = new HashSet<>();
    Set<String> isolationNodes = new HashSet<>();
    Set<Dag> actual = PipelinePlanner.split(connections, conditions, reduceNodes, isolationNodes, EMPTY_ACTIONS,
                                            EMPTY_CONNECTORS);

    Dag dag1 = new Dag(
      ImmutableSet.of(
        new Connection("n1", "c1")));
    Dag dag2 = new Dag(
      ImmutableSet.of(
        new Connection("c1", "n2"),
        new Connection("n2", "n3"),
        new Connection("n3", "c2")));
    Dag dag3 = new Dag(
      ImmutableSet.of(
        new Connection("c2", "n4")));

    Set<Dag> expected = ImmutableSet.of(dag1, dag2, dag3);
    Assert.assertEquals(actual, expected);
  }
}
