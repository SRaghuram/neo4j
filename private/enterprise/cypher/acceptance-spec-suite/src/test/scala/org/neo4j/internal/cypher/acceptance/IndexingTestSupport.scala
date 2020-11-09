/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util.stream.Collectors

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.TestConfiguration
import org.neo4j.values.storable.Value

import scala.collection.JavaConverters.mapAsJavaMapConverter

trait IndexingTestSupport extends ExecutionEngineFunSuite with CypherComparisonSupport {

  protected val LABEL: String = "Label"
  protected val PROPERTY: String = "prop"
  protected val NONINDEXED: String = "nonindexed"

  def cypherComparisonSupport: Boolean

  protected def createIndex(): Unit = {
    graph.createIndex(LABEL, PROPERTY)
  }

  protected def dropIndex(): Unit = {
    graph.withTx( tx => tx.execute(s"DROP INDEX ON :$LABEL($PROPERTY)"))
  }

  protected def createIndexedNode(value: Value): Node = {
    createLabeledNode(Map(PROPERTY -> value.asObject()), LABEL)
  }

  protected def setIndexedValue(node: Node, value: Value): Unit = {
    graph.withTx( tx =>  {
      tx.getNodeById(node.getId).setProperty(PROPERTY, value.asObject())
    } )
  }

  protected def setNonIndexedValue(node: Node, value: Value): Unit = {
    graph.withTx( tx => {
      tx.getNodeById(node.getId).setProperty(NONINDEXED, value.asObject())
    } )
  }

  protected def assertSeekMatchFor(value: Value, nodes: Node*): Unit = {
    val query = s"MATCH (n:$LABEL) WHERE n.$PROPERTY = $$param RETURN n"
    testRead(query, Map("param" -> value.asObject()), "NodeIndexSeek", nodes, config = Configs.All)
  }

  protected def assertScanMatch(nodes: Node*): Unit = {
    val query = s"MATCH (n:$LABEL) WHERE n.$PROPERTY IS NOT NULL RETURN n"
    testRead(query, Map(), "NodeIndexScan", nodes)
  }

  protected def assertRangeScanFor(op: String, bound: Value, nodes: Node*): Unit = {
    val predicate = s"n.$PROPERTY $op $$param"
    val query = s"MATCH (n:$LABEL) WHERE $predicate RETURN n"
    testRead(query, Map("param" -> bound.asObject()), "NodeIndexSeekByRange", nodes)
  }

  protected def assertLabelRangeScanFor(op: String, bound: Value, nodes: Node*): Unit = {
    val predicate = s"n.$NONINDEXED $op $$param"
    val query = s"MATCH (n:$LABEL) WHERE $predicate RETURN n"
    testRead(query, Map("param" -> bound.asObject()), "NodeByLabelScan", nodes)
  }

  protected def assertRangeScanFor(op1: String, bound1: Value, op2: String, bound2: Value, nodes: Node*): Unit = {
    val predicate1 = s"n.$PROPERTY $op1 $$param1"
    val predicate2 = s"n.$PROPERTY $op2 $$param2"
    val query = s"MATCH (n:$LABEL) WHERE $predicate1 AND $predicate2 RETURN n"
    testRead(query, Map("param1" -> bound1.asObject(), "param2" -> bound2.asObject()), "NodeIndexSeekByRange", nodes)
  }

  private def testRead(query: String, params: Map[String, AnyRef], wantedOperator: String, expected: Seq[Node],
                       config: TestConfiguration = Configs.InterpretedAndSlottedAndPipelined): Unit = {
    if (cypherComparisonSupport) {
      val result =
        executeWith(
          config,
          query,
          params = params,
          planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan(wantedOperator))
        )
      val nodes = result.columnAs[Node]("n").toSet
      expected.foreach(p => assert(nodes.contains(p)))
      nodes.size should be(expected.size)
    } else {
      graph.withTx( tx => {
        val result = tx.execute("CYPHER runtime=slotted "+query, params.asJava)
        val nodes = result.columnAs("n").stream().collect(Collectors.toSet)
        expected.foreach(p => assert(nodes.contains(p)))
        nodes.size() should be(expected.size)

        val plan = result.getExecutionPlanDescription.toString
        plan should include(wantedOperator)
      })
    }
  }
}
