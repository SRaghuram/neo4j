/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekByRange
import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.{Node, ResourceIterator}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class StartsWithImplementationAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  var aNode: Node = null
  var bNode: Node = null
  var cNode: Node = null
  var dNode: Node = null
  var eNode: Node = null
  var fNode: Node = null

  override def initTest() {
    super.initTest()
    aNode = createLabeledNode(Map("name" -> "ABCDEF"), "LABEL")
    bNode = createLabeledNode(Map("name" -> "AB"), "LABEL")
    cNode = createLabeledNode(Map("name" -> "abcdef"), "LABEL")
    dNode = createLabeledNode(Map("name" -> "ab"), "LABEL")
    eNode = createLabeledNode(Map("name" -> ""), "LABEL")
    fNode = createLabeledNode("LABEL")
  }

  test("should not plan an IndexSeek when index doesn't exist") {

    (1 to 10).foreach { _ =>
      createLabeledNode("Address")
    }

    createLabeledNode(Map("prop" -> "www123"), "Address")
    createLabeledNode(Map("prop" -> "www"), "Address")

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (a:Address) WHERE a.prop STARTS WITH 'www' RETURN a")

    result.executionPlanDescription() should not(includeSomewhere.aPlan(IndexSeekByRange.name))
  }

  test("Should handle prefix search with existing transaction state") {
    graph.createIndex("User", "name")
    graph.inTx {
      createLabeledNode(Map("name" -> "Stefan"), "User")
      createLabeledNode(Map("name" -> "Stephan"), "User")
      createLabeledNode(Map("name" -> "Stefanie"), "User")
      createLabeledNode(Map("name" -> "Craig"), "User")
    }
      val executeBefore = () => {
        drain(graph.execute("MATCH (u:User {name: 'Craig'}) SET u.name = 'Steven'"))
        drain(graph.execute("MATCH (u:User {name: 'Stephan'}) DELETE u"))
        drain(graph.execute("MATCH (u:User {name: 'Stefanie'}) SET u.name = 'steffi'"))
      }

      executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (u:User) WHERE u.name STARTS WITH 'Ste' RETURN u.name as name", executeBefore = executeBefore,
        resultAssertionInTx = Some(result => {
          result.toSet should equal(Set(Map("name" -> "Stefan"),Map("name" -> "Steven")))
        }))
  }

  private def drain(iter: ResourceIterator[_]): Unit = {
    try {
      while (iter.hasNext) iter.next()
    } finally {
      iter.close()
    }
  }
}
