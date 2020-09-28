/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.lang.Boolean.FALSE
import java.lang.Boolean.TRUE

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions.ExhaustiveShortestPathForbiddenException
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.impl.notification.NotificationCode.EXHAUSTIVE_SHORTEST_PATH
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

import scala.collection.mutable

class ShortestPathExhaustiveForbiddenAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++
    Map(
      GraphDatabaseSettings.cypher_hints_error -> FALSE,
      GraphDatabaseSettings.forbid_exhaustive_shortestpath -> TRUE)

  test("should fail at run time when using the shortest path fallback") {
    // when
    failWithError(Configs.All,
      s"""MATCH p = shortestPath((src:$topLeft)-[*0..]-(dst:$topLeft))
         |WHERE ANY(n in nodes(p) WHERE n:$topRight)
         |RETURN nodes(p) AS nodes""".stripMargin,
      ExhaustiveShortestPathForbiddenException.ERROR_MSG
    )
  }

  test("should warn if shortest path fallback is planned") {
    // when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      s"""EXPLAIN MATCH p = shortestPath((src:$topLeft)-[*0..]-(dst:$topLeft))
         |WHERE ANY(n in nodes(p) WHERE n:$topRight)
         |RETURN nodes(p) AS nodes""".stripMargin)

    // then
    result.notifications.toSeq should equal(
      Seq(EXHAUSTIVE_SHORTEST_PATH.notification(new org.neo4j.graphdb.InputPosition(58, 1, 59))
      )
    )
  }

  val dim = 4
  val dMax = dim - 1
  val topLeft = "CELL00"
  val topRight = s"CELL0$dMax"
  val bottomLeft = s"CELL${dMax}0"
  val bottomRight = s"CELL$dMax$dMax"
  val middle = s"CELL${dMax / 2}${dMax / 2}"
  val nodesByName: mutable.Map[String, Node] = mutable.Map[String, Node]()

  override protected def initTest(): Unit = {
    super.initTest()
    0 to dMax foreach { row =>
      0 to dMax foreach { col =>
        val name = s"$row$col"
        val node = createLabeledNode(Map("name" -> name, "row" -> row, "col" -> col), s"CELL$row$col", s"ROW$row",
          s"COL$col")
        nodesByName(name) = node
        if (row > 0) {
          relate(nodesByName(s"${row - 1}$col"), nodesByName(name), "DOWN", s"r${row - 1}-${row}c$col")
        }
        if (col > 0) {
          relate(nodesByName(s"$row${col - 1}"), nodesByName(name), "RIGHT", s"r${row}c${col - 1}$col")
        }
      }
    }
  }
}
