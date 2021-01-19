/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class DebugAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  test("CYPHER DEBUG=rawCardinalities should output estimated rows with float precision") {
    graph.createIndex("Node", "prop")
    for (i <- 0 until 20) {
      createLabeledNode(Map("prop" -> i), "Node")
    }

    val limit = 10
    val query =
      s"""CYPHER DEBUG=rawCardinalities MATCH (n), (m) RETURN n.prop ORDER BY n.prop LIMIT $limit""".stripMargin

    val cartesianProductPlanDescription =
      executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
      .executionPlanDescription()
      .find("CartesianProduct").head

    val expected =
      """
        |+-------------------+--------------------+--------------------+------------+
        || Operator          | Details            | Estimated Rows     | Ordered by |
        |+-------------------+--------------------+--------------------+------------+
        || +CartesianProduct |                    |               10.0 | n.prop ASC |
        || |\                +--------------------+--------------------+------------+
        || | +AllNodesScan   | m                  | 3.1622776601683795 |            |
        || |                 +--------------------+--------------------+------------+
        || +Sort             | `n.prop` ASC       | 3.1622776601683795 | n.prop ASC |
        || |                 +--------------------+--------------------+------------+
        || +Projection       | n.prop AS `n.prop` |               20.0 |            |
        || |                 +--------------------+--------------------+------------+
        || +AllNodesScan     | n                  |               20.0 |            |
        |+-------------------+--------------------+--------------------+------------+
        |
        |Total database accesses: ?
        |""".stripMargin

    cartesianProductPlanDescription.toString shouldEqual expected
  }
}
