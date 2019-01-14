/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.compatibility.v3_5.runtime

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast.ReferenceFromSlot
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.runtime.planDescription.{NoChildren, PlanDescriptionImpl, renderAsTreeTable}
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.test_helpers.{CypherFunSuite, WindowsStringSafe}

class ExecutionPlanDescriptionTest extends CypherFunSuite {
  implicit val windowsSafe = WindowsStringSafe

  test("use variable name instead of ReferenceFromSlot") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      Expression(ReferenceFromSlot(42, "  id@23")),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(Id.INVALID_ID, "NAME", NoChildren, arguments, Set("  n@76"))

    val details = renderAsTreeTable(plan)
    details should equal(
      """+----------+----------------+------+---------+-----------+-------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other |
        |+----------+----------------+------+---------+-----------+-------+
        || +NAME    |              1 |   42 |      33 | n         | id    |
        |+----------+----------------+------+---------+-----------+-------+
        |""".stripMargin)
  }

}