/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.plandescription.Arguments
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.NoChildren
import org.neo4j.cypher.internal.plandescription.PlanDescriptionImpl
import org.neo4j.cypher.internal.plandescription.PrettyStringCreator
import org.neo4j.cypher.internal.plandescription.renderAsTreeTable
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class ExecutionPlanDescriptionTest extends CypherFunSuite {
  implicit val windowsSafe = WindowsStringSafe

  test("use variable name instead of ReferenceFromSlot") {

    val arguments = Seq(
      Arguments.Rows(42),
      DbHits(33),
      Details(PrettyStringCreator.raw("id")),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(Id.INVALID_ID, "NAME", NoChildren, arguments, Set("  n@76"))

    val details = renderAsTreeTable(plan)
    details should equal(
      """+----------+---------+----------------+------+---------+
        || Operator | Details | Estimated Rows | Rows | DB Hits |
        |+----------+---------+----------------+------+---------+
        || +NAME    | id      |              1 |   42 |      33 |
        |+----------+---------+----------------+------+---------+
        |""".stripMargin)
  }

}
