/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans

import org.neo4j.cypher.internal.frontend.v3_2.ast.PatternExpression
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps.allNodesLeafPlanner
import org.neo4j.cypher.internal.compiler.v3_2.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_2.{IdName, QueryGraph}

class AllNodesLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  test("simple all nodes scan") {
    // given
    val queryGraph = QueryGraph(patternNodes = Set(IdName("n")))

    implicit val planContext = newMockedPlanContext
    implicit val context = newMockedLogicalPlanningContext(
      planContext = planContext,
      metrics = newMockedMetricsFactory.newMetrics(hardcodedStatistics, config))

    // when
    val resultPlans = allNodesLeafPlanner(queryGraph)

    // then
    resultPlans should equal(Seq(AllNodesScan(IdName("n"), Set.empty)(solved)))
  }
}
