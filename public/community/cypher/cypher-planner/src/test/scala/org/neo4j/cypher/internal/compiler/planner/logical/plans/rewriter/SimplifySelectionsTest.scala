/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.planner.FakePlan
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.DummyExpression
import org.neo4j.cypher.internal.logical.plans.DropResult
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SimplifySelectionsTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should rewrite Selection(false, source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)
    val selection = Selection(Seq(falseLiteral), source)

    selection.endoRewrite(simplifySelections) should equal(
      DropResult( source))
  }

  test("should rewrite Selection(true, source) to source") {
    val source: LogicalPlan = FakePlan(Set.empty)
    val selection = Selection(Seq(trueLiteral), source)

    selection.endoRewrite(simplifySelections) should equal(source)
  }

  test("should not rewrite plans not obviously true or false") {
    val source: LogicalPlan = FakePlan(Set.empty)
    val selection = Selection(Seq(DummyExpression(CTAny)), source)

    selection.endoRewrite(simplifySelections) should equal(selection)
  }
}
