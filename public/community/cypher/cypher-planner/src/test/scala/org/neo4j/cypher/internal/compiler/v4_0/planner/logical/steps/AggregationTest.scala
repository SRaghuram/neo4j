/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v4_0.planner._
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.ir.v4_0.{AggregatingQueryProjection, InterestingOrder}
import org.neo4j.cypher.internal.v4_0.logical.plans.{Aggregation, LogicalPlan, Projection}
import org.neo4j.cypher.internal.v4_0.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class AggregationTest extends CypherFunSuite with LogicalPlanningTestSupport with PlanMatchHelp {
  val aggregatingMap: Map[String, Expression] = Map("count(*)" -> CountStar()(pos))

  val propExp: Expression = Property(varFor("n"), PropertyKeyName("prop")(pos))(pos)
  val countExp: Expression = FunctionInvocation(FunctionName("count")(pos), propExp)(pos)
  val aggregatingMap2: Map[String, Expression] = Map("count(n.prop)" -> countExp)

  val propExp2: Expression = Property(varFor("n"), PropertyKeyName("bar")(pos))(pos)
  val groupingMap: Map[String, Expression] = Map("n.bar" -> propExp2)

  test("should introduce aggregation when needed") {
    val projection = AggregatingQueryProjection(
      groupingExpressions = Map.empty,
      aggregationExpressions = aggregatingMap
    )

    val context = newMockedLogicalPlanningContextWithFakeAttributes(
      planContext = newMockedPlanContext
    )
    val startPlan = newMockedLogicalPlan()

    val result = aggregation(startPlan, projection, InterestingOrder.empty, context)
    result should equal(
      Aggregation(startPlan, Map(), aggregatingMap)
    )
  }

  test("should introduce variables when needed") {
    //match (n) return n.y, sum(n.x)
    val projectionPlan = AggregatingQueryProjection(
      groupingExpressions = groupingMap,
      aggregationExpressions = aggregatingMap2
    )

    val context = newMockedLogicalPlanningContextWithFakeAttributes(
      planContext = newMockedPlanContext
    )

    val startPlan = newMockedLogicalPlan()

    val result = aggregation(startPlan, projectionPlan, InterestingOrder.empty, context)
    result should equal(
      Aggregation(
       startPlan, groupingMap, aggregatingMap2)
    )
  }

  test("RETURN x.prop, count(*) => WITH x.prop as `x.prop` RETURN `x.prop`, count(*)") {
    // Given RETURN x.prop, count(*) => WITH x.prop as `x.prop` RETURN `x.prop`, count(*)
    val groupingMap = Map("x.prop" -> Property(Variable("x")(pos), PropertyKeyName("prop")(pos))(pos))
    val groupingKeyMap = Map("x.prop" -> Variable("x.prop")(pos))
    val projection = AggregatingQueryProjection(
      groupingExpressions = groupingKeyMap,
      aggregationExpressions = aggregatingMap
    )

    val context = newMockedLogicalPlanningContextWithFakeAttributes(
      planContext = newMockedPlanContext
    )

    val startPlan = newMockedLogicalPlan()

    val projectionPlan: LogicalPlan = Projection(startPlan, groupingMap)

    // When
    val result = aggregation(projectionPlan, projection, InterestingOrder.empty, context)
    // Then
    result should equal(
      Aggregation(projectionPlan, groupingKeyMap, aggregatingMap)
    )
  }
}
