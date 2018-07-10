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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.ast
import org.opencypher.v9_0.ast.{AscSortItem, SortItem}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.InternalException
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class SortSkipAndLimitTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val x: Expression = UnsignedDecimalIntegerLiteral("110") _
  val y: Expression = UnsignedDecimalIntegerLiteral("10") _
  val sortVariable = Variable("n")(pos)
  val variableSortItem: AscSortItem = ast.AscSortItem(sortVariable) _
  val columnOrder: ColumnOrder = Ascending("n")

  private val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  test("should add skip if query graph contains skip") {
    // given
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWithRegularProjection(skip = Some(x))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Skip(startPlan, x))
    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(skip = Some(x))))
  }

  test("should add limit if query graph contains limit") {
    // given
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWithRegularProjection(limit = Some(x))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Limit(startPlan, x, DoNotIncludeTies))
    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(limit = Some(x))))
  }

  test("should add skip first and then limit if the query graph contains both") {
    // given
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWithRegularProjection(skip = Some(y), limit = Some(x))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Limit(Skip(startPlan, y), x, DoNotIncludeTies))
    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map.empty, QueryShuffle(limit = Some(x), skip = Some(y))))
  }

  test("should add sort if query graph contains sort items") {
    // given
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWithRegularProjection(sortItems = Seq(variableSortItem))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Sort(startPlan, Seq(columnOrder)))

    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(sortVariable.name -> sortVariable), QueryShuffle(sortItems = Seq(variableSortItem))))
  }

  test("should add sort and pre-projection") {
    val mSortVar = Variable("m")(pos)
    val mSortItem = ast.AscSortItem(mSortVar)(pos)
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWithRegularProjection(
      // The requirement to sort by m
      sortItems = Seq(mSortItem),
      projectionsMap = Map(
        // an already solved projection
        sortVariable.name -> sortVariable,
        // a projection necessary for the sorting
        mSortVar.name -> mSortVar,
        // and a projection that sort will not take care of
        "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos)))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Sort(Projection(startPlan, Map("m" -> mSortVar)), Seq(Ascending("m"))))

    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(mSortVar.name -> mSortVar), QueryShuffle(sortItems = Seq(mSortItem))))
  }

  test("should add sort and pre-projection for expressions") {
    val sortExpression = Add(sortVariable, SignedDecimalIntegerLiteral("5")(pos))(pos)
    val sortItem = ast.AscSortItem(sortExpression)(pos)
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWithRegularProjection(
      // The requirement to sort by m + 5
      sortItems = Seq(sortItem),
      projectionsMap = Map(
        // an already solved projection
        sortVariable.name -> sortVariable,
        // and a projection that sort will not take care of
        "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos)))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Sort(Projection(startPlan, Map("  FRESHID0" -> sortExpression)), Seq(Ascending("  FRESHID0"))))

    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(sortVariable.name -> sortVariable), QueryShuffle(sortItems = Seq(sortItem))))
  }

  test("should add sort and two step pre-projection for expressions") {
    val mVar = Variable("m")(pos)
    val mExpr = Add(sortVariable, SignedDecimalIntegerLiteral("10")(pos))(pos)
    val sortExpression = Add(mVar, SignedDecimalIntegerLiteral("5")(pos))(pos)
    val sortItem = ast.AscSortItem(sortExpression)(pos)
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWithRegularProjection(
      // The requirement to sort by m + 5
      sortItems = Seq(sortItem),
      projectionsMap = Map(
        // an already solved projection
        sortVariable.name -> sortVariable,
        // a projection necessary for the sorting
        mVar.name -> mExpr,
        // and a projection that sort will not take care of
        "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos)))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Sort(Projection(Projection(startPlan, Map(mVar.name -> mExpr)), Map("  FRESHID0" -> sortExpression)), Seq(Ascending("  FRESHID0"))))

    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(mVar.name -> mExpr), QueryShuffle(sortItems = Seq(sortItem))))
  }

  test("should fail if sort by unknown variable") {
    val mSortVar = Variable("m")(pos)
    val mSortItem = ast.AscSortItem(mSortVar)(pos)
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWithRegularProjection(
      // The requirement to sort by m
      sortItems = Seq(mSortItem),
      projectionsMap = Map(
        // an already solved projection
        sortVariable.name -> sortVariable,
        // and a projection that sort will not take care of
        "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos)))

    // when
    val exception = intercept[InternalException] {
      sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)
    }

    // then
    exception.getMessage should equal("Found unknown variable in sort items: m")
  }

  test("should add sort without pre-projection for DistinctQueryProjection") {
    val mSortVar = Variable("m")(pos)
    val mSortItem = ast.AscSortItem(mSortVar)(pos)

    val projectionsMap = Map(
      sortVariable.name -> sortVariable,
      mSortVar.name -> mSortVar,
      // a projection that sort will not take care of
      "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos))
    val sortItems = Seq(mSortItem)
    val projection = DistinctQueryProjection(
      groupingKeys = projectionsMap,
      shuffle = QueryShuffle(sortItems, skip = None, limit = None)
    )

    val (query, context, startPlan, solveds, cardinalities) = queryGraphWith(
      projection = projection,
      projectionsMap = projectionsMap,
      patternNodesInQG = Set("n"),
      solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes("n"), DistinctQueryProjection(Map(mSortVar.name -> mSortVar))))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Sort(startPlan, Seq(Ascending("m"))))

    solveds.get(result.id).horizon should equal(DistinctQueryProjection(Map(mSortVar.name -> mSortVar), QueryShuffle(sortItems = sortItems)))
  }

  test("should add sort without pre-projection for AggregatingQueryProjection") {
    val mSortVar = Variable("m")(pos)
    val mSortItem = ast.AscSortItem(mSortVar)(pos)
    val oSortVar = Variable("o")(pos)
    val oSortItem = ast.AscSortItem(oSortVar)(pos)

    val grouping = Map(
      sortVariable.name -> sortVariable,
      mSortVar.name -> mSortVar,
      // a projection that sort will not take care of
      "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos))
    val aggregating = Map(oSortVar.name -> oSortVar)

    val sortItems = Seq(mSortItem, oSortItem)
    val projection = AggregatingQueryProjection(
      groupingExpressions = grouping,
      aggregationExpressions = aggregating,
      shuffle = QueryShuffle(sortItems, skip = None, limit = None)
    )

    val (query, context, startPlan, solveds, cardinalities) = queryGraphWith(
      projection = projection,
      projectionsMap = grouping,
      patternNodesInQG = Set("n"),
      solved = RegularPlannerQuery(QueryGraph.empty.addPatternNodes("n"), AggregatingQueryProjection(Map(mSortVar.name -> mSortVar), Map(oSortVar.name -> oSortVar))))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Sort(startPlan, Seq(Ascending("m"), Ascending("o"))))

    solveds.get(result.id).horizon should equal(AggregatingQueryProjection(Map(mSortVar.name -> mSortVar), Map(oSortVar.name -> oSortVar), QueryShuffle(sortItems = sortItems)))
  }

  test("should add sort without pre-projection if things are already projected") {
    val sortExpression = Add(sortVariable, UnsignedDecimalIntegerLiteral("5")(pos))(pos)
    // given a plan that solves "n"
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWithRegularProjection(
      // The requirement to sort by n
      sortItems = Seq(variableSortItem),
      projectionsMap = Map(
        // an already solved projection
        sortVariable.name -> sortExpression,
        // and a projection that sort will not take care of
        "notSortColumn" -> UnsignedDecimalIntegerLiteral("5")(pos)),
      solved = RegularPlannerQuery(QueryGraph.empty, RegularQueryProjection(Map(sortVariable.name -> sortExpression))))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    result should equal(Sort(startPlan, Seq(columnOrder)))

    solveds.get(result.id).horizon should equal(RegularQueryProjection(Map(sortVariable.name -> sortExpression), QueryShuffle(sortItems = Seq(variableSortItem))))
  }

  test("should add the correct plans when query uses both ORDER BY, SKIP and LIMIT") {
    // given
    val (query, context, startPlan, solveds, cardinalities) = queryGraphWithRegularProjection(skip = Some(y), limit = Some(x), sortItems = Seq(variableSortItem))

    // when
    val result = sortSkipAndLimit(startPlan, query, context, solveds, cardinalities)

    // then
    val sorted = Sort(startPlan, Seq(columnOrder))
    val skipped = Skip(sorted, y)
    val limited = Limit(skipped, x, DoNotIncludeTies)

    result should equal(limited)
  }

  private def queryGraphWithRegularProjection(skip: Option[Expression] = None,
                                              limit: Option[Expression] = None,
                                              sortItems: Seq[SortItem] = Seq.empty,
                                              projectionsMap: Map[String, Expression] = Map("n" -> sortVariable),
                                              patternNodesInQG: Set[String] = Set("n"),
                                              solved: PlannerQuery = RegularPlannerQuery(QueryGraph.empty.addPatternNodes("n"))):
  (RegularPlannerQuery, LogicalPlanningContext, LogicalPlan, Solveds, Cardinalities) = {
    val projection = RegularQueryProjection(
      projections = projectionsMap,
      shuffle = QueryShuffle(sortItems, skip, limit)
    )
    queryGraphWith(projection, projectionsMap, patternNodesInQG, solved)
  }

  private def queryGraphWith(projection: QueryProjection,
                             projectionsMap: Map[String, Expression],
                             patternNodesInQG: Set[String],
                             solved: PlannerQuery):
  (RegularPlannerQuery, LogicalPlanningContext, LogicalPlan, Solveds, Cardinalities) = {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val qg = QueryGraph(patternNodes = patternNodesInQG)
    val query = RegularPlannerQuery(queryGraph = qg, horizon = projection)

    val plan = newMockedLogicalPlanWithSolved(idNames = patternNodesInQG, solveds = solveds, solved = solved)

    (query, context, plan, solveds, cardinalities)
  }
}
