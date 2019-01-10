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
package org.neo4j.cypher.internal.compiler.v4_0.planner

import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.ast.{AstConstructionTestSupport, SortItem}
import org.neo4j.cypher.internal.ir.v4_0._
import org.neo4j.cypher.internal.v4_0.expressions.UnsignedDecimalIntegerLiteral

class PlannerQueryTest extends CypherFunSuite with AstConstructionTestSupport {

  private val mockInterestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(null, null))

  test("pair map") {

    val qg1 = QueryGraph.empty
    val qg2 = QueryGraph.empty
    val qg3 = QueryGraph.empty

    val pq3 = RegularPlannerQuery(queryGraph = qg3, tail = None)
    val pq2 = RegularPlannerQuery(queryGraph = qg2, tail = Some(pq3))
    val pq1 = RegularPlannerQuery(queryGraph = qg1, tail = Some(pq2))

    var seenOnPos1 = List.empty[PlannerQuery]
    var seenOnPos2 = List.empty[PlannerQuery]

    val result = pq1.foldMap {
      case (pq1: PlannerQuery, pq2: PlannerQuery) =>
        seenOnPos1 = seenOnPos1 :+ pq1
        seenOnPos2 = seenOnPos2 :+ pq2
        pq2
    }

    seenOnPos1 should equal(List(pq1, pq2))
    seenOnPos2 should equal(List(pq2, pq3))
    result should equal(pq1)
  }

  test("foldMap on single plannerQuery returns that PQ") {

    val input = RegularPlannerQuery(queryGraph = QueryGraph.empty, tail = None)

    val result = input.foldMap {
      case (pq1: PlannerQuery, pq2: PlannerQuery) =>
        fail("should not pass through here")
    }

    result should equal(input)
  }

  test("foldMap plannerQuery with tail should change when reverseMapped") {

    val tail = RegularPlannerQuery(queryGraph = QueryGraph.empty)
    val firstQueryGraph = QueryGraph.empty
    val secondQueryGraph = QueryGraph(patternNodes = Set("a"))
    val input = RegularPlannerQuery(queryGraph = firstQueryGraph, tail = Some(tail))

    val result = input.foldMap {
      case (pq1: PlannerQuery, pq2: PlannerQuery) =>
        pq2.withQueryGraph(secondQueryGraph)
    }

    result should not equal input
    result.queryGraph should equal(firstQueryGraph)
    result.tail.get.queryGraph should equal(secondQueryGraph)
  }

  test("should compute laziness preference correctly for a single planner query") {
    val noLimit = RegularPlannerQuery(horizon = QueryProjection.empty)
    noLimit.preferredStrictness should equal(None)

    val shuffleWithLimit = QueryProjection.empty.withShuffle(QueryShuffle(limit = Some(UnsignedDecimalIntegerLiteral("42")(pos))))
    val hasLimit = RegularPlannerQuery(horizon = shuffleWithLimit, interestingOrder = InterestingOrder.empty)
    hasLimit.preferredStrictness should equal(Some(LazyMode))

    val shuffleWithLimitAndSort = QueryProjection.empty.withShuffle(QueryShuffle(limit = Some(UnsignedDecimalIntegerLiteral("42")(pos))))
    val hasLimitAndSort = RegularPlannerQuery(horizon = shuffleWithLimitAndSort, interestingOrder = mockInterestingOrder)
    hasLimitAndSort.preferredStrictness should equal(None)
  }

  test("should consider planner query tails when computing laziness preference") {
    val shuffleWithLimit = QueryProjection.empty.withShuffle(QueryShuffle(limit = Some(UnsignedDecimalIntegerLiteral("42")(pos))))

    // pq -> pqWithLimit -> pqWithLimitAndSort

    val pqWithLimitAndSort: PlannerQuery = RegularPlannerQuery(horizon = shuffleWithLimit, interestingOrder = mockInterestingOrder)
    val pqWithLimit = RegularPlannerQuery(horizon = shuffleWithLimit, tail = Some(pqWithLimitAndSort), interestingOrder = InterestingOrder.empty)
    val pq = RegularPlannerQuery(tail = Some(pqWithLimit))

    pq.preferredStrictness should equal(Some(LazyMode))
    pqWithLimit.preferredStrictness should equal(Some(LazyMode))
    pqWithLimitAndSort.preferredStrictness should equal(None)
  }
}
