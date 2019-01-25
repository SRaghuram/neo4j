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
package org.neo4j.cypher.internal.ir.v4_0

import org.neo4j.cypher.internal.v4_0.expressions.Variable
import org.neo4j.cypher.internal.v4_0.util.DummyPosition
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class ProvidedOrderTest extends CypherFunSuite {

  test("should append provided order") {
    val left = ProvidedOrder.asc(varFor("a")).asc(varFor("b"))
    val right = ProvidedOrder.asc(varFor("c")).asc(varFor("d"))
    left.followedBy(right).columns should be(left.columns ++ right.columns)
  }

  test("should append empty provided order") {
    val left = ProvidedOrder.asc(varFor("a")).asc(varFor("b"))
    val right = ProvidedOrder.empty
    left.followedBy(right).columns should be(left.columns)
  }

  test("when provided order is empty the result combined provided order should always be empty") {
    val left = ProvidedOrder.empty
    val right = ProvidedOrder.asc(varFor("c")).asc(varFor("d"))
    val empty = ProvidedOrder.empty
    left.followedBy(right).columns should be(Seq.empty)
    left.followedBy(empty).columns should be(Seq.empty)
  }

  test("should trim provided order to before any matching function arguments") {
    val left = ProvidedOrder.asc(varFor("a")).asc(varFor("b")).asc(varFor("c")).asc(varFor("d"))

    left.upToExcluding(Set("x")).columns should be(left.columns)

    left.upToExcluding(Set("a")).columns should be(Seq.empty)

    left.upToExcluding(Set("c")).columns should be(left.columns.slice(0, 2))

    ProvidedOrder.empty.upToExcluding(Set("c")).columns should be(Seq.empty)
  }

  test("Empty required order satisfied by anything") {
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.empty) should be(true)
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.asc(varFor("x"))) should be(true)
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.desc(varFor("x"))) should be(true)
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("y"))) should be(true)
    InterestingOrder.empty.satisfiedBy(ProvidedOrder.desc(varFor("x")).desc(varFor("y"))) should be(true)
  }

  test("Single property required order satisfied by matching provided order") {
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(ProvidedOrder.asc(varFor("x"))) should be(true)
  }

  test("Single property required order satisfied by longer provided order") {
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("y"))) should be(true)
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(ProvidedOrder.asc(varFor("x")).desc(varFor("y"))) should be(true)
  }

  test("Single property required order not satisfied by mismatching provided order") {
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(ProvidedOrder.asc(varFor("y"))) should be(false)
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(ProvidedOrder.desc(varFor("x"))) should be(false)
    InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"))).satisfiedBy(ProvidedOrder.asc(varFor("y")).asc(varFor("x"))) should be(false)
  }

  test("Multi property required order satisfied only be matching provided order") {
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")).desc(varFor("y")).asc(varFor("z")))

    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x"))) should be(false)
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).desc(varFor("y"))) should be(false)
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("z"))) should be(true)
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).desc(varFor("z")).asc(varFor("y"))) should be(false)
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).desc(varFor("y")).desc(varFor("z"))) should be(false)
    interestingOrder.satisfiedBy(ProvidedOrder.asc(varFor("x")).asc(varFor("y")).desc(varFor("z"))) should be(false)
  }

  private def varFor(name: String): Variable = Variable(name)(DummyPosition(0))
}
