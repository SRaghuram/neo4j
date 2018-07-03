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
package org.neo4j.cypher.internal.compiler.v3_5.ast.conditions

import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.UnsignedDecimalIntegerLiteral
import org.opencypher.v9_0.rewriting.conditions.noDuplicatesInReturnItems
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class NoDuplicatesInReturnItemsTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: (Any => Seq[String]) = noDuplicatesInReturnItems

  test("happy if the return items do not contain duplicates") {
    val return1: ReturnItem = AliasedReturnItem(UnsignedDecimalIntegerLiteral("42")_, varFor("a"))_
    val return2: ReturnItem = AliasedReturnItem(UnsignedDecimalIntegerLiteral("42")_, varFor("b"))_
    val return3: ReturnItem = UnaliasedReturnItem(UnsignedDecimalIntegerLiteral("42")_, "42")_
    val ast: ReturnItems = ReturnItems(false, Seq(return1, return2, return3))_

    condition(ast) shouldBe empty
  }

  test("unhappy if the return items contains aliased duplicates") {
    val return1: ReturnItem = AliasedReturnItem(UnsignedDecimalIntegerLiteral("42")_, varFor("a"))_
    val return2: ReturnItem = AliasedReturnItem(UnsignedDecimalIntegerLiteral("42")_, varFor("a"))_
    val return3: ReturnItem = UnaliasedReturnItem(UnsignedDecimalIntegerLiteral("42")_, "42")_
    val ast: ReturnItems = ReturnItems(false, Seq(return1, return2, return3))_

    condition(ast) should equal(Seq(s"ReturnItems at ${ast.position} contain duplicate return item: $ast"))
  }

  test("unhappy if the return items contains unaliased duplicates") {
    val return1: ReturnItem = AliasedReturnItem(UnsignedDecimalIntegerLiteral("42")_, varFor("a"))_
    val return2: ReturnItem = UnaliasedReturnItem(UnsignedDecimalIntegerLiteral("42")_, "42")_
    val return3: ReturnItem = UnaliasedReturnItem(UnsignedDecimalIntegerLiteral("42")_, "42")_
    val ast: ReturnItems = ReturnItems(false, Seq(return1, return2, return3))_

    condition(ast) should equal(Seq(s"ReturnItems at ${ast.position} contain duplicate return item: $ast"))
  }
}
