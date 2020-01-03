/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v4_0.rewriting.conditions

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class NoDuplicatesInReturnItemsTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: Any => Seq[String] = noDuplicatesInReturnItems

  test("happy if the return items do not contain duplicates") {
    val return1 = AliasedReturnItem(literalUnsignedInt(42), varFor("a"))_
    val return2 = AliasedReturnItem(literalUnsignedInt(42), varFor("b"))_
    val return3 = UnaliasedReturnItem(literalUnsignedInt(42), "42")_
    val ast: ReturnItems = ReturnItems(includeExisting = false, Seq(return1, return2, return3))_

    condition(ast) shouldBe empty
  }

  test("unhappy if the return items contains aliased duplicates") {
    val return1 = AliasedReturnItem(literalUnsignedInt(42), varFor("a"))_
    val return2 = AliasedReturnItem(literalUnsignedInt(42), varFor("a"))_
    val return3 = UnaliasedReturnItem(literalUnsignedInt(42), "42")_
    val ast: ReturnItems = ReturnItems(includeExisting = false, Seq(return1, return2, return3))_

    condition(ast) should equal(Seq(s"ReturnItems at ${ast.position} contain duplicate return item: $ast"))
  }

  test("unhappy if the return items contains unaliased duplicates") {
    val return1 = AliasedReturnItem(literalUnsignedInt(42), varFor("a"))_
    val return2 = UnaliasedReturnItem(literalUnsignedInt(42), "42")_
    val return3 = UnaliasedReturnItem(literalUnsignedInt(42), "42")_
    val ast: ReturnItems = ReturnItems(includeExisting = false, Seq(return1, return2, return3))_

    condition(ast) should equal(Seq(s"ReturnItems at ${ast.position} contain duplicate return item: $ast"))
  }
}
