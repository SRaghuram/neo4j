/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.codegen.api

import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.isEmpty
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.print
import org.neo4j.codegen.api.IntermediateRepresentation.staticallyKnownPredicate
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class IntermediateRepresentationTest extends CypherFunSuite {

  test("isEmpty") {
    isEmpty(noop()) shouldBe true
    isEmpty(block(noop(), noop(), noop())) shouldBe true
    isEmpty(block(noop(), print(constant("hello world!")), noop())) shouldBe false
    isEmpty(block()) shouldBe true
  }

  test("staticallyKnownPredicate") {
    staticallyKnownPredicate(constant(true)) shouldBe Some(true)
    staticallyKnownPredicate(constant(false)) shouldBe Some(false)
    staticallyKnownPredicate(notOp(constant(true))) shouldBe Some(false)
    staticallyKnownPredicate(notOp(constant(false))) shouldBe Some(true)
    staticallyKnownPredicate(notOp(notOp(constant(true)))) shouldBe Some(true)
    staticallyKnownPredicate(notOp(notOp(constant(false)))) shouldBe Some(false)
    staticallyKnownPredicate(notOp(notOp(notOp(constant(true))))) shouldBe Some(false)
    staticallyKnownPredicate(notOp(notOp(notOp(constant(false))))) shouldBe Some(true)
    staticallyKnownPredicate(notOp(load[Boolean]("boolean"))) shouldBe None
  }

  test("condition") {
    condition(constant(true))(print(constant("hello"))) shouldBe print(constant("hello"))
    condition(constant(true))(block()) shouldBe noop()
    condition(constant(false))(print(constant("hello"))) shouldBe noop()
  }

  test("ifElse") {
    ifElse(constant(true))(print(constant("hello")))(print(constant("there"))) shouldBe print(constant("hello"))
    ifElse(constant(false))(print(constant("hello")))(print(constant("there"))) shouldBe print(constant("there"))
    ifElse(load[Boolean]("boolean"))(print(constant("hello")))(block()) shouldBe condition(load[Boolean]("boolean"))(print(constant("hello")))
    ifElse(load[Boolean]("boolean"))(block())(print(constant("there"))) shouldBe condition(notOp(load[Boolean]("boolean")))(print(constant("there")))
  }

  test("ternary") {
    ternary(constant(true),
      print(constant("hello")),
      print(constant("there"))) shouldBe print(constant("hello"))
    ternary(constant(false),
      print(constant("hello")),
      print(constant("there"))) shouldBe print(constant("there"))
  }

  test("not") {
   notOp(constant(true)) shouldBe constant(false)
   notOp(constant(false)) shouldBe constant(true)
   notOp(Not(load[Boolean]("boolean"))) shouldBe load[Boolean]("boolean")
   notOp(Not(Not(load[Boolean]("boolean")))) shouldBe notOp(load[Boolean]("boolean"))
  }

  //this is here just because we cannot import IntermediateRepresentation.not because of scalatest
  private def notOp(inner: IntermediateRepresentation) = IntermediateRepresentation.not(inner)
}
