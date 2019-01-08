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
package org.neo4j.cypher.internal.frontend.v3_2.ast

import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_2.{DummyPosition, SemanticError, SemanticState}

class UnsignedDecimalIntegerLiteralTest extends CypherFunSuite {
  test("correctly parses decimal numbers") {
    assert(UnsignedDecimalIntegerLiteral("22")(DummyPosition(0)).value === 22)
    assert(UnsignedDecimalIntegerLiteral("0")(DummyPosition(0)).value === 0)
  }

  test("throws error for invalid decimal numbers") {
    assertSemanticError("12g3", "invalid literal number")
    assertSemanticError("923_23", "invalid literal number")
  }

  test("throws error for too large decimal numbers") {
    assertSemanticError("999999999999999999999999999", "integer is too large")
  }

  private def assertSemanticError(stringValue: String, errorMessage: String) {
    val literal = UnsignedDecimalIntegerLiteral(stringValue)(DummyPosition(4))
    val result = literal.semanticCheck(SemanticContext.Simple)(SemanticState.clean)
    assert(result.errors === Vector(SemanticError(errorMessage, DummyPosition(4))))
  }
}
