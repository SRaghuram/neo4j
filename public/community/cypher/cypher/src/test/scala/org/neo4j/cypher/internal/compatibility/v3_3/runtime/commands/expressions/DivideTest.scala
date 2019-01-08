/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.frontend.v3_3
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class DivideTest extends CypherFunSuite {
  test("should_throw_arithmetic_exception_for_divide_by_zero") {
    val ctx = ExecutionContext.empty
    val state = QueryStateHelper.empty

    intercept[v3_3.ArithmeticException](Divide(Literal(1), Literal(0))(ctx, state))
    intercept[v3_3.ArithmeticException](Divide(Literal(1.4), Literal(0))(ctx, state))
    intercept[v3_3.ArithmeticException](Divide(Literal(1), Literal(0.0))(ctx, state))
    intercept[v3_3.ArithmeticException](Divide(Literal(3.4), Literal(0.0))(ctx, state))
  }
}
