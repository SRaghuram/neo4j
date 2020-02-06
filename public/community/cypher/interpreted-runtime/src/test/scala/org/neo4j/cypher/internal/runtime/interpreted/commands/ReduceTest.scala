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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions._
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.longValue

class ReduceTest extends CypherFunSuite {

  test("canReturnSomethingFromAnIterable") {
    val l = Seq("x", "xxx", "xx")
    val expression = Add(ExpressionVariable(0, "acc"), SizeFunction(ExpressionVariable(1, "n")))
    val collection = Variable("l")
    val m = CypherRow.from("l" -> l)
    val s = QueryStateHelper.emptyWith(expressionVariables = new Array(2))

    val reduce = ReduceFunction(collection, "n", 1, expression, "acc", 0, Literal(0))

    reduce.apply(m, s) should equal(longValue(6))
  }

  test("returns_null_from_null_collection") {
    val expression = Add(ExpressionVariable(0, "acc"), LengthFunction(ExpressionVariable(1, "n")))
    val collection = Literal(null)
    val m = CypherRow.empty
    val s = QueryStateHelper.emptyWith(expressionVariables = new Array(2))

    val reduce = ReduceFunction(collection, "n", 1, expression, "acc", 0, Literal(0))

    reduce(m, s) should equal(Values.NO_VALUE)
  }
}
