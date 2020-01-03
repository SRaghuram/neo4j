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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.Mockito.{never, verify, when}
import org.mockito.internal.stubbing.defaultanswers.ReturnsMocks
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SkipPipeTest extends CypherFunSuite {
  test("skip 0 should not actually pull from the input") {
    // Given
    val inputIterator = mock[Iterator[ExecutionContext]](new ReturnsMocks)

    when(inputIterator.isEmpty).thenReturn(false)

    val src: Pipe = new DummyPipe(inputIterator)
    val skipPipe = SkipPipe(src, Literal(0))()

    // When
    skipPipe.createResults(QueryStateHelper.empty)

    // Then
    verify(inputIterator, never()).next()
  }

  test("skip should accept longs") {
    // Given
    val inputIterator = Iterator.empty

    val src: Pipe = new DummyPipe(inputIterator)
    val skipPipe = SkipPipe(src, Literal(Long.MaxValue))()

    // When
    val result = skipPipe.createResults(QueryStateHelper.empty)

    // Then
    result should be(empty)
  }
}
