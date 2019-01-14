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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext, QueryTransactionalContext}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContextHelper._

class EagerPipeTest extends CypherFunSuite {

  private val queryContext = mock[QueryContext]
  when(queryContext.transactionalContext).thenReturn(mock[QueryTransactionalContext])

  private val queryState = QueryStateHelper.emptyWith(query = queryContext)

  test("shouldMakeLazyEager") {
    // Given a lazy iterator that is not empty
    val lazyIterator = new LazyIterator[ExecutionContext](10, _ => ExecutionContext.empty)
    val src = new FakePipe(lazyIterator.map(_.toMap))
    val eager = EagerPipe(src)()
    lazyIterator should not be empty

    // When
    val resultIterator = eager.createResults(queryState)

    // Then the lazy iterator is emptied, and the returned iterator is not
    lazyIterator shouldBe empty
    resultIterator should not be empty
  }
}
