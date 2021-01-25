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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.EagerBuffer

import scala.collection.JavaConverters.asScalaIteratorConverter

case class EagerPipe(src: Pipe)(val id: Id = Id.INVALID_ID)
  extends PipeWithSource(src) {

  protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    val buffer = EagerBuffer.createEagerBuffer[CypherRow](state.memoryTracker.memoryTrackerForOperator(id.x),
                                                          1024,
                                                          8192,
                                                          EagerBuffer.GROW_NEW_CHUNKS_BY_100_PCT
                                                          )
    state.query.resources.trace(buffer)
    while (input.hasNext) {
      buffer.add(input.next)
    }
    ClosingIterator(buffer.autoClosingIterator().asScala).closing(buffer)
  }
}
