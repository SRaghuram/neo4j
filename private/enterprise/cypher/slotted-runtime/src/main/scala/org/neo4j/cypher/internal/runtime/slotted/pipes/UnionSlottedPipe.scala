/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.RowMapping
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

case class UnionSlottedPipe(lhs: Pipe, rhs: Pipe,
                            lhsMapping: RowMapping,
                            rhsMapping: RowMapping)
                           (val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val left = lhs.createResults(state)
    val right = rhs.createResults(state)

    new Iterator[ExecutionContext] {
      override def hasNext: Boolean = left.hasNext || right.hasNext
      override def next(): ExecutionContext =
        if (left.hasNext)
          lhsMapping(left.next(), state)
        else
          rhsMapping(right.next(), state)
    }
  }
}
