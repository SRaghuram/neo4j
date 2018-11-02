/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.opencypher.v9_0.util.attribution.Id

case class ApplySlottedPipe(lhs: Pipe, rhs: Pipe)
                           (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) with Pipe {
  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.flatMap {
      lhsContext =>
        val rhsState = state.withInitialContext(lhsContext)
        rhs.createResults(rhsState)
    }
}
