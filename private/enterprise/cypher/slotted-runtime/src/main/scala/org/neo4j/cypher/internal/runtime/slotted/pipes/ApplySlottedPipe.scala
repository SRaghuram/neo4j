/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class ApplySlottedPipe(lhs: Pipe, rhs: Pipe)
                           (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) with Pipe {
  override protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] =
    input.flatMap {
      lhsContext =>
        val rhsState = state.withInitialContext(lhsContext)
        rhs.createResults(rhsState)
    }
}
