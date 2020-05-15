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
import org.neo4j.kernel.impl.util.collection
import org.neo4j.kernel.impl.util.collection.ProbeTable
import org.neo4j.memory.Measurable

abstract class AbstractHashJoinPipe[Key <: Measurable](left: Pipe,
                                                          right: Pipe) extends PipeWithSource(left) {
  override protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] = {

    if (!input.hasNext)
      return Iterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return Iterator.empty

    val table = buildProbeTable(input, state)

    // This will only happen if all the lhs-values evaluate to null, which is probably rare.
    // But, it's cheap to check and will save us from exhausting the rhs, so it's probably worth it
    if (table.isEmpty)
      return Iterator.empty

    probeInput(rhsIterator, state, table)
  }

  def buildProbeTable(input: Iterator[CypherRow], queryState: QueryState): collection.ProbeTable[Key, CypherRow]

  def probeInput(rhsInput: Iterator[CypherRow],
                 queryState: QueryState,
                 probeTable: ProbeTable[Key, CypherRow]): Iterator[CypherRow]
}
