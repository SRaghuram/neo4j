/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.LongProbeTable

case class NodeHashJoinSlottedSingleNodePipe(lhsOffset: Int,
                                             rhsOffset: Int,
                                             left: Pipe,
                                             right: Pipe,
                                             slots: SlotConfiguration,
                                             longsToCopy: Array[(Int, Int)],
                                             refsToCopy: Array[(Int, Int)],
                                             cachedPropertiesToCopy: Array[(Int, Int)])
                                            (val id: Id = Id.INVALID_ID) extends PipeWithSource(left) {
  override protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] = {

    if (input.isEmpty)
      return Iterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return Iterator.empty

    val table = buildProbeTable(input, state)

    // This will only happen if all the lhs-values evaluate to null, which is probably rare.
    // But, it's cheap to check and will save us from exhausting the rhs, so it's probably worth it
    if (table.isEmpty) {
      table.close()
      return Iterator.empty
    }

    probeInput(rhsIterator, state, table)
  }

  private def buildProbeTable(lhsInput: Iterator[CypherRow], queryState: QueryState): LongProbeTable[CypherRow] = {
    val table = LongProbeTable.createLongProbeTable[CypherRow](queryState.memoryTracker.memoryTrackerForOperator(id.x))

    for (current <- lhsInput) {
      val nodeId = current.getLongAt(lhsOffset)
      if(nodeId != -1) {
        table.put(nodeId, current)
      }
    }

    table
  }

  private def probeInput(rhsInput: Iterator[CypherRow],
                         queryState: QueryState,
                         probeTable: LongProbeTable[CypherRow]): Iterator[CypherRow] =
    new PrefetchingIterator[CypherRow] {
      private var matches: util.Iterator[CypherRow] = util.Collections.emptyIterator()
      private var currentRhsRow: CypherRow = _

      override def produceNext(): Option[CypherRow] = {
        // If we have already found matches, we'll first exhaust these
        if (matches.hasNext) {
          val lhs = matches.next()
          val newRow = SlottedRow(slots)
          lhs.copyTo(newRow)
          copyDataFromRhs(newRow, currentRhsRow)
          return Some(newRow)
        }

        while (rhsInput.hasNext) {
          currentRhsRow = rhsInput.next()
          val nodeId = currentRhsRow.getLongAt(rhsOffset)
          if(nodeId != -1) {
            val innerMatches = probeTable.get(nodeId)
            if (innerMatches.hasNext) {
              matches = innerMatches
              return produceNext()
            }
          }
        }

        // We have produced the last row, close the probe table to release estimated heap usage
        probeTable.close()

        None
      }
    }

  private def copyDataFromRhs(newRow: SlottedRow, rhs: CypherRow): Unit = {
    var i = 0
    var len = longsToCopy.length
    while (i < len) {
      val longs = longsToCopy(i)
      newRow.setLongAt(longs._2, rhs.getLongAt(longs._1))
      i += 1
    }
    i = 0
    len = refsToCopy.length
    while (i < len) {
      val refs = refsToCopy(i)
      newRow.setRefAt(refs._2, rhs.getRefAt(refs._1))
      i += 1
    }
    i = 0
    len = cachedPropertiesToCopy.length
    while (i < len) {
      val cached = cachedPropertiesToCopy(i)
      newRow.setCachedPropertyAt(cached._2, rhs.getCachedPropertyAt(cached._1))
      i += 1
    }
  }
}
