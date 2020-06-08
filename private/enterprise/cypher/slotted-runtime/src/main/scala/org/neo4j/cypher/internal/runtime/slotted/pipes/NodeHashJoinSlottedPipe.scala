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
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.copyDataFromRhs
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.fillKeyArray
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.ProbeTable
import org.neo4j.values.storable.LongArray
import org.neo4j.values.storable.Values

case class NodeHashJoinSlottedPipe(lhsOffsets: Array[Int],
                                   rhsOffsets: Array[Int],
                                   left: Pipe,
                                   right: Pipe,
                                   slots: SlotConfiguration,
                                   longsToCopy: Array[(Int, Int)],
                                   refsToCopy: Array[(Int, Int)],
                                   cachedPropertiesToCopy: Array[(Int, Int)])
                                  (val id: Id = Id.INVALID_ID) extends PipeWithSource(left) {
  private val width: Int = lhsOffsets.length

  override protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] = {

    if (input.isEmpty)
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

  private def buildProbeTable(lhsInput: Iterator[CypherRow], queryState: QueryState): ProbeTable[LongArray, CypherRow] = {
    val table = ProbeTable.createProbeTable[LongArray, CypherRow](queryState.memoryTracker.memoryTrackerForOperator(id.x))

    for (current <- lhsInput) {
      val key = new Array[Long](width)
      fillKeyArray(current, key, lhsOffsets)

      if (key(0) != -1)
        table.put(Values.longArray(key), current)
    }

    table
  }

  private def probeInput(rhsInput: Iterator[CypherRow],
                         queryState: QueryState,
                         probeTable: ProbeTable[LongArray, CypherRow]): Iterator[CypherRow] =
    new PrefetchingIterator[CypherRow] {
      private val key = new Array[Long](width)
      private var matches: util.Iterator[CypherRow] = util.Collections.emptyIterator()
      private var currentRhsRow: CypherRow = _

      override def produceNext(): Option[CypherRow] = {
        // If we have already found matches, we'll first exhaust these
        if (matches.hasNext) {
          val lhs = matches.next()
          val newRow = SlottedRow(slots)
          lhs.copyTo(newRow)
          copyDataFromRhs(longsToCopy, refsToCopy, cachedPropertiesToCopy, newRow, currentRhsRow)
          return Some(newRow)
        }

        while (rhsInput.hasNext) {
          currentRhsRow = rhsInput.next()
          fillKeyArray(currentRhsRow, key, rhsOffsets)
          if (key(0) != -1 /*If we have nulls in the key, no match will be found*/ ) {
            matches = probeTable.get(Values.longArray(key))
            if (matches.hasNext) {
              // If we did not recurse back in like this, we would have to double up on the logic for creating output rows from matches
              return produceNext()
            }
          }
        }

        // We have produced the last row, close the probe table to release estimated heap usage
        probeTable.close()

        None
      }
    }

}

object NodeHashJoinSlottedPipe {

  /**
   * Copies longs, refs, and cached properties from the given rhs into the given new row.
   */
  def copyDataFromRhs(longsToCopy: Array[(Int, Int)],
                      refsToCopy: Array[(Int, Int)],
                      cachedPropertiesToCopy: Array[(Int, Int)],
                      newRow: WritableRow,
                      rhs: ReadableRow): Unit = {
    var i = 0
    while (i < longsToCopy.length) {
      val (from, to) = longsToCopy(i)
      newRow.setLongAt(to, rhs.getLongAt(from))
      i += 1
    }
    i = 0
    while (i < refsToCopy.length) {
      val (from, to) = refsToCopy(i)
      newRow.setRefAt(to, rhs.getRefAt(from))
      i += 1
    }
    i = 0
    while (i < cachedPropertiesToCopy.length) {
      val (from, to) = cachedPropertiesToCopy(i)
      newRow.setCachedPropertyAt(to, rhs.getCachedPropertyAt(from))
      i += 1
    }
  }

  /**
   * Modifies the given key array by writing the ids of the nodes
   * at the offsets of the given execution context into the array.
   *
   * If at least one node is null. It will write -1 into the first
   * position of the array.
   */
  def fillKeyArray(current: ReadableRow,
                   key: Array[Long],
                   offsets: Array[Int]): Unit = {
    // We use a while loop like this to be able to break out early
    var i = 0
    while (i < offsets.length) {
      val thisId = current.getLongAt(offsets(i))
      key(i) = thisId
      if (NullChecker.entityIsNull(thisId)) {
        key(0) = NullChecker.NULL_ENTITY // We flag the null in this cryptic way to avoid creating objects
        return
      }
      i += 1
    }
  }
}
