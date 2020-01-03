/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util

import org.eclipse.collections.api.multimap.list.MutableListMultimap
import org.eclipse.collections.impl.factory.Multimaps
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.{copyDataFromRhs, fillKeyArray}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, PrefetchingIterator}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.storable.{LongArray, Values}

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

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    if (input.isEmpty)
      return Iterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return Iterator.empty

    val table = buildProbeTable(state.memoryTracker.memoryTrackingIterator(input), state)

    // This will only happen if all the lhs-values evaluate to null, which is probably rare.
    // But, it's cheap to check and will save us from exhausting the rhs, so it's probably worth it
    if (table.isEmpty)
      return Iterator.empty

    probeInput(rhsIterator, state, table)
  }

  private def buildProbeTable(lhsInput: Iterator[ExecutionContext], queryState: QueryState): MutableListMultimap[LongArray, ExecutionContext] = {
    val table = Multimaps.mutable.list.empty[LongArray, ExecutionContext]()

    for (current <- lhsInput) {
      val key = new Array[Long](width)
      fillKeyArray(current, key, lhsOffsets)

      if (key(0) != -1)
        table.put(Values.longArray(key), current)
    }

    table
  }

  private def probeInput(rhsInput: Iterator[ExecutionContext],
                         queryState: QueryState,
                         probeTable: MutableListMultimap[LongArray, ExecutionContext]): Iterator[ExecutionContext] =
    new PrefetchingIterator[ExecutionContext] {
      private val key = new Array[Long](width)
      private var matches: util.Iterator[ExecutionContext] = util.Collections.emptyIterator()
      private var currentRhsRow: ExecutionContext = _

      override def produceNext(): Option[ExecutionContext] = {
        // If we have already found matches, we'll first exhaust these
        if (matches.hasNext) {
          val lhs = matches.next()
          val newRow = SlottedExecutionContext(slots)
          lhs.copyTo(newRow)
          copyDataFromRhs(longsToCopy, refsToCopy, cachedPropertiesToCopy, newRow, currentRhsRow)
          return Some(newRow)
        }

        while (rhsInput.hasNext) {
          currentRhsRow = rhsInput.next()
          fillKeyArray(currentRhsRow, key, rhsOffsets)
          if (key(0) != -1 /*If we have nulls in the key, no match will be found*/ ) {
            matches = probeTable.get(Values.longArray(key)).iterator()
            if (matches.hasNext) {
              // If we did not recurse back in like this, we would have to double up on the logic for creating output rows from matches
              return produceNext()
            }
          }
        }

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
                      newRow: ExecutionContext,
                      rhs: ExecutionContext): Unit = {
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
  def fillKeyArray(current: ExecutionContext,
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
