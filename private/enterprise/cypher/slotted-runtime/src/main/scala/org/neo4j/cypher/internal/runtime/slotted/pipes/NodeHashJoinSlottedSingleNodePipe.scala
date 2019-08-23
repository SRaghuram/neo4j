/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps
import org.eclipse.collections.impl.list.mutable.FastList
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.{ExecutionContext, PrefetchingIterator}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

case class NodeHashJoinSlottedSingleNodePipe(lhsOffset: Int,
                                             rhsOffset: Int,
                                             left: Pipe,
                                             right: Pipe,
                                             slots: SlotConfiguration,
                                             longsToCopy: Array[(Int, Int)],
                                             refsToCopy: Array[(Int, Int)],
                                             cachedPropertiesToCopy: Array[(Int, Int)])
                                            (val id: Id = Id.INVALID_ID) extends PipeWithSource(left) {
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

  private def buildProbeTable(lhsInput: Iterator[ExecutionContext], queryState: QueryState): MutableLongObjectMap[FastList[ExecutionContext]] = {
    val table = LongObjectMaps.mutable.empty[FastList[ExecutionContext]]()

    for (current <- lhsInput) {
      val nodeId = current.getLongAt(lhsOffset)
      if(nodeId != -1) {
        val list = table.getIfAbsentPut(nodeId, new FastList[ExecutionContext](1))
        list.add(current)
      }
    }

    table
  }

  private def probeInput(rhsInput: Iterator[ExecutionContext],
                         queryState: QueryState,
                         probeTable: MutableLongObjectMap[FastList[ExecutionContext]]): Iterator[ExecutionContext] =
    new PrefetchingIterator[ExecutionContext] {
      private var matches: util.Iterator[ExecutionContext] = util.Collections.emptyIterator()
      private var currentRhsRow: ExecutionContext = _

      override def produceNext(): Option[ExecutionContext] = {
        // If we have already found matches, we'll first exhaust these
        if (matches.hasNext) {
          val lhs = matches.next()
          val newRow = SlottedExecutionContext(slots)
          lhs.copyTo(newRow)
          copyDataFromRhs(newRow, currentRhsRow)
          return Some(newRow)
        }

        while (rhsInput.hasNext) {
          currentRhsRow = rhsInput.next()
          val nodeId = currentRhsRow.getLongAt(rhsOffset)
          if(nodeId != -1) {
            val innerMatches = probeTable.get(nodeId)
            if(innerMatches != null) {
              matches = innerMatches.iterator()
              return produceNext()
            }
          }
        }

        None
      }
    }

  private def copyDataFromRhs(newRow: SlottedExecutionContext, rhs: ExecutionContext): Unit = {
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
