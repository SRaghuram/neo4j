/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingLongHashSet
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

class TriadicState(override val argumentRowId: Long,
                   override val argumentRowIdsForReducers: Array[Long],
                   memoryTracker: MemoryTracker) extends ArgumentState {

  private case class Chunk(sourceId: Long, seenIds: HeapTrackingLongHashSet)

  private[this] val seenChunks: HeapTrackingArrayList[Chunk] = HeapTrackingArrayList.newArrayList(memoryTracker)

  def addSeenRows(sourceId: Long, seenRows: java.util.List[MorselRow], seenOffset: Int): Unit = {
    val seenIds = HeapTrackingCollections.newLongSet(memoryTracker)
    var i = 0
    while (i < seenRows.size()) {
      val x = seenRows.get(i).getLongAt(seenOffset)
      seenIds.add(x)
      i += 1
    }
    seenChunks.add(Chunk(sourceId, seenIds))
  }

  def contains(sourceId: Long, targetId: Long): Boolean = {
    var head = seenChunks.get(0)
    while (head.sourceId != sourceId) {
      // Assuming that TriadicFilter receives rows in the same chunks as TriadicBuild, we can trim old chunks here.
      // This works for current TriadicSelection uses, but won't work in general case.
      head.seenIds.close()
      seenChunks.remove(0)
      head = seenChunks.get(0)
    }
    head.seenIds.contains(targetId)
  }

  override def close(): Unit = {
    seenChunks.forEach(_.seenIds.close())
    seenChunks.close()
    super.close()
  }

  override def toString: String = s"TriadicState($argumentRowId)"

  override final def shallowSize: Long = TriadicState.SHALLOW_SIZE
}

object TriadicState {
  private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[TriadicState])

  object Factory extends ArgumentStateFactory[TriadicState] {

    override def completeOnConstruction: Boolean = true

    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long],
                                          memoryTracker: MemoryTracker): TriadicState =
      new TriadicState(argumentRowId, argumentRowIdsForReducers, memoryTracker)

    override def newConcurrentArgumentState(argumentRowId: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long]): TriadicState =
      throw new IllegalStateException("TriadicSelection is not supported in parallel")
  }
}
