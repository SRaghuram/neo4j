/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateBufferFactoryFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

import scala.collection.mutable.ArrayBuffer

/**
 * Delegating [[Buffer]] used in argument state maps.
 */
class ArgumentStateBuffer(override val argumentRowId: Long,
                          inner: Buffer[Morsel],
                          override val argumentRowIdsForReducers: Array[Long])
  extends MorselAccumulator[Morsel]
  with Buffer[Morsel] {

  // MorselAccumulator
  override def update(morsel: Morsel, resources: QueryResources): Unit = put(morsel, resources)

  // Buffer
  override def put(morsel: Morsel, resources: QueryResources): Unit = {
    inner.put(morsel, resources)
  }

  override def canPut: Boolean = inner.canPut
  override def hasData: Boolean = inner.hasData
  override def take(): Morsel = inner.take()
  override def foreach(f: Morsel => Unit): Unit = inner.foreach(f)

  override def close(): Unit = {
    inner.close()
  }

  override def toString: String = {
    s"${getClass.getSimpleName}(argumentRowId=$argumentRowId, inner=$inner)"
  }

  override def shallowSize: Long = ArgumentStateBuffer.SHALLOW_SIZE // NOTE: This is not final since it is overridden by ArgumentStreamArgumentStateBuffer

  /**
   * Returns a [[MorselReadCursor]] over all containing morsels.
   */
  override def readCursor(onFirstRow: Boolean = false): MorselReadCursor = {
    if (inner.hasData) {
      val morsels = new ArrayBuffer[Morsel]()
      inner.foreach(m => morsels += m)
      MorselsSnapshot(morsels).readCursor(onFirstRow)
    }
    else {
      Morsel.empty.readCursor(onFirstRow)
    }
  }

  case class MorselsSnapshot(override val morsels: IndexedSeq[Morsel]) extends MorselIndexedSeq
}

object ArgumentStateBuffer extends ArgumentStateBufferFactoryFactory {
  final val SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[ArgumentStateBuffer])

  class Factory(stateFactory: StateFactory, operatorId: Id) extends ArgumentStateFactory[ArgumentStateBuffer] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long],
                                          memoryTracker: MemoryTracker): ArgumentStateBuffer =
      new ArgumentStateBuffer(argumentRowId, stateFactory.newBuffer[Morsel](operatorId), argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): ArgumentStateBuffer =
      new ArgumentStateBuffer(argumentRowId, stateFactory.newBuffer[Morsel](operatorId), argumentRowIdsForReducers)
  }

  def createFactory(stateFactory: StateFactory, operatorId: Int): Factory = {
    new Factory(stateFactory, Id(operatorId))
  }
}