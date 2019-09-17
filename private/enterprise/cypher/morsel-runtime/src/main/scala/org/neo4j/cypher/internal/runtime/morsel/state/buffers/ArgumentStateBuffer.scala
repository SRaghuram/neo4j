/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state.buffers

import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateFactory, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.morsel.state.StateFactory

/**
  * Delegating [[Buffer]] used in argument state maps.
  */
class ArgumentStateBuffer(override val argumentRowId: Long,
                          inner: Buffer[MorselExecutionContext],
                          override val argumentRowIdsForReducers: Array[Long])
  extends MorselAccumulator[MorselExecutionContext]
     with Buffer[MorselExecutionContext] {

  // MorselAccumulator
  override def update(morsel: MorselExecutionContext): Unit = put(morsel)

  // Buffer
  override def put(morsel: MorselExecutionContext): Unit = {
    morsel.resetToFirstRow()
    inner.put(morsel)
  }

  override def canPut: Boolean = inner.canPut
  override def hasData: Boolean = inner.hasData
  override def take(): MorselExecutionContext = inner.take()
  override def foreach(f: MorselExecutionContext => Unit): Unit = inner.foreach(f)

  override def iterator: java.util.Iterator[MorselExecutionContext] = {
    inner.iterator
  }

  override def toString: String = {
    s"ArgumentStateBuffer(argumentRowId=$argumentRowId)"
  }
}

object ArgumentStateBuffer {
  class Factory(stateFactory: StateFactory) extends ArgumentStateFactory[ArgumentStateBuffer] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): ArgumentStateBuffer =
      new ArgumentStateBuffer(argumentRowId, stateFactory.newBuffer[MorselExecutionContext](), argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): ArgumentStateBuffer =
      new ArgumentStateBuffer(argumentRowId, stateFactory.newBuffer[MorselExecutionContext](), argumentRowIdsForReducers)
  }
}
