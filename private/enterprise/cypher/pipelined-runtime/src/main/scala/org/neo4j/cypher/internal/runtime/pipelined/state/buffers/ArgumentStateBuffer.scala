/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedExecutionContext
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.{ArgumentStateFactory, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory

/**
  * Delegating [[Buffer]] used in argument state maps.
  */
class ArgumentStateBuffer(override val argumentRowId: Long,
                          inner: Buffer[PipelinedExecutionContext],
                          override val argumentRowIdsForReducers: Array[Long])
  extends MorselAccumulator[PipelinedExecutionContext]
     with Buffer[PipelinedExecutionContext] {

  // MorselAccumulator
  override def update(morsel: PipelinedExecutionContext): Unit = put(morsel)

  // Buffer
  override def put(morsel: PipelinedExecutionContext): Unit = {
    morsel.resetToFirstRow()
    inner.put(morsel)
  }

  override def canPut: Boolean = inner.canPut
  override def hasData: Boolean = inner.hasData
  override def take(): PipelinedExecutionContext = inner.take()
  override def foreach(f: PipelinedExecutionContext => Unit): Unit = inner.foreach(f)

  override def iterator: java.util.Iterator[PipelinedExecutionContext] = {
    inner.iterator
  }

  override def toString: String = {
    s"ArgumentStateBuffer(argumentRowId=$argumentRowId)"
  }
}

object ArgumentStateBuffer {
  class Factory(stateFactory: StateFactory) extends ArgumentStateFactory[ArgumentStateBuffer] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: PipelinedExecutionContext, argumentRowIdsForReducers: Array[Long]): ArgumentStateBuffer =
      new ArgumentStateBuffer(argumentRowId, stateFactory.newBuffer[PipelinedExecutionContext](), argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: PipelinedExecutionContext, argumentRowIdsForReducers: Array[Long]): ArgumentStateBuffer =
      new ArgumentStateBuffer(argumentRowId, stateFactory.newBuffer[PipelinedExecutionContext](), argumentRowIdsForReducers)
  }
}
