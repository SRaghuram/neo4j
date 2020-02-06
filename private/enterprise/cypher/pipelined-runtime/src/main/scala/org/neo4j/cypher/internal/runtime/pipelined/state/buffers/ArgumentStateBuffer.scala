/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselCypherRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Delegating [[Buffer]] used in argument state maps.
 */
class ArgumentStateBuffer(override val argumentRowId: Long,
                          inner: Buffer[MorselCypherRow],
                          override val argumentRowIdsForReducers: Array[Long])
  extends MorselAccumulator[MorselCypherRow]
  with Buffer[MorselCypherRow] {

  // MorselAccumulator
  override def update(morsel: MorselCypherRow): Unit = put(morsel)

  // Buffer
  override def put(morsel: MorselCypherRow): Unit = {
    morsel.resetToFirstRow()
    inner.put(morsel)
  }

  override def canPut: Boolean = inner.canPut
  override def hasData: Boolean = inner.hasData
  override def take(): MorselCypherRow = inner.take()
  override def foreach(f: MorselCypherRow => Unit): Unit = inner.foreach(f)

  override def iterator: java.util.Iterator[MorselCypherRow] = {
    inner.iterator
  }

  override def toString: String = {
    s"ArgumentStateBuffer(argumentRowId=$argumentRowId)"
  }
}

object ArgumentStateBuffer {
  class Factory(stateFactory: StateFactory, operatorId: Id) extends ArgumentStateFactory[ArgumentStateBuffer] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselCypherRow, argumentRowIdsForReducers: Array[Long]): ArgumentStateBuffer =
      new ArgumentStateBuffer(argumentRowId, stateFactory.newBuffer[MorselCypherRow](operatorId), argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselCypherRow, argumentRowIdsForReducers: Array[Long]): ArgumentStateBuffer =
      new ArgumentStateBuffer(argumentRowId, stateFactory.newBuffer[MorselCypherRow](operatorId), argumentRowIdsForReducers)
  }
}
