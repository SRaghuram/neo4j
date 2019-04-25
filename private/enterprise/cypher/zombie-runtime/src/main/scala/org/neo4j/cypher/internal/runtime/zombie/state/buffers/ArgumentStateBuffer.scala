/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state.buffers

import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentStateFactory, MorselAccumulator}

/**
  * Delegating [[Buffer]] used in argument state maps.
  */
class ArgumentStateBuffer(override val argumentRowId: Long,
                          inner: Buffer[MorselExecutionContext])
  extends MorselAccumulator
     with Buffer[MorselExecutionContext] {

  // MorselAccumulator
  override def update(morsel: MorselExecutionContext): Unit = inner.put(morsel)

  // Buffer
  override def put(morsel: MorselExecutionContext): Unit = inner.put(morsel)
  override def hasData: Boolean = inner.hasData
  override def take(): MorselExecutionContext = inner.take()
  override def foreach(f: MorselExecutionContext => Unit): Unit = inner.foreach(f)
}

object ArgumentStateBuffer {
  object Factory extends ArgumentStateFactory[ArgumentStateBuffer] {
    override def newStandardArgumentState(argumentRowId: Long): ArgumentStateBuffer =
      new ArgumentStateBuffer(argumentRowId, new StandardBuffer[MorselExecutionContext])

    override def newConcurrentArgumentState(argumentRowId: Long): ArgumentStateBuffer =
      new ArgumentStateBuffer(argumentRowId, new ConcurrentBuffer[MorselExecutionContext])
  }
}
