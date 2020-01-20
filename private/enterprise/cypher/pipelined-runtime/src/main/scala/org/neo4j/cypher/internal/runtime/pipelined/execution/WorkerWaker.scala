/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryStatus
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink

trait WorkerWaker {

  /**
   * Wake up an idle worker.
   */
  def wakeOne(): Unit
}

class AlarmSink[-T <: AnyRef](inner: Sink[T], waker: WorkerWaker, queryStatus: QueryStatus) extends Sink[T] {

  /**
   * Put an element in this sink
   */
  override def put(t: T): Unit = {
    if (!queryStatus.cancelled) {
      inner.put(t)
      waker.wakeOne()
    } else {
      DebugSupport.ERROR_HANDLING.log("Dropped data %s because of query cancellation", t)
    }
  }

  override def canPut: Boolean = inner.canPut
}
