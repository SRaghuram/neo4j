/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.morsel.state.QueryStatus
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Sink

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
      DebugSupport.logErrorHandling(s"Dropped data $t because of query cancellation")
    }
  }

  override def canPut: Boolean = inner.canPut
}
