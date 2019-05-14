/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.execution

import org.neo4j.cypher.internal.runtime.zombie.state.buffers.Sink

trait WorkerWaker {

  /**
    * Wake up an idle worker.
    */
  def wakeOne(): Unit
}

class AlarmSink[-T <: AnyRef](inner: Sink[T], waker: WorkerWaker) extends Sink[T] {

  /**
    * Put an element in this sink
    */
  override def put(t: T): Unit = {
    inner.put(t)
    waker.wakeOne()
  }
}
