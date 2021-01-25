/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources

/**
 * Implementation of a concurrent [[Buffer]] of elements of type `T`.
 */
class ConcurrentBuffer[T <: AnyRef] extends Buffer[T] {
  private val data = new java.util.concurrent.ConcurrentLinkedQueue[T]()

  /**
   * The size counter is here in order to track the size of data, and we don't want to rely on `data.size`
   * since that has linear complexity. The buffer has a soft requirement not to grow above `Buffer.MAX_SIZE_HINT`,
   * since that is only a soft limit we don't need to worry about the situations where size and data.size temporarily
   * gets out of sync.
   */
  private val size = new AtomicInteger(0)

  override def hasData: Boolean = !data.isEmpty

  override def put(t: T, resources: QueryResources): Unit = {
    size.incrementAndGet()
    data.add(t)
  }

  override def canPut: Boolean = size.get() < Buffer.MAX_SIZE_HINT
  override def take(): T = {
    val taken = data.poll()
    if (taken != null) {
      size.decrementAndGet()
    }
    taken
  }

  override def foreach(f: T => Unit): Unit = {
    data.forEach(t => f(t))
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb ++= s"${getClass.getSimpleName}("
    data.forEach(t => {
      sb ++= t.toString
      sb += ','
    })
    sb += ')'
    sb.result()
  }
}
