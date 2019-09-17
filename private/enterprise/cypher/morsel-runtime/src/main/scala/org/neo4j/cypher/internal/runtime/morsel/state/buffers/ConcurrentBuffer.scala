/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state.buffers

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.neo4j.cypher.internal.runtime.WithHeapUsageEstimation
import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.exceptions.TransactionOutOfMemoryException

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

  override def put(t: T): Unit = {
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
    sb ++= "ConcurrentBuffer("
    data.forEach(t => {
      sb ++= t.toString
      sb += ','
    })
    sb += ')'
    sb.result()
  }

  override def iterator: java.util.Iterator[T] = data.iterator()
}

class BoundedConcurrentBuffer[T <: WithHeapUsageEstimation](bound: Int) extends ConcurrentBuffer[T] {
  private val heapSize = new AtomicLong(0)

  override def put(t: T): Unit = {
    val _size = heapSize.addAndGet(t.estimatedHeapUsage)
    if (_size > bound) {
      throw new TransactionOutOfMemoryException()
    }
    super.put(t)
  }

  override def take(): T = {
    val t = super.take()
    if (t != null) {
      heapSize.addAndGet(-t.estimatedHeapUsage)
    }
    t
  }
}
