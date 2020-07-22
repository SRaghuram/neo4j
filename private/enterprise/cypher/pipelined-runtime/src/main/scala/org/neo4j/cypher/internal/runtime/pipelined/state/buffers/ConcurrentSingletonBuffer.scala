/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import java.util.concurrent.atomic.AtomicReference

import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources

/**
 * Implementation of a concurrent singleton [[Buffer]] of elements of type `T`.
 */
class ConcurrentSingletonBuffer[T <: AnyRef] extends SingletonBuffer[T] {
  private val datum = new AtomicReference[T]()

  override def put(t: T, resources: QueryResources): Unit = {
    if (!datum.compareAndSet(null.asInstanceOf[T], t)) {
      throw new IllegalStateException(s"SingletonBuffer is full: tried to put $t but already held element ${datum.get}")
    }
  }

  override def tryPut(t: T): Unit = {
    datum.compareAndSet(null.asInstanceOf[T], t)
  }

  override def canPut: Boolean = datum.get() == null

  override def hasData: Boolean = datum.get() != null

  override def take(): T = {
    datum.getAndSet(null.asInstanceOf[T])
  }

  override def foreach(f: T => Unit): Unit = {
    val t = datum.get()
    if (t != null) {
      f(t)
    }
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb ++= s"${getClass.getSimpleName}("
    val t = datum.get()
    if (t != null) {
      sb ++= t.toString
    }
    sb += ')'
    sb.result()
  }
}
