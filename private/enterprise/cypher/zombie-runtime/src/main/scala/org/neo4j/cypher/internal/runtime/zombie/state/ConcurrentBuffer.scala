/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

/**
  * Implementation of a concurrent [[Buffer]] of elements of type `T`.
  */
class ConcurrentBuffer[T <: AnyRef] extends Buffer[T] {
  private val data = new java.util.concurrent.ConcurrentLinkedQueue[T]()

  override def hasData: Boolean = !data.isEmpty

  override def produce(t: T): Unit = {
    data.add(t)
  }
  override def consume(): T = {
    data.poll()
  }
}
