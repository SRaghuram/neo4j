/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import java.util

import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.internal.helpers.collection.Iterators

/**
 * Implementation of a standard non-Thread-safe singleton buffer of elements of type T.
 */
class StandardSingletonBuffer[T <: AnyRef] extends SingletonBuffer[T] {
  private var datum: T = _

  override def put(t: T, resources: QueryResources): Unit = {
    if (datum != null) {
      throw new IllegalStateException(s"SingletonBuffer is full: tried to put $t, but already held element $datum")
    }
    datum = t
  }

  override def tryPut(t: T): Unit = {
    if (datum == null) {
      datum = t
    }
  }

  override def canPut: Boolean = datum == null

  override def hasData: Boolean = datum != null

  override def take(): T = {
    val t = datum
    datum = null.asInstanceOf[T]
    t
  }

  override def foreach(f: T => Unit): Unit = {
    if (datum != null) {
      f(datum)
    }
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb ++= "StandardSingletonBuffer("
    if (datum != null) {
      sb ++= datum.toString
    }
    sb += ')'
    sb.result()
  }

  override def iterator: util.Iterator[T] = Iterators.iterator[T](datum)
}
