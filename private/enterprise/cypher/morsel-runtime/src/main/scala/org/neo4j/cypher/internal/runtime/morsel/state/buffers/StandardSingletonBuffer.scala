/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state.buffers

/**
  * Implementation of a standard non-Thread-safe singleton buffer of elements of type T.
  */
class StandardSingletonBuffer[T <: AnyRef] extends Buffer[T] {
  private var datum: T = _

  override def put(t: T): Unit = {
    if (datum != null) {
      throw new IllegalStateException(s"SingletonBuffer is full: tried to put $t, but already held element $datum")
    }
    datum = t
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
}
