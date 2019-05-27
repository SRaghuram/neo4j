/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state.buffers

import scala.collection.mutable.ArrayBuffer

/**
  * Implementation of a standard non-Thread-safe buffer of elements of type T.
  */
class StandardBuffer[T <: AnyRef] extends Buffer[T] {
  private val data = new ArrayBuffer[T]

  override def put(t: T): Unit = {
    data.append(t)
  }

  override def canPut: Boolean = data.size < Buffer.MAX_SIZE_HINT

  override def hasData: Boolean = data.nonEmpty

  override def take(): T = {
    if (data.isEmpty) null.asInstanceOf[T]
    else data.remove(0)
  }

  override def foreach(f: T => Unit): Unit = {
    data.foreach(f)
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb ++= "StandardBuffer("
    data.foreach(t => {
      sb ++= t.toString
      sb += ','
    })
    sb += ')'
    sb.result()
  }
}
