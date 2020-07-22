/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources

/**
  * Basic buffer (put things and then take them in FIFO order).
  */
trait Buffer[T <: AnyRef] extends Sink[T] with Source[T] with AutoCloseable {

  /**
    * Perform {{{f}}} on each element in the buffer.
    *
    * Note that there are no guarantees that an element observed
    * by [[foreach]] will not be taken.
    */
  def foreach(f: T => Unit): Unit

  /**
   * @return an iterator of the contents of the buffer. Does not modify the buffer.
   */
  override def close(): Unit = {}
}

trait SingletonBuffer[T <: AnyRef] extends Buffer[T] {
  /**
    * Tries to put a datum into the singleton buffer, but simply does nothing if it is already full.
    */
  def tryPut(t: T): Unit
}

object Buffer {

  /**
    * This is not a hard limit, buffers can temporarily reach sizes bigger than this limit.
    */
  val MAX_SIZE_HINT = 10
}

/**
  * Place where you put things of type `T`.
  */
trait Sink[-T <: AnyRef] {

  /**
    * Put an element in this sink
    */
  def put(t: T, resources: QueryResources): Unit

  /**
   * Checks if there is room in the sink
   * @return `true` if there is room in the sink, otherwise `false`
   */
  def canPut: Boolean
}

/**
 * Place where you take things of type `T`.
 */
trait Source[+T <: AnyRef] {

  /**
   * @return `true` if this source has data
   */
  def hasData: Boolean

  /**
   * This modifies the source and removes the returned element.
   *
   * @return the taken T, or `null` if nothing is available
   */
  def take(): T

  /**
   * This modifies the source and removes the returned elements.
   *
   * @param n the maximum number of elements to take.
   * @return the taken Ts, or `null` if nothing is available
   */
  def take(n: Int): IndexedSeq[T] = {
    val builder = IndexedSeq.newBuilder[T]
    var i = 0
    while (i < n) {
      builder += take()
      i += 1
    }
    builder.result()
  }
}

/**
 * A source where you can also close the things you take.
 */
trait ClosingSource[T <: AnyRef] extends Source[T] {

  def close(data: T): Unit
}

