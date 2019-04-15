/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state.buffers

/**
  * Basic buffer (put things and then take them in FIFO order).
  */
trait Buffer[T <: AnyRef] extends Sink[T] with Source[T] {

  /**
    * Perform {{{f}}} on each element in the buffer.
    *
    * Not that there are no guarantees that an element observed
    * by [[foreach]] will not be taken.
    */
  def foreach(f: T => Unit): Unit
}

/**
  * Place where you put things of type `T`.
  */
trait Sink[-T <: AnyRef] {

  /**
    * Put an element in this sink
    */
  def put(t: T): Unit
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
    * @return the T to take, or `null` if nothing is available
    */
  def take(): T
}
