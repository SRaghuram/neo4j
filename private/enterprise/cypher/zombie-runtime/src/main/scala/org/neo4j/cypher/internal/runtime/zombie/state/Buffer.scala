/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

/**
  * Basic buffer (put things and then take the in FIFO order).
  */
trait Buffer[T <: AnyRef] extends Sink[T] with Source[T]

/**
  * Place where you put things of type `T`.
  */
trait Sink[T <: AnyRef] {

  /**
    * Put an element in this sink
    */
  def put(t: T): Unit
}

/**
  * Place where you take things of type `T`.
  */
trait Source[T <: AnyRef] {

  /**
    * True if this source has data
    */
  def hasData: Boolean

  /**
    * Return the T to take, or `null` is available
    */
  def take(): T
}
