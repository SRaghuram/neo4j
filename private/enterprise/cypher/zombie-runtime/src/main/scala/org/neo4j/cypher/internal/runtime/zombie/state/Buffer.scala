/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

/**
  * Basic buffer.
  */
trait Buffer[T <: AnyRef] extends Consumable[T] {
  def produce(t: T): Unit
}

trait Consumable[T <: AnyRef] {
  def hasData: Boolean
  def consume(): T
}
