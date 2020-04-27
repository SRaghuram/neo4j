/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import scala.reflect.ClassTag

/**
  * Wrapper for Array that only exposes read functionality.
  *
  * This was needed to
  *   1) micro-optimize the pipelined runtime code where scala collections were too slow. This slowness
  *      comes from having general solutions and mega-morphic call sites preventing inlining.
  *   2) prevent accidental modification of the arrays, which could happen if we used standard arrays.
  */
class ReadOnlyArray[T](inner: Array[T]) {

  final val length = inner.length
  final val isEmpty = inner.length == 0
  final def nonEmpty: Boolean = !isEmpty

  def apply(i: Int): T = inner(i)

  def map[U](f: T => U): ReadOnlyArray[U] = {
    val result = new Array[Any](length)
    var i = 0
    while (i < length) {
      result(i) = f(inner(i))
      i += 1
    }
    new ReadOnlyArray[U](result.asInstanceOf[Array[U]])
  }

  def foreach(f: T => Unit): Unit = {
    var i = 0
    while (i < length) {
      f(inner(i))
      i += 1
    }
  }

  /**
    * Return this data in a `Seq`. Not for hot path use.
    */
  def toSeq(): Seq[T] = inner.toSeq

  /**
    * Return a copy of this array with an appended element `t`. Not for hot path use.
    */
  def :+(t: T): ReadOnlyArray[T] = {
    val result = new Array[Any](length + 1)
    System.arraycopy(inner, 0, result, 0, length)
    result(length) = t
    new ReadOnlyArray[T](result.asInstanceOf[Array[T]])
  }

  /**
    * Return a copy of this array with a prepended element `t`. Not for hot path use.
    */
  def +:(t: T): ReadOnlyArray[T] = {
    val result = new Array[Any](length + 1)
    System.arraycopy(inner, 0, result, 1, length)
    result(0) = t
    new ReadOnlyArray[T](result.asInstanceOf[Array[T]])
  }

  // Equals and hashCode are implemented to simplify testing

  def canEqual(other: Any): Boolean = other.isInstanceOf[ReadOnlyArray[_]]

  override def equals(other: Any): Boolean =
    other match {
      case that: ReadOnlyArray[_] =>
        (that canEqual this) && inner.indices.forall(i => inner(i) == that.inner(i))
      case _ => false
    }

  override def hashCode(): Int = {
    inner.foldLeft(0)((a, b) => 31 * a + b.hashCode())
  }
}

object ReadOnlyArray {
  def empty[T: ClassTag]: ReadOnlyArray[T] = new ReadOnlyArray[T](Array.empty[T])

  def apply[T: ClassTag](ts: T*): ReadOnlyArray[T] = new ReadOnlyArray[T](ts.toArray)
}
