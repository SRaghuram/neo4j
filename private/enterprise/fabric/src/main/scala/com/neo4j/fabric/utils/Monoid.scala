/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.utils

object Monoid {
  def apply[T: Monoid]: Monoid[T] = implicitly[Monoid[T]]

  def create[T](emp: T)(comb: (T, T) => T): Monoid[T] = new Monoid[T] {
    override def empty: T = emp

    override def combine(a: T, b: T): T = comb(a, b)
  }

  trait Monoid[T] {
    def empty: T

    def combine(a: T, b: T): T
  }

  implicit class MonoidOps[T: Monoid](t: T) {
    def combine(o: T): T = Monoid[T].combine(t, o)
  }

  implicit def SeqMonoid[T]: Monoid[Seq[T]] = new Monoid[Seq[T]] {
    override def empty: Seq[T] = Seq()

    override def combine(a: Seq[T], b: Seq[T]): Seq[T] = a ++ b
  }

  implicit def ListMonoid[T]: Monoid[List[T]] = new Monoid[List[T]] {
    override def empty: List[T] = List()

    override def combine(a: List[T], b: List[T]): List[T] = a ++ b
  }

}
