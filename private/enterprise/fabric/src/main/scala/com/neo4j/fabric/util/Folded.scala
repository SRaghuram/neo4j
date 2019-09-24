/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.util

import org.neo4j.cypher.internal.v4_0.util.Foldable.FoldableAny

object Folded {

  trait Instruction[R]

  case class Stop[R](r: R) extends Instruction[R]

  case class Descend[R](r: R) extends Instruction[R]

  case class DescendWith[R](r1: R, r2: R) extends Instruction[R]

  implicit class FoldableOps[T](f: T) {

    def folded[R](init: R)(merge: (R, R) => R)(instructions: PartialFunction[Any, Instruction[R]]): R =
      f.treeFold(init) {
        case a: Any if instructions.isDefinedAt(a) =>
          (r: R) =>
            instructions(a) match {
              case i: Stop[R]        => (merge(r, i.r), None)
              case i: Descend[R]     => (merge(r, i.r), Some(identity))
              case i: DescendWith[R] => (merge(r, i.r1), Some(rb => merge(rb, i.r2)))
            }
      }
  }
}
