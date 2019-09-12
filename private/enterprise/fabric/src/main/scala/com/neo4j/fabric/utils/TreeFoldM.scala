/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.utils

import com.neo4j.fabric.utils.Monoid._
import org.neo4j.cypher.internal.v4_0.util.Foldable.FoldableAny

object TreeFoldM {

  trait Instruction[R]

  case class Stop[R](r: R) extends Instruction[R]

  case class Descend[R](r: R) extends Instruction[R]

  case class DescendWith[R](r1: R, r2: R) extends Instruction[R]

  implicit class FoldableOps[T](f: T) {

    def treeFoldM[R: Monoid](instructions: PartialFunction[Any, Instruction[R]]): R =
      f.treeFold(Monoid[R].empty) {
        case a: Any if instructions.isDefinedAt(a) =>
          (r: R) =>
            instructions(a) match {
              case i: Stop[R]        => (r combine i.r, None)
              case i: Descend[R]     => (r combine i.r, Some(identity))
              case i: DescendWith[R] => (r combine i.r1, Some(rb => rb combine i.r2))
            }
      }
  }

}
