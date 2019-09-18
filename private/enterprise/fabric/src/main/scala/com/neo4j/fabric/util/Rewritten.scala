/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.util

import org.neo4j.cypher.internal.v4_0.util
import org.neo4j.cypher.internal.v4_0.util.Rewritable._
import org.neo4j.cypher.internal.v4_0.util.Rewriter

object Rewritten {

  implicit class RewritingOps[T <: AnyRef](val that: T) extends AnyVal {
    def rewritten: Rewritten[T] = Rewritten(that)
  }

  private val never: PartialFunction[AnyRef, Boolean] = {
    case _ => false
  }

  case class Rewritten[T <: AnyRef](
    that: T,
    stopper: AnyRef => Boolean = never
  ) {

    def stoppingAt(stop: PartialFunction[AnyRef, Boolean]): Rewritten[T] =
      copy(stopper = stop.orElse(never))

    def bottomUp(pf: PartialFunction[AnyRef, AnyRef]): T =
      that.endoRewrite(util.bottomUp(Rewriter.lift(pf), stopper))

    def topDown(pf: PartialFunction[AnyRef, AnyRef]): T =
      that.endoRewrite(util.topDown(Rewriter.lift(pf), stopper))
  }

}
