/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import scala.collection.mutable

object Collections {

  // Curiously, this is faster than doing e.g. IndexedSeq(...) or ArraySeq(...)
  def singletonIndexedSeq[T <: AnyRef](t: T): mutable.ArraySeq[T] = {
    val result = new mutable.ArraySeq[T](1)
    result(0) = t
    result
  }
}
