/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext

/**
  * Allows consuming the same input morsel in parallel.
  */
trait MorselParallelizer {

  /**
    * Return the next shallow copy of the underlying morsel.
    */
  def nextCopy: MorselExecutionContext
}
