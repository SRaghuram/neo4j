/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel

/**
 * Allows executing over the same input morsel in parallel.
 */
trait MorselParallelizer {

  /**
   * Return the original morsel holding the data of this parallelizer.
   */
  def originalForClosing: Morsel

  /**
   * Return the next shallow copy of the underlying morsel.
   */
  def nextCopy: Morsel
}
