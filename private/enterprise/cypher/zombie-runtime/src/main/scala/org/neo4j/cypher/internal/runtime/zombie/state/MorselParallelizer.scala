/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext

/**
  * Executor of queries. It's currently a merge of a dispatcher, a scheduler and a spatula.
  */
trait MorselParallelizer {
  def original: MorselExecutionContext
  def parallelClone: MorselExecutionContext
}
