/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

/**
  * A pipeline that aggregates input morsels into a queue,
  * and processes these morsels in a [[ContinuableOperatorTask]].
  */
trait ReducePipeline extends Pipeline {
  def initCollector(): ReduceCollector
}
