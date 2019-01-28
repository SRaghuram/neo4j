/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.{ContinuableOperatorTask, MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.HasWorkIdentity

/**
  * A streaming operator is initialized with an input morsel, to produce 0-n [[ContinuableOperatorTask]].
  */
trait StreamingOperator extends HasWorkIdentity {
  def init(context: QueryContext,
           state: QueryState,
           inputMorsel: MorselExecutionContext,
           resources: QueryResources): IndexedSeq[ContinuableOperatorTask]
}

/**
  * A continuable operator is initialized to produce exactly one [[ContinuableOperatorTask]].
  */
trait ContinuableOperator extends HasWorkIdentity {
  def init(context: QueryContext,
           state: QueryState,
           resources: QueryResources): ContinuableOperatorTask
}
