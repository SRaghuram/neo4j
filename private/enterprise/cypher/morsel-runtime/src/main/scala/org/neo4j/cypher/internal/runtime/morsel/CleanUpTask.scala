/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.morsel.execution.WorkerExecutionResources
import org.neo4j.cypher.internal.runtime.morsel.operators.{NoOutputOperator, PreparedOutput}
import org.neo4j.cypher.internal.runtime.morsel.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

/**
  * A task that is scheduled to clean up the execution state in case of cancellation
  * in parallel.
  */
class CleanUpTask(executionState: ExecutionState) extends Task[WorkerExecutionResources] {
  override def executeWorkUnit(resources: WorkerExecutionResources,
                               workUnitEvent: WorkUnitEvent,
                               queryProfiler: QueryProfiler): PreparedOutput = {
    executionState.cancelQuery(resources)
    NoOutputOperator
  }

  override def canContinue: Boolean = false

  override def workId: Id = Id.INVALID_ID

  override def workDescription: String = "Cleaning up the ExecutionState"
}
