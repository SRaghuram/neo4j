/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.NoOutputOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.PreparedOutput
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * A task that is scheduled to clean up the execution state in case of cancellation
 * in parallel.
 */
class CleanUpTask(executionState: ExecutionState) extends Task[QueryResources] {
  override def executeWorkUnit(resources: QueryResources,
                               workUnitEvent: WorkUnitEvent,
                               queryProfiler: QueryProfiler): PreparedOutput = {
    executionState.cancelQuery(resources)
    NoOutputOperator
  }

  override def canContinue: Boolean = false

  override def workId: Id = Id.INVALID_ID

  override def workDescription: String = "Cleaning up the ExecutionState"
}
