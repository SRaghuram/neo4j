/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.execution

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.QueryState
import org.neo4j.cypher.internal.runtime.zombie.{ExecutablePipeline, ExecutionState}

class ExecutingQuery(val executablePipelines: IndexedSeq[ExecutablePipeline],
                     val executionState: ExecutionState,
                     val queryContext: QueryContext,
                     val queryState: QueryState) extends QueryExecutionHandle {

  override def await(): Option[Throwable] = {
    executionState.awaitCompletion()
    None
  }
}
