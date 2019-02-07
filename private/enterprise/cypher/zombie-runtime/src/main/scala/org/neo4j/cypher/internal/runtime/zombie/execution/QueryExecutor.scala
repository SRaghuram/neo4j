/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.execution

import org.neo4j.cypher.internal.physicalplanning.StateDefinition
import org.neo4j.cypher.internal.runtime.scheduling.SchedulerTracer
import org.neo4j.cypher.internal.runtime.zombie.ExecutablePipeline
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.virtual.MapValue

/**
  * Executor of queries. It's currently a merge of a dispatcher, a scheduler and a spatula.
  */
trait QueryExecutor {
  def execute[E <: Exception](executablePipelines: IndexedSeq[ExecutablePipeline],
                              stateDefinition: StateDefinition,
                              inputDataStream: InputDataStream,
                              queryContext: QueryContext,
                              params: MapValue,
                              schedulerTracer: SchedulerTracer,
                              queryIndexes: Array[IndexReadSession],
                              visitor: QueryResult.QueryResultVisitor[E]): Unit
}
