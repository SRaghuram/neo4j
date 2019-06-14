/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.runtime.morsel.ExecutablePipeline
import org.neo4j.cypher.internal.runtime.morsel.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscription}
import org.neo4j.values.AnyValue

/**
  * Executor of queries. A zombie spatula.
  */
trait QueryExecutor {

  /**
    * Execute a query using this executor.
    */
  def execute[E <: Exception](executablePipelines: IndexedSeq[ExecutablePipeline],
                              executionGraphDefinition: ExecutionGraphDefinition,
                              inputDataStream: InputDataStream,
                              queryContext: QueryContext,
                              params: Array[AnyValue],
                              schedulerTracer: SchedulerTracer,
                              queryIndexes: Array[IndexReadSession],
                              nExpressionSlots: Int,
                              prePopulateResults: Boolean,
                              subscriber: QuerySubscriber,
                              doProfile: Boolean): (QuerySubscription, QueryProfile)

  /**
    * Assert that all resources that have been acquired for query execution by any query have also been released
    * back to the query manager.
    */
  def assertAllReleased(): Unit
}
