/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.config.MemoryTracking
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.pipelined.ExecutablePipeline
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SchedulerTracer
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
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
                              doProfile: Boolean,
                              morselSize: Int,
                              memoryTracking: MemoryTracking,
                              executionGraphSchedulingPolicy: ExecutionGraphSchedulingPolicy): ProfiledQuerySubscription

  /**
   * Assert that all resources that have been acquired for query execution by any query have also been released
   * back to the query manager.
   */
  def assertAllReleased(): Unit

  /**
   * Wait until all query workers have completed ongoing work according to current demand and settled down in an idle state
   *
   * @param timeoutMs Timeout in ms
   * @return true if all workers settled in an idle state, or false if the timeout occurred
   */
  def waitForWorkersToIdle(timeoutMs: Int): Boolean
}

case class ProfiledQuerySubscription(executingQuery: ExecutingQuery, profile: QueryProfile, memoryTracker: QueryMemoryTracker, mutableQueryStatistics: MutableQueryStatistics)