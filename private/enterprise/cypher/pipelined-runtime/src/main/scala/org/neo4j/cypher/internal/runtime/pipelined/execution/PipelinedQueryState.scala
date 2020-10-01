/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NullPipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.profiler.Profiler
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

/**
 * The query state of the pipelined runtime.
 * It extends [[QueryState]] for convenience of evaluating expressions that only need to access the CypherRow.
 */
case class PipelinedQueryState(queryContext: QueryContext,
                               override val params: Array[AnyValue],
                               override val subscriber: QuerySubscriber,
                               flowControl: FlowControl,
                               morselSize: Int,
                               override val queryIndexes: Array[IndexReadSession],
                               numberOfWorkers: Int,
                               nExpressionSlots: Int,
                               override val prePopulateResults: Boolean,
                               doProfile: Boolean,
                               override val input: InputDataStream,
                               override val memoryTracker: QueryMemoryTracker)
  extends QueryState(queryContext,
    null,
    params,
    null,
    queryIndexes,
    null,
    subscriber,
    memoryTracker,
    prePopulateResults = prePopulateResults
  ) {

  /**
   * If more complex expressions need to be evaluated (i.e. not just accessing the CypherRow, use this method to obtain
   * a suitable QueryState for that.
   * @param resources the resources of the current worker
   */
  def queryStateForExpressionEvaluation(resources: QueryResources): QueryState = {

    val pipeDecorator =
      if (doProfile) {
        val profileInformation = resources.profileInformation
        new Profiler(queryContext.transactionalContext.dbmsInfo, profileInformation)
      } else {
        NullPipeDecorator
      }

    new QueryState(queryContext,
      null,
      params,
      resources.expressionCursors,
      queryIndexes,
      resources.expressionVariables(nExpressionSlots),
      subscriber,
      memoryTracker,
      pipeDecorator,
      initialContext = None,
      cachedIn = cachedIn)
  }
}
