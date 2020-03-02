/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

/**
 * The query state of the pipelined runtime.
 * It extends [[SlottedQueryState]] for convenience of evaluating expressions that only need to access the CypherRow.
 */
case class QueryState(queryContext: QueryContext,
                      override val params: Array[AnyValue],
                      override val subscriber: QuerySubscriber,
                      flowControl: FlowControl,
                      morselSize: Int,
                      override val queryIndexes: Array[IndexReadSession],
                      numberOfWorkers: Int,
                      nExpressionSlots: Int,
                      override val prePopulateResults: Boolean,
                      doProfile: Boolean,
                      override val input: InputDataStream)
  extends SlottedQueryState(queryContext,
    null,
    params,
    null,
    Array.empty[IndexReadSession],
    null,
    subscriber,
    NoMemoryTracker,
    prePopulateResults = prePopulateResults
  ) {

  /**
   * If more complex expressions need to be evaluated (i.e. not just accessing the CypherRow, use this method to obtain
   * a suitable QueryState for that.
   * @param resources the resources of the current worker
   */
  def queryStateForExpressionEvaluation(resources: QueryResources): SlottedQueryState = {
    new SlottedQueryState(queryContext,
      null,
      params,
      resources.expressionCursors,
      Array.empty[IndexReadSession],
      resources.expressionVariables(nExpressionSlots),
      subscriber,
      NoMemoryTracker)
  }
}
