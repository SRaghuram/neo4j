/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.MemoryTrackingController
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.createParameterArray
import org.neo4j.cypher.internal.runtime.interpreted.BaseExecutionResultBuilderFactory
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionResultBuilder
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

class SlottedExecutionResultBuilderFactory(pipe: Pipe,
                                           queryIndexes: QueryIndexes,
                                           nExpressionSlots: Int,
                                           readOnly: Boolean,
                                           columns: Seq[String],
                                           logicalPlan: LogicalPlan,
                                           pipelines: SlotConfigurations,
                                           parameterMapping: ParameterMapping,
                                           lenientCreateRelationship: Boolean,
                                           memoryTrackingController: MemoryTrackingController,
                                           hasLoadCSV: Boolean = false)
  extends BaseExecutionResultBuilderFactory(pipe, readOnly, columns, logicalPlan, hasLoadCSV) {

  override def create(queryContext: QueryContext): ExecutionResultBuilder = SlottedExecutionResultBuilder(queryContext)

  case class SlottedExecutionResultBuilder(queryContext: QueryContext) extends BaseExecutionResultBuilder {

    val transactionMemoryTracker: MemoryTracker = queryContext.transactionalContext.transaction.memoryTracker()
    val cursors = new ExpressionCursors(queryContext.transactionalContext.cursors, queryContext.transactionalContext.transaction.pageCursorTracer(), transactionMemoryTracker)
    queryContext.resources.trace(cursors)
    override protected def createQueryState(params: MapValue,
                                            prePopulateResults: Boolean,
                                            input: InputDataStream,
                                            subscriber: QuerySubscriber,
    doProfile: Boolean): QueryState = {

      new QueryState(queryContext,
        externalResource,
        createParameterArray(params, parameterMapping),
        cursors,
        queryIndexes.initiateLabelAndSchemaIndexes(queryContext),
        new Array[AnyValue](nExpressionSlots),
        subscriber,
        QueryMemoryTracker(memoryTrackingController.memoryTracking(doProfile), transactionMemoryTracker),
        pipeDecorator,
        lenientCreateRelationship = lenientCreateRelationship,
        prePopulateResults = prePopulateResults,
        input = input)
    }
  }
}
