/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.physical_planning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.{BaseExecutionResultBuilderFactory, ExecutionResultBuilder}
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.result.QueryResult
import org.neo4j.values.virtual.MapValue

class SlottedExecutionResultBuilderFactory(pipe: Pipe,
                                           queryIndexes: QueryIndexes,
                                           readOnly: Boolean,
                                           columns: Seq[String],
                                           logicalPlan: LogicalPlan,
                                           pipelines: SlotConfigurations,
                                           lenientCreateRelationship: Boolean,
                                           hasLoadCSV: Boolean = false)
  extends BaseExecutionResultBuilderFactory(pipe, readOnly, columns, logicalPlan, hasLoadCSV) {

  override def create(queryContext: QueryContext): ExecutionResultBuilder = SlottedExecutionResultBuilder(queryContext)

  case class SlottedExecutionResultBuilder(queryContext: QueryContext) extends BaseExecutionResultBuilder {

    val cursors = new ExpressionCursors(queryContext.transactionalContext.cursors)
    override protected def createQueryState(params: MapValue, prePopulateResults: Boolean, input: InputDataStream): SlottedQueryState = {
      new SlottedQueryState(queryContext,
                            externalResource,
                            params,
                            cursors,
                            queryIndexes.indexes.map(index => queryContext.transactionalContext.dataRead.indexReadSession(index)),
                            pipeDecorator,
                            lenientCreateRelationship = lenientCreateRelationship,
                            prePopulateResults = prePopulateResults,
                            input = input)
    }

    override def buildResultIterator(results: Iterator[ExecutionContext], readOnly: Boolean): IteratorBasedResult = {
      IteratorBasedResult(results, Some(results.asInstanceOf[Iterator[QueryResult.Record]]))
    }
  }

}
