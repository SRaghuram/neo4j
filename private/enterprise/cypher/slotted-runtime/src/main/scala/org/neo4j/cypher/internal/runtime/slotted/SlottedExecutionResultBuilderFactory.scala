/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.IteratorBasedResult
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.executionplan.{BaseExecutionResultBuilderFactory, ExecutionResultBuilder}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.result.QueryResult
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

class SlottedExecutionResultBuilderFactory(pipe: Pipe,
                                           readOnly: Boolean,
                                           columns: List[String],
                                           logicalPlan: LogicalPlan,
                                           pipelines: SlotConfigurations,
                                           lenientCreateRelationship: Boolean)
  extends BaseExecutionResultBuilderFactory(pipe, readOnly, columns, logicalPlan) {

  override def create(queryContext: QueryContext): ExecutionResultBuilder = SlottedExecutionWorkflowBuilder(queryContext)

  case class SlottedExecutionWorkflowBuilder(queryContext: QueryContext) extends BaseExecutionWorkflowBuilder {
    override protected def createQueryState(params: MapValue, prePopulateResults: Boolean): SlottedQueryState = {
      new SlottedQueryState(queryContext,
                            externalResource,
                            params,
                            pipeDecorator,
                            triadicState = mutable.Map.empty,
                            repeatableReads = mutable.Map.empty,
                            lenientCreateRelationship = lenientCreateRelationship,
                            prePopulateResults = prePopulateResults)
    }

    override def buildResultIterator(results: Iterator[ExecutionContext], readOnly: Boolean): IteratorBasedResult = {
      IteratorBasedResult(results, Some(results.asInstanceOf[Iterator[QueryResult.Record]]))
    }
  }
}
