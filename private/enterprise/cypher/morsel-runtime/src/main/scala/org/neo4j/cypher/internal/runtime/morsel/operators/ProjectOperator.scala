/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.runtime.{NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.internal.kernel.api.IndexReadSession

class ProjectOperator(val workIdentity: WorkIdentity,
                      val projectionOps: CommandProjection) extends StatelessOperator {



  override def operate(currentRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       resources: QueryResources): Unit = {

    val queryState = new OldQueryState(context,
                                           resources = null,
                                           params = state.params,
                                           resources.expressionCursors,
                                           Array.empty[IndexReadSession],
                                           resources.expressionVariables(state.nExpressionSlots),
                                           state.subscriber,
                                           NoMemoryTracker)

    while (currentRow.isValidRow) {
      projectionOps.project(currentRow, queryState)
      currentRow.moveToNextRow()
    }
  }
}
