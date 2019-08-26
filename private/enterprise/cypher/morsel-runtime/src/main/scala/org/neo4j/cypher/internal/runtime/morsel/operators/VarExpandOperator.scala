/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection

class VarExpandOperator(val workIdentity: WorkIdentity,
                        fromOffset: Int,
                        relOffset: Int,
                        toOffset: Int,
                        dir: SemanticDirection,
                        types: RelationshipTypes,
                        minLength: Int,
                        maxLength: Int,
                        shouldExpandAll: Boolean) extends StreamingOperator {

  override def toString: String = "VarExpand"

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    IndexedSeq(new OTask(inputMorsel.nextCopy))

  class OTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def workIdentity: WorkIdentity = VarExpandOperator.this.workIdentity

    override def toString: String = "VarExpandTask"

    private var varExpandCursor: VarExpandCursor = _

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val fromNode = inputMorsel.getLongAt(fromOffset)
      val toNode = if (shouldExpandAll) -1L else inputMorsel.getLongAt(toOffset)
      if (entityIsNull(fromNode)) {
        false
      } else {
        varExpandCursor = new VarExpandCursor(fromNode,
                                              toNode,
                                              resources.cursorPools,
                                              dir,
                                              types.types(context),
                                              minLength,
                                              maxLength,
                                              context.transactionalContext.dataRead,
                                              context)
        true
      }
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                           context: QueryContext,
                           state: QueryState): Unit = {

      while (outputRow.isValidRow && varExpandCursor.next()) {
        outputRow.copyFrom(inputMorsel)
        if (shouldExpandAll) {
          outputRow.setLongAt(toOffset, varExpandCursor.toNode)
        }
        outputRow.setRefAt(relOffset, varExpandCursor.relationships)
        outputRow.moveToNextRow()
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      if (varExpandCursor != null) {
        varExpandCursor.setTracer(event)
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      if (varExpandCursor != null) {
        varExpandCursor.free(resources.cursorPools)
        varExpandCursor = null
      }
    }
  }
}
