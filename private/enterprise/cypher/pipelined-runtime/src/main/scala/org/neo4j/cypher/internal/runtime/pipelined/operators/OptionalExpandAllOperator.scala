/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.execution.{CursorPools, MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.{ExecutionContext, NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.values.storable.Values

class OptionalExpandAllOperator(val workIdentity: WorkIdentity,
                                fromSlot: Slot,
                                relOffset: Int,
                                toOffset: Int,
                                dir: SemanticDirection,
                                types: RelationshipTypes,
                                maybeExpression: Option[Expression]) extends StreamingOperator {



  override def toString: String = "OptionalExpandAll"

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    maybeExpression match {
      case None => IndexedSeq(new OptionalExpandAllTask(inputMorsel.nextCopy))
      case Some(expression) => IndexedSeq(new FilteringOptionalExpandAllTask(inputMorsel.nextCopy, expression))
    }
  }

  class OptionalExpandAllTask(inputMorsel: MorselExecutionContext) extends ExpandAllTask(inputMorsel,
                                                                                         workIdentity,
                                                                                         fromSlot,
                                                                                         relOffset,
                                                                                         toOffset,
                                                                                         dir,
                                                                                         types) {

    override def toString: String = "OptionalExpandAllTask"

    protected var hasWritten = false

    protected def setUp(context: QueryContext,
                                 state: QueryState,
                                 resources: QueryResources): Unit = {

    }

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val fromNode = getFromNodeFunction.applyAsLong(inputMorsel)
      hasWritten = false
      if (entityIsNull(fromNode)) {
        relationships = RelationshipSelectionCursor.EMPTY
      } else {
        setUp(context, state, resources)
        val pools: CursorPools = resources.cursorPools
        nodeCursor = pools.nodeCursorPool.allocateAndTrace()
        relationships = getRelationshipsCursor(context, pools, fromNode, dir, types.types(context))
      }
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {

      while (outputRow.isValidRow && relationships.next()) {
        hasWritten = writeRow(outputRow,
                              relationships.relationshipReference(),
                              relationships.otherNodeReference())
      }
      if (outputRow.isValidRow && !hasWritten) {
        writeNullRow(outputRow)
        hasWritten = true
      }
    }

    private def writeNullRow(outputRow: MorselExecutionContext): Unit = {
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, -1)
      outputRow.setLongAt(toOffset, -1)
      outputRow.moveToNextRow()
    }

    protected def writeRow(outputRow: MorselExecutionContext, relId: Long, otherSide: Long): Boolean = {
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, relId)
      outputRow.setLongAt(toOffset, otherSide)
      outputRow.moveToNextRow()
      true
    }
  }

  class FilteringOptionalExpandAllTask(inputMorsel: MorselExecutionContext,
                                       predicate: Expression)
    extends OptionalExpandAllTask(inputMorsel: MorselExecutionContext) {

    private var expressionState: SlottedQueryState = _

    override protected def setUp(context: QueryContext,
                                 state: QueryState,
                                 resources: QueryResources): Unit = {
      expressionState = new SlottedQueryState(context,
                                              resources = null,
                                              params = state.params,
                                              resources.expressionCursors,
                                              Array.empty[IndexReadSession],
                                              resources.expressionVariables(state.nExpressionSlots),
                                              state.subscriber,
                                              NoMemoryTracker)
    }

    override protected def writeRow(outputRow: MorselExecutionContext,
                                    relId: Long, otherSide: Long): Boolean = {

      //preemptively write the row, if predicate fails it will be overwritten
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, relId)
      outputRow.setLongAt(toOffset, otherSide)
      if (predicate.apply(outputRow, expressionState) eq Values.TRUE) {
        outputRow.moveToNextRow()
        true
      } else {
        false
      }
    }
  }
}




