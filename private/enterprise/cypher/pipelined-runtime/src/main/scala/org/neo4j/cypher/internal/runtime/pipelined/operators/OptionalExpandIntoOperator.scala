/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.internal.kernel.api.helpers.{CachingExpandInto, RelationshipSelectionCursor}
import org.neo4j.values.storable.Values

class OptionalExpandIntoOperator(val workIdentity: WorkIdentity,
                                fromSlot: Slot,
                                relOffset: Int,
                                toSlot: Slot,
                                dir: SemanticDirection,
                                types: RelationshipTypes,
                                maybeExpression: Option[Expression]) extends StreamingOperator {



  override def toString: String = "OptionalExpandInto"

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    maybeExpression match {
      case None => IndexedSeq(new OptionalExpandIntoTask(inputMorsel.nextCopy))
      case Some(expression) => IndexedSeq(new FilteringOptionalExpandIntoTask(inputMorsel.nextCopy, expression))
    }
  }

  class OptionalExpandIntoTask(inputMorsel: MorselExecutionContext) extends ExpandIntoTask(inputMorsel,
                                                                                           workIdentity,
                                                                                           fromSlot,
                                                                                           relOffset,
                                                                                           toSlot,
                                                                                           dir,
                                                                                           types) {

    override def toString: String = "OptionalExpandIntoTask"

    protected var hasWritten = false

    protected def setUp(context: QueryContext,
                        state: QueryState,
                        resources: QueryResources): Unit = {

    }

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      if (expandInto == null) {
        expandInto = new CachingExpandInto(context.transactionalContext.dataRead,
                                           kernelDirection(dir))
      }
      val fromNode = getFromNodeFunction.applyAsLong(inputMorsel)
      val toNode = getToNodeFunction.applyAsLong(inputMorsel)
      hasWritten = false
      if (entityIsNull(fromNode) || entityIsNull(toNode)) {
        relationships = RelationshipSelectionCursor.EMPTY
      } else {
        setUp(context, state, resources)
        setupCursors(context, resources, fromNode, toNode)
      }
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {

      while (outputRow.isValidRow && relationships.next()) {
        hasWritten = writeRow(outputRow,
                              relationships.relationshipReference())
      }
      if (outputRow.isValidRow && !hasWritten) {
        writeNullRow(outputRow)
        hasWritten = true
      }
    }

    private def writeNullRow(outputRow: MorselExecutionContext): Unit = {
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, -1)
      outputRow.moveToNextRow()
    }

    protected def writeRow(outputRow: MorselExecutionContext, relId: Long): Boolean = {
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, relId)
      outputRow.moveToNextRow()
      true
    }
  }

  class FilteringOptionalExpandIntoTask(inputMorsel: MorselExecutionContext,
                                       predicate: Expression)
    extends OptionalExpandIntoTask(inputMorsel: MorselExecutionContext) {

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
                                    relId: Long): Boolean = {

      //preemptively write the row, if predicate fails it will be overwritten
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, relId)
      if (predicate.apply(outputRow, expressionState) eq Values.TRUE) {
        outputRow.moveToNextRow()
        true
      } else {
        false
      }
    }
  }
}









