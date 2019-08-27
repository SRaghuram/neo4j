/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.{NO_ENTITY_FUNCTION, makeGetPrimitiveNodeFromSlotFunctionFor}
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.NO_PREDICATE_OFFSET
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{RelationshipTypes, VarLengthExpandPipe}
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.{NULL_ENTITY, entityIsNull}
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.values.storable.Values

class VarExpandOperator(val workIdentity: WorkIdentity,
                        fromSlot: Slot,
                        relOffset: Int,
                        toSlot: Slot,
                        dir: SemanticDirection,
                        projectedDir: SemanticDirection,
                        types: RelationshipTypes,
                        minLength: Int,
                        maxLength: Int,
                        shouldExpandAll: Boolean,
                        tempNodeOffset: Int,
                        tempRelationshipOffset: Int,
                        nodePredicate: Expression,
                        relationshipPredicate: Expression) extends StreamingOperator {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================

  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot, throwOnTypeError = false)
  private val getToNodeFunction =
    if (shouldExpandAll) NO_ENTITY_FUNCTION // We only need this getter in the ExpandInto case
    else makeGetPrimitiveNodeFromSlotFunctionFor(toSlot, throwOnTypeError = false)
  private val toOffset = toSlot.offset
  private val projectBackwards = VarLengthExpandPipe.projectBackwards(dir, projectedDir)

  //===========================================================================
  // Runtime code
  //===========================================================================

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

    private var validInput: Boolean = false
    private var varExpandCursor: VarExpandCursor = _
    private var predicateState: OldQueryState = _

    override protected def enterOperate(context: QueryContext, state: QueryState, resources: QueryResources): Unit = {
      if (tempNodeOffset != NO_PREDICATE_OFFSET || tempRelationshipOffset != NO_PREDICATE_OFFSET) {
        predicateState = new OldQueryState(context,
                                           resources = null,
                                           params = state.params,
                                           resources.expressionCursors,
                                           Array.empty[IndexReadSession],
                                           resources.expressionVariables(state.nExpressionSlots),
                                           state.subscriber,
                                           NoMemoryTracker)
      }
    }

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val fromNode = getFromNodeFunction.applyAsLong(inputMorsel)
      val toNode = getToNodeFunction.applyAsLong(inputMorsel)

      val nodeVarExpandPredicate =
        if (tempNodeOffset != NO_PREDICATE_OFFSET) {
          new VarExpandPredicate[Long] {
            override def isTrue(nodeId: Long): Boolean = {
              val value = context.nodeById(nodeId)
              predicateState.expressionVariables(tempNodeOffset) = value
              nodePredicate(inputMorsel, predicateState) eq Values.TRUE
            }
          }
        } else {
          VarExpandPredicate.NO_NODE_PREDICATE
        }

      val relVarExpandPredicate =
        if (tempRelationshipOffset != NO_PREDICATE_OFFSET) {
          new VarExpandPredicate[RelationshipSelectionCursor] {
            override def isTrue(cursor: RelationshipSelectionCursor): Boolean = {
              val value = VarExpandCursor.relationshipFromCursor(context, cursor)
              predicateState.expressionVariables(tempRelationshipOffset) = value
              relationshipPredicate(inputMorsel, predicateState) eq Values.TRUE
            }
          }
        } else {
          VarExpandPredicate.NO_RELATIONSHIP_PREDICATE
        }

      if (!nodeVarExpandPredicate.isTrue(fromNode) || (!shouldExpandAll && entityIsNull(toNode))) {
        false
      } else if (entityIsNull(fromNode)) {
        validInput = false
        varExpandCursor = null
        true
      } else {
        validInput = true
        varExpandCursor = new VarExpandCursor(fromNode,
                                              toNode,
                                              resources.cursorPools,
                                              dir,
                                              projectBackwards,
                                              types.types(context),
                                              minLength,
                                              maxLength,
                                              context.transactionalContext.dataRead,
                                              context,
                                              nodeVarExpandPredicate,
                                              relVarExpandPredicate)
        true
      }
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                           context: QueryContext,
                           state: QueryState): Unit = {

      if (!validInput) {
        outputRow.copyFrom(inputMorsel)
        if (shouldExpandAll) {
          outputRow.setLongAt(toOffset, NULL_ENTITY)
        }
        outputRow.setRefAt(relOffset, Values.NO_VALUE)
        outputRow.moveToNextRow()
        return
      }

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
