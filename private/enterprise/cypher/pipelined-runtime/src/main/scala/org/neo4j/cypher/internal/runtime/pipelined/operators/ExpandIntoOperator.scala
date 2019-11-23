/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.execution.{CursorPools, MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.operations.ExpandIntoCursors
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor

class ExpandIntoOperator(val workIdentity: WorkIdentity,
                        fromSlot: Slot,
                        relOffset: Int,
                        toSlot: Slot,
                        dir: SemanticDirection,
                        types: RelationshipTypes) extends StreamingOperator {
  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)
  private val getToNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(toSlot)

  override def toString: String = "ExpandInto"

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    IndexedSeq(new OTask(inputMorsel.nextCopy))

  class OTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def workIdentity: WorkIdentity = ExpandIntoOperator.this.workIdentity

    override def toString: String = "ExpandIntoTask"

    private var nodeCursor: NodeCursor = _
    private var groupCursor: RelationshipGroupCursor = _
    private var traversalCursor: RelationshipTraversalCursor = _
    private var relationships: RelationshipSelectionCursor = _

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val fromNode = getFromNodeFunction.applyAsLong(inputMorsel)
      val toNode = getToNodeFunction.applyAsLong(inputMorsel)
      if (entityIsNull(fromNode) || entityIsNull(toNode))
        false
      else {
        val pools: CursorPools = resources.cursorPools
        nodeCursor = pools.nodeCursorPool.allocateAndTrace()
        val d = dir match {
          case SemanticDirection.OUTGOING => Direction.OUTGOING
          case SemanticDirection.INCOMING => Direction.INCOMING
          case SemanticDirection.BOTH => Direction.BOTH
        }
        groupCursor = pools.relationshipGroupCursorPool.allocateAndTrace()
        traversalCursor = pools.relationshipTraversalCursorPool.allocateAndTrace()
        relationships = ExpandIntoCursors.connectingRelationships(context.transactionalContext.dataRead,
                                                                  nodeCursor,
                                                                  groupCursor, traversalCursor,
                                                                  fromNode,
                                                                  d,
                                                                  toNode,
                                                                  types.types(context))
        true
      }
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {

      while (outputRow.isValidRow && relationships.next()) {
        val relId = relationships.relationshipReference()

        // Now we have everything needed to create a row.
        outputRow.copyFrom(inputMorsel)
        outputRow.setLongAt(relOffset, relId)
        outputRow.moveToNextRow()
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      if (relationships != null) {
        nodeCursor.setTracer(event)
        relationships.setTracer(event)
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      val pools = resources.cursorPools
      pools.nodeCursorPool.free(nodeCursor)
      pools.relationshipGroupCursorPool.free(groupCursor)
      pools.relationshipTraversalCursorPool.free(traversalCursor)
      nodeCursor = null
      groupCursor = null
      traversalCursor = null
      relationships = null
    }
  }
}




