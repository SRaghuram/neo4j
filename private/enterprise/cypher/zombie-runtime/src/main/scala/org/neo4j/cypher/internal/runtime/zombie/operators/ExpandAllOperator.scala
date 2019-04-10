/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyTypes
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.{allCursor, incomingCursor, outgoingCursor}
import org.neo4j.internal.kernel.api.{NodeCursor, RelationshipGroupCursor, RelationshipTraversalCursor}

class ExpandAllOperator(val workIdentity: WorkIdentity,
                        fromOffset: Int,
                        relOffset: Int,
                        toOffset: Int,
                        dir: SemanticDirection,
                        types: LazyTypes) extends StreamingOperator {

  override def toString: String = "ExpandAll"

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    IndexedSeq(new OTask(inputMorsel.nextCopy))

  class OTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = "ExpandAllTask"

    /*
    This might look wrong, but it's like this by design. This allows the loop to terminate early and still be
    picked up at any point again - all without impacting the tight loop.
    The mutable state is an unfortunate cost for this feature.
     */
    private var nodeCursor: NodeCursor = _
    private var groupCursor: RelationshipGroupCursor = _
    private var traversalCursor: RelationshipTraversalCursor = _
    private var relationships: RelationshipSelectionCursor = _

    protected override def initializeInnerLoop(context: QueryContext, state: QueryState, resources: QueryResources): Boolean = {
      val fromNode = inputMorsel.getLongAt(fromOffset)
      if (entityIsNull(fromNode))
        false
      else {
        nodeCursor = resources.cursorPools.nodeCursorPool.allocate()
        groupCursor = resources.cursorPools.relationshipGroupCursorPool.allocate()
        traversalCursor = resources.cursorPools.relationshipTraversalCursorPool.allocate()
        relationships = getRelationshipsCursor(context, fromNode, dir, types.types(context))
        true
      }
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                           context: QueryContext,
                           state: QueryState): Unit = {

      while (outputRow.isValidRow && relationships.next()) {
        val relId = relationships.relationshipReference()
        val otherSide = relationships.otherNodeReference()

        // Now we have everything needed to create a row.
        outputRow.copyFrom(inputMorsel)
        outputRow.setLongAt(relOffset, relId)
        outputRow.setLongAt(toOffset, otherSide)
        outputRow.moveToNextRow()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      val pools = resources.cursorPools
      pools.nodeCursorPool.free(nodeCursor)
      pools.relationshipGroupCursorPool.free(groupCursor)
      pools.relationshipTraversalCursorPool.free(traversalCursor)
      relationships = null
    }

    private def getRelationshipsCursor(context: QueryContext,
                                       node: Long,
                                       dir: SemanticDirection,
                                       types: Option[Array[Int]]): RelationshipSelectionCursor = {

      val read = context.transactionalContext.dataRead
      read.singleNode(node, nodeCursor)
      if (!nodeCursor.next()) RelationshipSelectionCursor.EMPTY
      else {
        dir match {
          case OUTGOING => outgoingCursor(groupCursor, traversalCursor, nodeCursor, types.orNull)
          case INCOMING => incomingCursor(groupCursor, traversalCursor, nodeCursor, types.orNull)
          case BOTH => allCursor(groupCursor, traversalCursor, nodeCursor, types.orNull)
        }
      }
    }
  }

//  class OTaskTemplate(val inner: OperatorTaskTemplate, val inputMorsel: MorselExecutionContext) extends InputLoopTaskTemplate {
//
//    override def toString: String = "ExpandAllTaskTemplate"
//
//    /*
//    This might look wrong, but it's like this by design. This allows the loop to terminate early and still be
//    picked up at any point again - all without impacting the tight loop.
//    The mutable state is an unfortunate cost for this feature.
//     */
//    //private var nodeCursor: NodeCursor = _
//    //private var groupCursor: RelationshipGroupCursor = _
//    //private var traversalCursor: RelationshipTraversalCursor = _
//    //private var relationships: RelationshipSelectionCursor = _
//    protected override def genFields: Seq[Field] = {
//      val fields = super.genFields
//      fields
//      fields ++ inner.genFields
//    }
//
//    protected override def genInitializeInnerLoop() = {
//      //val fromNode = inputMorsel.getLongAt(fromOffset)
//      //if (entityIsNull(fromNode))
//      //  false
//      //else {
//      //  nodeCursor = resources.cursorPools.nodeCursorPool.allocate()
//      //  groupCursor = resources.cursorPools.relationshipGroupCursorPool.allocate()
//      //  traversalCursor = resources.cursorPools.relationshipTraversalCursorPool.allocate()
//      //  relationships = getRelationshipsCursor(context, fromNode, dir, types.types(context))
//      //  true
//      //}
//      constant(true)
//    }
//
//    override protected def genInnerLoop(outputRow: MorselExecutionContext,
//                           context: QueryContext,
//                           state: QueryState): Unit = {
//
//      while (outputRow.isValidRow && relationships.next()) {
//        val relId = relationships.relationshipReference()
//        val otherSide = relationships.otherNodeReference()
//
//        // Now we have everything needed to create a row.
//        outputRow.copyFrom(inputMorsel)
//        outputRow.setLongAt(relOffset, relId)
//        outputRow.setLongAt(toOffset, otherSide)
//        outputRow.moveToNextRow()
//      }
//    }
//
//    override protected def closeInnerLoop(resources: QueryResources): Unit = {
//      val pools = resources.cursorPools
//      pools.nodeCursorPool.free(nodeCursor)
//      pools.relationshipGroupCursorPool.free(groupCursor)
//      pools.relationshipTraversalCursorPool.free(traversalCursor)
//      relationships = null
//    }
//
//    private def getRelationshipsCursor(context: QueryContext,
//                                       node: Long,
//                                       dir: SemanticDirection,
//                                       types: Option[Array[Int]]): RelationshipSelectionCursor = {
//
//      val read = context.transactionalContext.dataRead
//      read.singleNode(node, nodeCursor)
//      if (!nodeCursor.next()) RelationshipSelectionCursor.EMPTY
//      else {
//        dir match {
//          case OUTGOING => outgoingCursor(groupCursor, traversalCursor, nodeCursor, types.orNull)
//          case INCOMING => incomingCursor(groupCursor, traversalCursor, nodeCursor, types.orNull)
//          case BOTH => allCursor(groupCursor, traversalCursor, nodeCursor, types.orNull)
//        }
//      }
//    }
//  }
}
