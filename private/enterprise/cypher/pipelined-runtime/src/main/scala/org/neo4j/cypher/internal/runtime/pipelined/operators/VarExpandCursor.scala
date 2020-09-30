/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.GrowingArray
import org.neo4j.cypher.internal.runtime.pipelined.execution.CursorPools
import org.neo4j.cypher.internal.runtime.pipelined.operators.VarExpandCursor.relationshipFromCursor
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingCursor
import org.neo4j.kernel.impl.newapi.Cursors.emptyTraversalCursor
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualValues

sealed trait ExpandStatus
case object NOT_STARTED extends ExpandStatus
case object EXPAND extends ExpandStatus
case object EMIT extends ExpandStatus

abstract class VarExpandCursor(val fromNode: Long,
                               val targetToNode: Long,
                               nodeCursor: NodeCursor,
                               projectBackwards: Boolean,
                               relTypes: Array[Int],
                               minLength: Int,
                               maxLength: Int,
                               read: Read,
                               executionContext: CypherRow,
                               dbAccess: DbAccess,
                               params: Array[AnyValue],
                               cursors: ExpressionCursors,
                               expressionVariables: Array[AnyValue],
                               val memoryTracker: MemoryTracker) {

  private var expandStatus: ExpandStatus = NOT_STARTED
  private var pathLength: Int = 0
  private var event: OperatorProfileEvent = _

  private val relTraversalCursors: GrowingArray[RelationshipTraversalCursor] = new GrowingArray[RelationshipTraversalCursor](memoryTracker)
  private val selectionCursors: GrowingArray[RelationshipTraversalCursor] = new GrowingArray[RelationshipTraversalCursor](memoryTracker)

  // this needs to be explicitly managed on every work unit, to avoid parallel workers accessing each others cursorPools.
  private var cursorPools: CursorPools = _

  protected def selectionCursor(traversalCursor: RelationshipTraversalCursor,
                                node: NodeCursor,
                                types: Array[Int]): RelationshipTraversalCursor

  //extension point
  protected def satisfyPredicates(executionContext: CypherRow,
                                  dbAccess: DbAccess,
                                  params: Array[AnyValue],
                                  cursors: ExpressionCursors,
                                  expressionVariables: Array[AnyValue],
                                  selectionCursor: RelationshipTraversalCursor): Boolean

  def enterWorkUnit(cursorPools: CursorPools): Unit = {
    this.cursorPools = cursorPools
  }

  def next(): Boolean = {

    if (expandStatus == NOT_STARTED) {
      expandStatus = if (pathLength < maxLength) EXPAND else EMIT
      if (minLength == 0 && validToNode) {
        return true
      }
    }

    while (pathLength > 0 || expandStatus == EXPAND) {
      expandStatus match {
        case EXPAND =>
          expand(toNode)
          pathLength += 1
          expandStatus = EMIT

        case EMIT =>
          val r = pathLength - 1
          val selectionCursor = selectionCursors.get(r)
          var hasNext = false
          do {
            hasNext = selectionCursor.next()
          } while (hasNext &&
            (!relationshipIsUniqueInPath(r, selectionCursor.relationshipReference()) ||
              !satisfyPredicates(executionContext, dbAccess, params, cursors, expressionVariables, selectionCursor)
              ))

          if (hasNext) {
            if (pathLength < maxLength) {
              expandStatus = EXPAND
            }
            if (pathLength >= minLength && validToNode) {
              return true
            }
          } else {
            expandStatus = EMIT
            pathLength -= 1
          }
      }
    }
    false
  }

  private def validToNode: Boolean = targetToNode < 0 || targetToNode == toNode

  private def relationshipIsUniqueInPath(relInPath: Int, relationshipId: Long): Boolean = {
    var i = relInPath - 1
    while (i >= 0) {
      if (selectionCursors.get(i).relationshipReference() == relationshipId)
        return false
      i -= 1
    }
    true
  }

  private def expand(node: Long): Unit = {

    read.singleNode(node, nodeCursor)
    val cursor =
      if (!nodeCursor.next()) {
        emptyTraversalCursor(read)
      } else {
        val traversalCursor = relTraversalCursors.computeIfAbsent(pathLength, () => {
          val cursor = cursorPools.relationshipTraversalCursorPool.allocateAndTrace()
          cursor.setTracer(event)
          cursor
        })
        selectionCursor(traversalCursor, nodeCursor, relTypes)
      }
    selectionCursors.set(pathLength, cursor)
  }

  def toNode: Long = {
    if (selectionCursors.hasNeverSeenData) {
      fromNode
    } else {
      selectionCursors.get(pathLength-1).otherNodeReference()
    }
  }

  def relationships: ListValue = {
    val r = new Array[AnyValue](pathLength)
    if (projectBackwards) {
      var i = pathLength - 1
      while (i >= 0) {
        val cursor = selectionCursors.get(i)
        r(pathLength - 1 - i) = relationshipFromCursor(dbAccess, cursor)
        i -= 1
      }
    } else {
      var i = 0
      while (i < pathLength) {
        val cursor = selectionCursors.get(i)
        r(i) = relationshipFromCursor(dbAccess, cursor)
        i += 1
      }
    }
    VirtualValues.list(r:_*)
  }

  def setTracer(event: OperatorProfileEvent): Unit = {
    this.event = event
    nodeCursor.setTracer(event)
    relTraversalCursors.foreach(_.setTracer(event))
  }

  def free(cursorPools: CursorPools): Unit = {
    cursorPools.nodeCursorPool.free(nodeCursor)
    relTraversalCursors.foreach(cursor => cursorPools.relationshipTraversalCursorPool.free(cursor))
    if (relTraversalCursors != null) {
      relTraversalCursors.close()
    }
    if (selectionCursors != null) {
      selectionCursors.close()
    }
  }
}

object VarExpandCursor {
  def apply(direction: SemanticDirection,
            fromNode: Long,
            targetToNode: Long,
            nodeCursor: NodeCursor,
            projectBackwards: Boolean,
            relTypes: Array[Int],
            minLength: Int,
            maxLength: Int,
            read: Read,
            dbAccess: DbAccess,
            nodePredicate: VarExpandPredicate[Long],
            relationshipPredicate: VarExpandPredicate[RelationshipTraversalCursor],
            memoryTracker: MemoryTracker): VarExpandCursor = direction match {
    case SemanticDirection.OUTGOING =>
      new OutgoingVarExpandCursor(fromNode,
        targetToNode,
        nodeCursor,
        projectBackwards,
        relTypes,
        minLength,
        maxLength,
        read,
        null,
        dbAccess,
        null,
        null,
        null,
        memoryTracker) {

        override protected def satisfyPredicates(executionContext: CypherRow,
                                                 dbAccess: DbAccess,
                                                 params: Array[AnyValue],
                                                 cursors: ExpressionCursors,
                                                 expressionVariables: Array[AnyValue],
                                                 selectionCursor: RelationshipTraversalCursor): Boolean =
          relationshipPredicate.isTrue(selectionCursor) && nodePredicate.isTrue(selectionCursor.otherNodeReference())
      }
    case SemanticDirection.INCOMING =>
      new IncomingVarExpandCursor(fromNode,
        targetToNode,
        nodeCursor,
        projectBackwards,
        relTypes,
        minLength,
        maxLength,
        read,
        null,
        dbAccess,
        null,
        null,
        null,
        memoryTracker) {
        override protected def satisfyPredicates(executionContext: CypherRow,
                                                 dbAccess: DbAccess,
                                                 params: Array[AnyValue],
                                                 cursors: ExpressionCursors,
                                                 expressionVariables: Array[AnyValue],
                                                 traversalCursor: RelationshipTraversalCursor): Boolean =
          relationshipPredicate.isTrue(traversalCursor) && nodePredicate.isTrue(traversalCursor.otherNodeReference())
      }
    case SemanticDirection.BOTH =>
      new AllVarExpandCursor(fromNode,
        targetToNode,
        nodeCursor,
        projectBackwards,
        relTypes,
        minLength,
        maxLength,
        read,
        null,
        dbAccess,
        null,
        null,
        null,
        memoryTracker) {
        override protected def satisfyPredicates(executionContext: CypherRow,
                                                 dbAccess: DbAccess,
                                                 params: Array[AnyValue],
                                                 cursors: ExpressionCursors,
                                                 expressionVariables: Array[AnyValue],
                                                 selectionCursor: RelationshipTraversalCursor): Boolean =
          relationshipPredicate.isTrue(selectionCursor) && nodePredicate.isTrue(selectionCursor.otherNodeReference())
      }
  }

  def relationshipFromCursor(dbAccess: DbAccess, cursor: RelationshipTraversalCursor): RelationshipValue = {
    dbAccess.relationshipById(cursor.relationshipReference(),
      cursor.sourceNodeReference(),
      cursor.targetNodeReference(),
      cursor.`type`())
  }
}

trait VarExpandPredicate[ENTITY] {
  def isTrue(entity: ENTITY): Boolean
}

object VarExpandPredicate {
  val NO_NODE_PREDICATE: VarExpandPredicate[Long] = (_: Long) => true
  val NO_RELATIONSHIP_PREDICATE: VarExpandPredicate[RelationshipTraversalCursor] = (_: RelationshipTraversalCursor) => true
}

abstract class OutgoingVarExpandCursor(override val fromNode: Long,
                                       targetToNode: Long,
                                       nodeCursor: NodeCursor,
                                       projectBackwards: Boolean,
                                       relTypes: Array[Int],
                                       minLength: Int,
                                       maxLength: Int,
                                       read: Read,
                                       executionContext: CypherRow,
                                       dbAccess: DbAccess,
                                       params: Array[AnyValue],
                                       cursors: ExpressionCursors,
                                       expressionVariables: Array[AnyValue],
                                       memoryTracker: MemoryTracker)
  extends VarExpandCursor(fromNode,
    targetToNode,
    nodeCursor,
    projectBackwards,
    relTypes,
    minLength,
    maxLength,
    read,
    executionContext,
    dbAccess,
    params,
    cursors,
    expressionVariables,
    memoryTracker) {

  override protected def selectionCursor(traversalCursor: RelationshipTraversalCursor,
                                         node: NodeCursor,
                                         types: Array[Int]): RelationshipTraversalCursor = outgoingCursor(traversalCursor, node, types)
}

abstract class IncomingVarExpandCursor(fromNode: Long,
                                       targetToNode: Long,
                                       nodeCursor: NodeCursor,
                                       projectBackwards: Boolean,
                                       relTypes: Array[Int],
                                       minLength: Int,
                                       maxLength: Int,
                                       read: Read,
                                       executionContext: CypherRow,
                                       dbAccess: DbAccess,
                                       params: Array[AnyValue],
                                       cursors: ExpressionCursors,
                                       expressionVariables: Array[AnyValue],
                                       memoryTracker: MemoryTracker)
  extends VarExpandCursor(fromNode,
    targetToNode,
    nodeCursor,
    projectBackwards,
    relTypes,
    minLength,
    maxLength,
    read,
    executionContext,
    dbAccess,
    params,
    cursors,
    expressionVariables,
    memoryTracker) {

  override protected def selectionCursor(traversalCursor: RelationshipTraversalCursor,
                                         node: NodeCursor,
                                         types: Array[Int]): RelationshipTraversalCursor = {

    incomingCursor(traversalCursor, node, types)
  }
}

abstract class AllVarExpandCursor(fromNode: Long,
                                  targetToNode: Long,
                                  nodeCursor: NodeCursor,
                                  projectBackwards: Boolean,
                                  relTypes: Array[Int],
                                  minLength: Int,
                                  maxLength: Int,
                                  read: Read,
                                  executionContext: CypherRow,
                                  dbAccess: DbAccess,
                                  params: Array[AnyValue],
                                  cursors: ExpressionCursors,
                                  expressionVariables: Array[AnyValue],
                                  memoryTracker: MemoryTracker)
  extends VarExpandCursor(fromNode,
    targetToNode,
    nodeCursor,
    projectBackwards,
    relTypes,
    minLength,
    maxLength,
    read,
    executionContext,
    dbAccess,
    params,
    cursors,
    expressionVariables,
    memoryTracker) {

  override protected def selectionCursor(traversalCursor: RelationshipTraversalCursor,
                                         node: NodeCursor,
                                         types: Array[Int]): RelationshipTraversalCursor = allCursor(traversalCursor, node, types)
}