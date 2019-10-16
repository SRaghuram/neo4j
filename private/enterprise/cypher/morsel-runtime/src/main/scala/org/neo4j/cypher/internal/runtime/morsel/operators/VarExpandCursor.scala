/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.morsel.execution.CursorPools
import org.neo4j.cypher.internal.runtime.morsel.operators.VarExpandCursor.relationshipFromCursor
import org.neo4j.cypher.internal.runtime.{DbAccess, ExecutionContext, ExpressionCursors}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.{allCursor, incomingCursor, outgoingCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{ListValue, RelationshipValue, VirtualValues}

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
                               executionContext: ExecutionContext,
                               dbAccess: DbAccess,
                               params: Array[AnyValue],
                               cursors: ExpressionCursors,
                               expressionVariables: Array[AnyValue]) {

  private var expandStatus: ExpandStatus = NOT_STARTED
  private var pathLength: Int = 0
  private var event: OperatorProfileEvent = OperatorProfileEvent.NONE

  private val relTraCursors: GrowingArray[RelationshipTraversalCursor] = new GrowingArray[RelationshipTraversalCursor]()
  private val relGroupCursors: GrowingArray[RelationshipGroupCursor] = new GrowingArray[RelationshipGroupCursor]()
  private val selectionCursors: GrowingArray[RelationshipSelectionCursor] = new GrowingArray[RelationshipSelectionCursor]()

  // this needs to be explicitly managed on every work unit, to avoid parallel workers accessing each others cursorPools.
  private var cursorPools: CursorPools = _

  protected def selectionCursor(groupCursor: RelationshipGroupCursor,
                            traversalCursor: RelationshipTraversalCursor,
                            node: NodeCursor,
                            types: Array[Int]): RelationshipSelectionCursor

  //extension point
  protected def satisfyPredicates(executionContext: ExecutionContext,
                                   dbAccess: DbAccess,
                                   params: Array[AnyValue],
                                   cursors: ExpressionCursors,
                                   expressionVariables: Array[AnyValue],
                                   selectionCursor: RelationshipSelectionCursor): Boolean

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

    val groupCursor = relGroupCursors.computeIfAbsent(pathLength, () => {
      val cursor = cursorPools.relationshipGroupCursorPool.allocateAndTrace()
      cursor.setTracer(event)
      cursor
    })
    val traversalCursor = relTraCursors.computeIfAbsent(pathLength, () => {
      val cursor = cursorPools.relationshipTraversalCursorPool.allocateAndTrace()
      cursor.setTracer(event)
      cursor
    })

    read.singleNode(node, nodeCursor)

    val cursor =
      if (!nodeCursor.next()) {
        RelationshipSelectionCursor.EMPTY
      } else selectionCursor(groupCursor, traversalCursor, nodeCursor, relTypes)


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
    relTraCursors.foreach(_.setTracer(event))
    relGroupCursors.foreach(_.setTracer(event))
  }

  def free(cursorPools: CursorPools): Unit = {
    cursorPools.nodeCursorPool.free(nodeCursor)
    relTraCursors.foreach(cursor => cursorPools.relationshipTraversalCursorPool.free(cursor))
    relGroupCursors.foreach(cursor => cursorPools.relationshipGroupCursorPool.free(cursor))
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
            relationshipPredicate: VarExpandPredicate[RelationshipSelectionCursor]): VarExpandCursor = direction match {
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
                                  null) {

        override protected def satisfyPredicates(executionContext: ExecutionContext,
                                                 dbAccess: DbAccess,
                                                 params: Array[AnyValue],
                                                 cursors: ExpressionCursors,
                                                 expressionVariables: Array[AnyValue],
                                                 selectionCursor: RelationshipSelectionCursor): Boolean =
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
                                  null) {
        override protected def satisfyPredicates(executionContext: ExecutionContext,
                                                 dbAccess: DbAccess,
                                                 params: Array[AnyValue],
                                                 cursors: ExpressionCursors,
                                                 expressionVariables: Array[AnyValue],
                                                 selectionCursor: RelationshipSelectionCursor): Boolean =
          relationshipPredicate.isTrue(selectionCursor) && nodePredicate.isTrue(selectionCursor.otherNodeReference())
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
                             null) {
        override protected def satisfyPredicates(executionContext: ExecutionContext,
                                                 dbAccess: DbAccess,
                                                 params: Array[AnyValue],
                                                 cursors: ExpressionCursors,
                                                 expressionVariables: Array[AnyValue],
                                                 selectionCursor: RelationshipSelectionCursor): Boolean =
          relationshipPredicate.isTrue(selectionCursor) && nodePredicate.isTrue(selectionCursor.otherNodeReference())
      }
  }

  def relationshipFromCursor(dbAccess: DbAccess, cursor: RelationshipSelectionCursor): RelationshipValue = {
    dbAccess.relationshipById(cursor.relationshipReference(),
                              cursor.sourceNodeReference(),
                              cursor.targetNodeReference(),
                              cursor.`type`())
  }
}

/**
  * Random access data structure which grows dynamically as elements are added.
  */
class GrowingArray[T <: AnyRef] {

  private var array: Array[AnyRef] = new Array[AnyRef](4)
  private var highWaterMark: Int = 0

  /**
    * Set an element at a given index, and grows the underlying structure if needed.
    */
  def set(index: Int, t: T): Unit = {
    ensureCapacity(index+1)
    array(index) = t
  }

  /**
    * Get the element at a given index.
    */
  def get(index: Int): T = {
    array(index).asInstanceOf[T]
  }

  /**
    * Get the element at a given index. If the element at that index is `null`,
    * instead compute a new element, set it at the index, and return it.
    *
    * This is useful for storing resources that can be reused depending on their index.
    */
  def computeIfAbsent(index: Int, compute: () => T): T = {
    ensureCapacity(index+1)
    var t = array(index)
    if (t == null) {
      t = compute()
      array(index) = t
    }
    t.asInstanceOf[T]
  }

  /**
    * Apply the given function `f` once for each element.
    *
    * If there are gaps, `f` will be called with `null` as argument.
    */
  def foreach(f: T => Unit): Unit = {
    var i = 0
    while (i < highWaterMark) {
      f(get(i))
      i += 1
    }
  }

  /**
    * Return `true` if any element has ever been set.
    */
  def hasNeverSeenData: Boolean = highWaterMark == 0

  private def ensureCapacity(size: Int): Unit = {
    if (this.highWaterMark < size) {
      this.highWaterMark = size
    }

    if (array.length < size) {
      val temp = array
      array = new Array[AnyRef](array.length * 2)
      System.arraycopy(temp, 0, array, 0, temp.length)
    }
  }
}

trait VarExpandPredicate[ENTITY] {
  def isTrue(entity: ENTITY): Boolean
}

object VarExpandPredicate {
  val NO_NODE_PREDICATE: VarExpandPredicate[Long] = (_: Long) => true
  val NO_RELATIONSHIP_PREDICATE: VarExpandPredicate[RelationshipSelectionCursor] = (_: RelationshipSelectionCursor) => true
}

abstract class OutgoingVarExpandCursor(override val fromNode: Long,
                                       targetToNode: Long,
                                       nodeCursor: NodeCursor,
                                       projectBackwards: Boolean,
                                       relTypes: Array[Int],
                                       minLength: Int,
                                       maxLength: Int,
                                       read: Read,
                                       executionContext: ExecutionContext,
                                       dbAccess: DbAccess,
                                       params: Array[AnyValue],
                                       cursors: ExpressionCursors,
                                       expressionVariables: Array[AnyValue])
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
                          expressionVariables) {

  override protected def selectionCursor(groupCursor: RelationshipGroupCursor,
                                         traversalCursor: RelationshipTraversalCursor,
                                         node: NodeCursor,
                                         types: Array[Int]): RelationshipSelectionCursor = outgoingCursor(groupCursor,
                                                                                                          traversalCursor,
                                                                                                          node, types)
}

abstract class IncomingVarExpandCursor(fromNode: Long,
                                       targetToNode: Long,
                                       nodeCursor: NodeCursor,
                                       projectBackwards: Boolean,
                                       relTypes: Array[Int],
                                       minLength: Int,
                                       maxLength: Int,
                                       read: Read,
                                       executionContext: ExecutionContext,
                                       dbAccess: DbAccess,
                                       params: Array[AnyValue],
                                       cursors: ExpressionCursors,
                                       expressionVariables: Array[AnyValue])
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
                          expressionVariables) {

  override protected def selectionCursor(groupCursor: RelationshipGroupCursor,
                                         traversalCursor: RelationshipTraversalCursor,
                                         node: NodeCursor,
                                         types: Array[Int]): RelationshipSelectionCursor = incomingCursor(groupCursor,
                                                                                                          traversalCursor,
                                                                                                          node, types)
}

abstract class AllVarExpandCursor(fromNode: Long,
                                  targetToNode: Long,
                                  nodeCursor: NodeCursor,
                                  projectBackwards: Boolean,
                                  relTypes: Array[Int],
                                  minLength: Int,
                                  maxLength: Int,
                                  read: Read,
                                  executionContext: ExecutionContext,
                                  dbAccess: DbAccess,
                                  params: Array[AnyValue],
                                  cursors: ExpressionCursors,
                                  expressionVariables: Array[AnyValue])
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
                          expressionVariables) {

  override protected def selectionCursor(groupCursor: RelationshipGroupCursor,
                                         traversalCursor: RelationshipTraversalCursor,
                                         node: NodeCursor,
                                         types: Array[Int]): RelationshipSelectionCursor = allCursor(groupCursor, traversalCursor, node, types)
}