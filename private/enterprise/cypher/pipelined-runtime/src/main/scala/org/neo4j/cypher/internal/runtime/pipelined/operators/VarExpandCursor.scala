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
import org.neo4j.cypher.internal.runtime.pipelined.execution.CursorPools
import org.neo4j.cypher.internal.runtime.pipelined.operators.VarExpandCursor.relationshipFromCursor
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingCursor
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
                               expressionVariables: Array[AnyValue]) {

  private var expandStatus: ExpandStatus = NOT_STARTED
  private var pathLength: Int = 0
  private var event: OperatorProfileEvent = _

  private val relTraCursors: GrowingArray[RelationshipTraversalCursor] = new GrowingArray[RelationshipTraversalCursor]()
  private val selectionCursors: GrowingArray[RelationshipTraversalCursor] = new GrowingArray[RelationshipTraversalCursor]()

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
        RelationshipTraversalCursor.EMPTY
      } else {
        val traversalCursor = relTraCursors.computeIfAbsent(pathLength, () => {
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
    relTraCursors.foreach(_.setTracer(event))
  }

  def free(cursorPools: CursorPools): Unit = {
    cursorPools.nodeCursorPool.free(nodeCursor)
    relTraCursors.foreach(cursor => cursorPools.relationshipTraversalCursorPool.free(cursor))
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
            relationshipPredicate: VarExpandPredicate[RelationshipTraversalCursor]): VarExpandCursor = direction match {
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
        null) {
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
        null) {
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
      val newLength = math.max(array.length * 2, size)
      array = new Array[AnyRef](newLength)
      System.arraycopy(temp, 0, array, 0, temp.length)
    }
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

  override protected def selectionCursor(traversalCursor: RelationshipTraversalCursor,
                                         node: NodeCursor,
                                         types: Array[Int]): RelationshipTraversalCursor = allCursor(traversalCursor, node, types)
}