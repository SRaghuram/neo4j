/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.morsel.execution.CursorPools
import org.neo4j.cypher.internal.runtime.morsel.operators.VarExpandCursor.relationshipFromCursor
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.{allCursor, incomingCursor, outgoingCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{ListValue, RelationshipValue, VirtualValues}

sealed trait ExpandStatus
case object NOT_STARTED extends ExpandStatus
case object EXPAND extends ExpandStatus
case object EMIT extends ExpandStatus

class VarExpandCursor(fromNode: Long,
                      targetToNode: Long,
                      theNodeCursor: NodeCursor,
                      dir: SemanticDirection,
                      projectBackwards: Boolean,
                      relTypes: Array[Int],
                      minLength: Int,
                      maxLength: Int,
                      read: Read,
                      dbAccess: DbAccess,
                      nodePredicate: VarExpandPredicate[Long],
                      relationshipPredicate: VarExpandPredicate[RelationshipSelectionCursor]) {

  private var expandStatus: ExpandStatus = NOT_STARTED
  private var pathLength: Int = 0

  private val nodeCursor: NodeCursor = theNodeCursor
  private val relTraCursors: GrowingArray[RelationshipTraversalCursor] = new GrowingArray[RelationshipTraversalCursor]()
  private val relGroupCursors: GrowingArray[RelationshipGroupCursor] = new GrowingArray[RelationshipGroupCursor]()
  private val selectionCursors: GrowingArray[RelationshipSelectionCursor] = new GrowingArray[RelationshipSelectionCursor]()

  // this needs to be explicitly managed on every work unit, to avoid parallel workers accessing each others cursorPools.
  private var cursorPools: CursorPools = _

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
             !relationshipPredicate.isTrue(selectionCursor) ||
             !nodePredicate.isTrue(selectionCursor.otherNodeReference())
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

    val groupCursor = relGroupCursors.computeIfAbsent(pathLength, cursorPools.relationshipGroupCursorPool.allocate)
    val traversalCursor = relTraCursors.computeIfAbsent(pathLength, cursorPools.relationshipTraversalCursorPool.allocate)

    read.singleNode(node, nodeCursor)

    val selectionCursor =
      if (!nodeCursor.next()) {
        RelationshipSelectionCursor.EMPTY
      } else {

        val selectionCursor = dir match {
          case OUTGOING => outgoingCursor(groupCursor, traversalCursor, nodeCursor, relTypes)
          case INCOMING => incomingCursor(groupCursor, traversalCursor, nodeCursor, relTypes)
          case BOTH => allCursor(groupCursor, traversalCursor, nodeCursor, relTypes)
        }

        selectionCursor
      }

    selectionCursors.set(pathLength, selectionCursor)
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
  val NO_NODE_PREDICATE: VarExpandPredicate[Long] = (entity: Long) => true
  val NO_RELATIONSHIP_PREDICATE: VarExpandPredicate[RelationshipSelectionCursor] = (entity: RelationshipSelectionCursor) => true
}
