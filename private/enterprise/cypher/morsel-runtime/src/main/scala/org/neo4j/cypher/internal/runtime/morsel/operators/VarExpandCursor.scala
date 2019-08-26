/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util

import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.morsel.execution.CursorPools
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.{allCursor, incomingCursor, outgoingCursor}
import org.neo4j.internal.kernel.api.{NodeCursor, Read, RelationshipGroupCursor, RelationshipTraversalCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{ListValue, VirtualValues}

sealed trait ExpandStatus
case object NOT_STARTED extends ExpandStatus
case object EXPAND extends ExpandStatus
case object EMIT extends ExpandStatus

class VarExpandCursor(fromNode: Long,
                      targetToNode: Long,
                      cursorPools: CursorPools,
                      dir: SemanticDirection,
                      relTypes: Array[Int],
                      minLength: Int,
                      maxLength: Int,
                      read: Read,
                      dbAccess: DbAccess) {

  var expandStatus: ExpandStatus = NOT_STARTED
  var pathLength: Int = 0

  // TODO: resources?
  private val nodeCursor: NodeCursor = cursorPools.nodeCursorPool.allocate()
  private val relTraCursors: GrowingArray[RelationshipTraversalCursor] = new GrowingArray[RelationshipTraversalCursor]()
  private val relGroupCursors: GrowingArray[RelationshipGroupCursor] = new GrowingArray[RelationshipGroupCursor]()
  private val selectionCursors: GrowingArray[RelationshipSelectionCursor] = new GrowingArray[RelationshipSelectionCursor]()

  def next(): Boolean = {

    if (expandStatus == NOT_STARTED) {
      expandStatus = EXPAND
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
          val selectionCursor = selectionCursors.get(pathLength-1)
          if (selectionCursor.next()) {
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
    if (selectionCursors.isEmpty) {
      fromNode
    } else {
      selectionCursors.get(pathLength-1).otherNodeReference()
    }
  }

  def relationships: ListValue = {
    val r = new util.ArrayList[AnyValue]()
    var i = 0
    while (i < pathLength) {
      val cursor = relTraCursors.get(i)
      r.add(dbAccess.relationshipById(cursor.relationshipReference(),
                                      cursor.originNodeReference(),
                                      cursor.neighbourNodeReference(),
                                      cursor.`type`()))
      i += 1
    }
    VirtualValues.fromList(r)
  }

  def setTracer(event: OperatorProfileEvent): Unit = {
    // TODO: also set on new cursors
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

class GrowingArray[T <: AnyRef] {

  private var array: Array[AnyRef] = new Array[AnyRef](4)
  private var size: Int = 0

  def set(index: Int, t: T): Unit = {
    ensureCapacity(index+1)
    array(index) = t
  }

  def get(index: Int): T = {
    array(index).asInstanceOf[T]
  }

  def computeIfAbsent(index: Int, compute: () => T): T = {
    ensureCapacity(index+1)
    var t = array(index)
    if (t == null) {
      t = compute()
      array(index) = t
    }
    t.asInstanceOf[T]
  }

  def foreach(f: T => Unit): Unit = {
    var i = 0
    while (i < size) {
      f(get(i))
      i += 1
    }
  }

  def isEmpty: Boolean = size == 0

  private def ensureCapacity(size: Int): Unit = {
    if (this.size < size) {
      this.size = size
    }

    if (array.length < size) {
      val temp = array
      array = new Array[AnyRef](array.length * 2)
      System.arraycopy(temp, 0, array, 0, temp.length)
    }
  }
}
