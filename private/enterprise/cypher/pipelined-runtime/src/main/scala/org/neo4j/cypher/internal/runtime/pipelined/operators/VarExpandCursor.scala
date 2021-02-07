/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingLongHashSet
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.GrowingArray
import org.neo4j.cypher.internal.runtime.pipelined.execution.CursorPools
import org.neo4j.cypher.internal.runtime.pipelined.operators.VarExpandCursor.IdSetMinPathLength
import org.neo4j.cypher.internal.runtime.pipelined.operators.VarExpandCursor.relationshipFromCursor
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingCursor
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_ENTITY
import org.neo4j.kernel.impl.newapi.Cursors.emptyTraversalCursor
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.ListValue.AppendList
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

  private val selectionCursors: GrowingArray[RelationshipTraversalCursor] = new GrowingArray[RelationshipTraversalCursor](memoryTracker)
  private val emptyRelationCursor: RelationshipTraversalCursor = emptyTraversalCursor(read)

   // This cache is an optimisation for var length expands with very long paths
   // (we had one example with 1 000 000 relationships that failed to finish in pipelined before this change).
  private val relationshipCache: MemoryTrackingAppendedListCache = new MemoryTrackingAppendedListCache(memoryTracker)

  // Set to true when `idSet` is populated.
  private var useIdSet = false

  // Contains all relationship ids in the current path if `useIdSet` is true.
  private var idSet: HeapTrackingLongHashSet = _

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

          if (useIdSet) {
            val oldId = selectionCursor.relationshipReference()
            if (oldId != -1) {
              idSet.remove(oldId)
            }
          }

          var hasNext = false

          // Invalidate cache at `r` because the selection cursor will move
          relationshipCache.softInvalidate(r)

          do {
            hasNext = selectionCursor.next()
          } while (hasNext &&
            (!relationshipIsUniqueInPath(r, selectionCursor.relationshipReference()) ||
              !satisfyPredicates(executionContext, dbAccess, params, cursors, expressionVariables, selectionCursor)
              ))

          if (hasNext) {
            if (useIdSet) {
              idSet.add(selectionCursor.relationshipReference())
            } else if (pathLength == IdSetMinPathLength) {
              initIdSet()
            }

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

  private def validToNode: Boolean = targetToNode == NO_SUCH_ENTITY || targetToNode == toNode

  private def relationshipIsUniqueInPath(relInPath: Int, relationshipId: Long): Boolean = {
    if (useIdSet) {
      !idSet.contains(relationshipId)
    } else {
      var i = relInPath - 1
      while (i >= 0) {
        if (selectionCursors.get(i).relationshipReference() == relationshipId)
          return false
        i -= 1
      }
      true
    }
  }

  private def expand(node: Long): Unit = {
    read.singleNode(node, nodeCursor)
    val previousCursor = selectionCursors.getOrNull(pathLength)

    if (!nodeCursor.next()) {
      if (previousCursor != null && previousCursor != emptyRelationCursor) {
        cursorPools.relationshipTraversalCursorPool.free(previousCursor)
      }
      selectionCursors.set(pathLength, emptyRelationCursor)
    } else {
      if (previousCursor != null && previousCursor != emptyRelationCursor) {
        // Re-use current cursor
        selectionCursor(previousCursor, nodeCursor, relTypes)
      } else {
        // Allocate new cursor
        val cursor = cursorPools.relationshipTraversalCursorPool.allocateAndTrace()
        cursor.setTracer(event)
        selectionCursor(cursor, nodeCursor, relTypes)
        selectionCursors.set(pathLength, cursor)
      }
    }
  }

  def toNode: Long = {
    if (selectionCursors.hasNeverSeenData) {
      fromNode
    } else {
      selectionCursors.get(pathLength-1).otherNodeReference()
    }
  }

  private def updateCache(index: Int, prevList: ListValue): AppendList = {
    val cursor = selectionCursors.get(index)
    val relationship = relationshipFromCursor(dbAccess, cursor)
    val list = prevList.append(relationship)
    relationshipCache.append(list)
    list
  }

  /*
   * Updates the relationship cache with all new relationship values and returns the
   * list of relationships in the current path.
   *
   * Let's assume we have a graph: ()-[r1]->()-[r2]->()-[r3]->()-[r4]->()-[r5]->()
   *
   * If pathLength = 4 and relationShipCache.getSize = 2
   * We will need to update the cache with the following:
   * 2 -> [r1, r2, r3]
   * 3 -> [r1, r2, r3, r4]
   *
   * The function will then return the last AppendList in the cache: [r1, r2, r3, r4]
   */
  def relationships: ListValue = {
    if (pathLength == 0) {
      return VirtualValues.EMPTY_LIST
    }
    assert(relationshipCache.getSize <= pathLength)

    var list = relationshipCache.lastOrDefault(() => VirtualValues.EMPTY_LIST) // Depends on assertion
    var i = relationshipCache.getSize

    while (i < pathLength) {
      list = updateCache(i, list)
      i += 1
    }

    if (projectBackwards) {
      list.reverse()
    } else {
      list
    }
  }

  def initIdSet(): Unit = {
    if (!useIdSet) {
      idSet = HeapTrackingCollections.newLongSet(memoryTracker, pathLength * 2)
      var i = 0
      while (i < pathLength) {
        idSet.add(selectionCursors.get(i).relationshipReference())
        i += 1
      }
      useIdSet = true
    }
  }

  def setTracer(event: OperatorProfileEvent): Unit = {
    this.event = event
    nodeCursor.setTracer(event)

    // Should never call setTracer when selectionCursors is closed with event != null
    if (!(selectionCursors.isClosed() && event == null))
      selectionCursors.foreach(_.setTracer(event))
  }

  def free(cursorPools: CursorPools): Unit = {
    cursorPools.nodeCursorPool.free(nodeCursor)
    if (!selectionCursors.isClosed()) {
      selectionCursors.foreach { cursor =>
        if (cursor != emptyRelationCursor) {
          cursorPools.relationshipTraversalCursorPool.free(cursor)
        }
      }
      selectionCursors.close()
    }
    if (idSet != null) {
      idSet.close()
      idSet = null
    }
    relationshipCache.close()
  }
}

object VarExpandCursor {
  // Use `idSet` after this path length has been passed. Below this limit searching the `selectionCursors` array directly is faster.
  val IdSetMinPathLength = 128

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

class MemoryTrackingAppendedListCache(memoryTracker: MemoryTracker) extends AutoCloseable {
  // TODO: Track size of append lists.
  // We don't track memory of the append lists, since the size of an AppendList
  // might change from when it is inserted to when it is removed.
  private val appendLists = new GrowingArray[ListValue](memoryTracker)
  private var size = 0

  /*
   * append value to cache
   */
  def append(value: ListValue): Unit = {
    appendLists.set(size, value)
    size = size + 1
  }

  /*
   * Updates the size, but does not remove anything from the appendLists.
   * We will override values at index greater or equal to i when calling the append function.
   */
  def softInvalidate(i: Int): Unit = {
    if (size > i) {
      size = i
    }
  }

  /*
   * returns the size of the valid cached lists.
   */
  def getSize: Int = {
    size
  }

  /*
   * returns the last cached value or default if cache is empty.
   */
  def lastOrDefault(default: () => ListValue): ListValue = {
    if (size > 0) {
      appendLists.get(size - 1)
    } else {
      default()
    }
  }

  override def close(): Unit = {
    appendLists.close()
  }
}

object MemoryTrackingAppendedListCache {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[MemoryTrackingAppendedListCache])
}
