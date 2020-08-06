/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveRelationshipFromSlotFunctionFor
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.VarLengthProjectEndpointsTask.varLengthFindStartAndEnd
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.RelationshipReference
import org.neo4j.values.virtual.RelationshipValue

class ProjectEndpointsOperator(val workIdentity: WorkIdentity,
                               relSlot: Slot,
                               startOffset: Int,
                               endOffset: Int,
                               types: RelationshipTypes,
                               isSimplePath: Boolean) extends StreamingOperator {
  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    if (isSimplePath) {
      singletonIndexedSeq(new ProjectEndpointsTask(workIdentity, inputMorsel.nextCopy, relSlot, startOffset, endOffset, types))
    } else {
      singletonIndexedSeq(new VarLengthProjectEndpointsTask(workIdentity, inputMorsel.nextCopy, relSlot, startOffset, endOffset, types))
    }
  }
}

abstract class BaseProjectEndpointsTask(val workIdentity: WorkIdentity,
                                        inputMorsel: Morsel,
                                        startOffset: Int,
                                        endOffset: Int) extends InputLoopTask(inputMorsel) {

  private var forwardDirection = true
  protected var source: Long = -1L
  protected var target: Long = -1L
  protected var initialized = false

  override def toString: String = "ProjectEndPoints"

  override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
    while (outputRow.onValidRow() && (!forwardDirection || initialized)) {
      initialized = false
      outputRow.copyFrom(inputCursor)
      if (forwardDirection) {
        outputRow.setLongAt(startOffset, source)
        outputRow.setLongAt(endOffset, target)
        forwardDirection = false
      } else {
        outputRow.setLongAt(startOffset, target)
        outputRow.setLongAt(endOffset, source)

        forwardDirection = true
      }
      outputRow.next()
    }
  }

  override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
}

class ProjectEndpointsTask(workIdentity: WorkIdentity,
                           inputMorsel: Morsel,
                           relSlot: Slot,
                           startOffset: Int,
                           endOffset: Int,
                           types: RelationshipTypes)
  extends BaseProjectEndpointsTask(workIdentity, inputMorsel, startOffset, endOffset) {

  private val getRel = makeGetPrimitiveRelationshipFromSlotFunctionFor(relSlot)

  override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
    val relId = getRel.applyAsLong(inputCursor)
    val typesToCheck = types.types(state.query)
    if (entityIsNull(relId)) {
      false
    } else {
      val cursor = resources.expressionCursors.relationshipScanCursor
      state.query.transactionalContext.dataRead.singleRelationship(relId, cursor)
      if (cursor.next() && (typesToCheck == null || typesToCheck.contains(cursor.`type`()))) {
        source = cursor.sourceNodeReference()
        target = cursor.targetNodeReference()
        initialized = true
        true
      } else {
        false
      }
    }
  }
}

class VarLengthProjectEndpointsTask(workIdentity: WorkIdentity,
                                    inputMorsel: Morsel,
                                    relSlot: Slot,
                                    startOffset: Int,
                                    endOffset: Int,
                                    types: RelationshipTypes)
  extends BaseProjectEndpointsTask(workIdentity, inputMorsel, startOffset, endOffset) with ListSupport {

  override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
    val rels = makeTraversable(inputCursor.getRefAt(relSlot.offset))
    val typesToCheck = types.types(state.query)
    varLengthFindStartAndEnd(rels, state, typesToCheck) match {
      case Some((s, e)) =>
        source = s
        target = e
        initialized = true
        true
      case None => false
    }
  }
}

object VarLengthProjectEndpointsTask {
  def varLengthFindStartAndEnd(rels: ListValue,
                               state: PipelinedQueryState,
                               typesToCheck: Array[Int]): Option[(Long, Long)] = {
    if (rels.isEmpty) {
      return None
    }
    val qtx = state.query
    if (typesToCheck == null) {
      val firstRel = rels.head match {
        case relValue: RelationshipValue => relValue
        case relRef: RelationshipReference => qtx.relationshipById(relRef.id())
      }
      val lastRel = rels.last match {
        case relValue: RelationshipValue => relValue
        case relRef: RelationshipReference => qtx.relationshipById(relRef.id())
      }
      Some((firstRel.startNode().id(), lastRel.endNode().id()))
    } else {
      var i = 0
      var firstRel: RelationshipValue = null
      var lastRel: RelationshipValue = null
      val numberOfRels = rels.size()
      while (i < numberOfRels) {
        val r = rels.value(i) match {
          case relValue: RelationshipValue => relValue
          case relRef: RelationshipReference => qtx.relationshipById(relRef.id())
        }
        if (!typesToCheck.contains(qtx.relationshipType(r.`type`().stringValue()))) {
          return None
        }
        if (i == 0) {
          firstRel = r
        }
        if (i == numberOfRels - 1) {
          lastRel = r
        }
        i += 1
      }
      Some((firstRel.startNode().id(), lastRel.endNode().id()))
    }
  }
}

abstract class BaseProjectEndpointsMiddleOperator(val workIdentity: WorkIdentity,
                                                  relSlot: Slot,
                                                  startOffset: Int,
                                                  startInScope: Boolean,
                                                  endOffset: Int,
                                                  endInScope: Boolean,
                                                  types: RelationshipTypes,
                                                  directed: Boolean) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val readCursor = morsel.readCursor()
    val writeCursor = morsel.writeCursor(onFirstRow = true)
    val typesToCheck: Array[Int] = types.types(state.query)

    while (readCursor.next()) {
      findStartAndEnd(readCursor, state, resources, typesToCheck) match {
        case Some((start, end)) =>
          writeRow(readCursor, writeCursor, start, end)
        case _ =>

      }
    }
    writeCursor.truncate()
  }

  protected def findStartAndEnd(readCursor: MorselReadCursor,
                              state: PipelinedQueryState,
                              resources: QueryResources,
                              typesToCheck: Array[Int]): Option[(Long, Long)]


  private def writeRow(readCursor: MorselReadCursor,
                       writeCursor: MorselWriteCursor,
                       start: Long,
                       end: Long): Unit = {
    if ((!startInScope || readCursor.getLongAt(startOffset) == start) &&
      (!endInScope || readCursor.getLongAt(endOffset) == end)) {
      writeCursor.setLongAt(startOffset, start)
      writeCursor.setLongAt(endOffset, end)
      writeCursor.next()
    } else if (!directed &&
      (!startInScope || readCursor.getLongAt(startOffset) == end) &&
      (!endInScope || readCursor.getLongAt(endOffset) == start)) {
      writeCursor.setLongAt(startOffset, end)
      writeCursor.setLongAt(endOffset, start)
      writeCursor.next()
    } else {
      //skip
    }
  }
}

class ProjectEndpointsMiddleOperator(workIdentity: WorkIdentity,
                                     relSlot: Slot,
                                     startOffset: Int,
                                     startInScope: Boolean,
                                     endOffset: Int,
                                     endInScope: Boolean,
                                     types: RelationshipTypes,
                                     directed: Boolean)
  extends BaseProjectEndpointsMiddleOperator(workIdentity, relSlot, startOffset, startInScope, endOffset, endInScope, types, directed) {
  private val getRel = makeGetPrimitiveRelationshipFromSlotFunctionFor(relSlot)

  override protected def findStartAndEnd(readCursor: MorselReadCursor,
                                         state: PipelinedQueryState,
                                         resources: QueryResources,
                                         typesToCheck: Array[Int]): Option[(Long, Long)] = {
    val relId = getRel.applyAsLong(readCursor)
    val cursor = resources.expressionCursors.relationshipScanCursor
    if (entityIsNull(relId)) {
      None
    } else {
      state.query.transactionalContext.dataRead.singleRelationship(relId, cursor)
      if (cursor.next() && hasValidType(cursor, typesToCheck)) {
        Some(cursor.sourceNodeReference(), cursor.targetNodeReference())
      } else {
        None
      }
    }
  }

  private def hasValidType(cursor: RelationshipScanCursor,
                           typesToCheck: Array[Int]): Boolean =
    typesToCheck == null || typesToCheck.contains(cursor.`type`())

}

class VarLengthProjectEndpointsMiddleOperator(workIdentity: WorkIdentity,
                                              relSlot: Slot,
                                              startOffset: Int,
                                              startInScope: Boolean,
                                              endOffset: Int,
                                              endInScope: Boolean,
                                              types: RelationshipTypes,
                                              directed: Boolean)
  extends BaseProjectEndpointsMiddleOperator(workIdentity, relSlot, startOffset, startInScope, endOffset, endInScope, types, directed) with ListSupport {

  override protected def findStartAndEnd(readCursor: MorselReadCursor,
                                         state: PipelinedQueryState,
                                         resources: QueryResources,
                                         typesToCheck: Array[Int]): Option[(Long, Long)] = {
    varLengthFindStartAndEnd(makeTraversable(readCursor.getRefAt(relSlot.offset)), state, typesToCheck)
  }
}



