/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.CompositeValueIndexCursor
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EntityIndexSeeker
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeek
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekMode
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyQueriesNodeIndexSeekTaskTemplate.isImpossible
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.internal.kernel.api.IndexQuery
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate
import org.neo4j.internal.kernel.api.IndexQueryConstraints
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.values.storable.Value

import scala.collection.mutable.ArrayBuffer

class DirectedRelationshipIndexSeekOperator(val workIdentity: WorkIdentity,
                                            relOffset: Int,
                                            startOffset: Int,
                                            endOffset: Int,
                                            properties: Array[SlottedIndexedProperty],
                                            queryIndexId: Int,
                                            indexOrder: IndexOrder,
                                            argumentSize: SlotConfiguration.Size,
                                            valueExpr: QueryExpression[Expression],
                                            indexMode: IndexSeekMode = IndexSeek)
  extends StreamingOperator {

  private val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
  private val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot)
  private val propertyIds: Array[Int] = properties.map(_.propertyKeyId)

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    singletonIndexedSeq(
      new DirectedRelationshipIndexSeekTask(
        inputMorsel.nextCopy,
        workIdentity,
        relOffset,
        startOffset,
        endOffset,
        indexPropertyIndices,
        indexPropertySlotOffsets,
        queryIndexId,
        indexOrder,
        argumentSize,
        propertyIds,
        valueExpr,
        indexMode)
    )
  }
}

class DirectedRelationshipIndexSeekTask(inputMorsel: Morsel,
                                        val workIdentity: WorkIdentity,
                                        relOffset: Int,
                                        startOffset: Int,
                                        endOffset: Int,
                                        indexPropertyIndices: Array[Int],
                                        indexPropertySlotOffsets: Array[Int],
                                        queryIndexId: Int,
                                        indexOrder: IndexOrder,
                                        argumentSize: SlotConfiguration.Size,
                                        val propertyIds: Array[Int],
                                        val valueExpr: QueryExpression[Expression],
                                        val indexMode: IndexSeekMode = IndexSeek) extends InputLoopTask(inputMorsel) with EntityIndexSeeker {

  protected var relCursor: RelationshipValueIndexCursor = _
  protected var scanCursor: RelationshipScanCursor = _
  private var cursorsToClose: Array[RelationshipValueIndexCursor] = _
  private var exactSeekValues: Array[Value] = _

  // INPUT LOOP TASK
  override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
    val queryState = state.queryStateForExpressionEvaluation(resources)
    initExecutionContext.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
    val indexQueries = computeIndexQueries(queryState, initExecutionContext)
    val read = state.query.transactionalContext.transaction.dataRead
    val (cursor, closers) = computeCursor(indexQueries, read, state, resources, queryIndexId, indexOrder, needsValues || indexOrder != IndexOrder.NONE)
    relCursor = cursor
    cursorsToClose = closers
    scanCursor = resources.cursorPools.relationshipScanCursorPool.allocateAndTrace()
    true
  }

  override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
    val read = state.query.transactionalContext.transaction.dataRead
    while (outputRow.onValidRow && relCursor != null && relCursor.next()) {
      val relationship = relCursor.relationshipReference()
      outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
      outputRow.setLongAt(relOffset, relationship)
      read.singleRelationship(relationship, scanCursor)
      scanCursor.next()
      outputRow.setLongAt(startOffset, scanCursor.sourceNodeReference())
      outputRow.setLongAt(endOffset, scanCursor.targetNodeReference())
      cacheProperties(outputRow)
      outputRow.next()
    }
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
    if (relCursor != null) {
      relCursor.setTracer(event)
    }
  }

  override protected def closeInnerLoop(resources: QueryResources): Unit = {
    if (cursorsToClose != null) {
      cursorsToClose.foreach(resources.cursorPools.relationshipValueIndexCursorPool.free)
      cursorsToClose = null
    }
    if (scanCursor != null) {
      resources.cursorPools.relationshipScanCursorPool.free(scanCursor)
      scanCursor = null
    }
    relCursor = null
  }

  // HELPERS
  protected def cacheProperties(outputRow: MorselFullCursor): Unit = {
    var i = 0
    while (i < indexPropertyIndices.length) {
      val indexPropertyIndex = indexPropertyIndices(i)
      val value =
        if (exactSeekValues != null) {
          exactSeekValues(indexPropertyIndex)
        } else {
          relCursor.propertyValue(indexPropertyIndex)
        }
      outputRow.setCachedPropertyAt(indexPropertySlotOffsets(i), value)
      i += 1
    }
  }
  protected def computeCursor(indexQueries: Seq[Seq[IndexQuery]],
                              read: Read,
                              state: PipelinedQueryState,
                              resources: QueryResources,
                              queryIndex: Int,
                              kernelIndexOrder: IndexOrder,
                              needsValues: Boolean): (RelationshipValueIndexCursor, Array[RelationshipValueIndexCursor]) = {
    if (indexQueries.size == 1) {
      val cursor = nonCompositeSeek(state.queryIndexes(queryIndex), resources, read, indexQueries.head)
      (cursor, Array(cursor))
    } else {
      val cursors = ArrayBuffer.empty[RelationshipValueIndexCursor]
      indexQueries.filterNot(isImpossible).foreach(query => {
        val cursor = seek(state.queryIndexes(queryIndex), resources, read, query, needsValues)
        cursors += cursor
      })
      val cursorArray = cursors.toArray
      (orderedCursor(kernelIndexOrder, cursorArray), cursorArray)
    }
  }

  private def orderedCursor(indexOrder: IndexOrder, cursors: Array[RelationshipValueIndexCursor]): RelationshipValueIndexCursor = indexOrder match {
    case IndexOrder.NONE => CompositeValueIndexCursor.unordered(cursors)
    case IndexOrder.ASCENDING => CompositeValueIndexCursor.ascending(cursors)
    case IndexOrder.DESCENDING => CompositeValueIndexCursor.descending(cursors)
  }

  private def needsValues: Boolean = indexPropertyIndices.nonEmpty

  //Only used for non-composite indexes, contains optimizations so that we
  //can avoid getting the value from the index when doing exact seeks.
  private def nonCompositeSeek(index: IndexReadSession,
                               resources: QueryResources,
                               read: Read,
                               predicates: Seq[IndexQuery]): RelationshipValueIndexCursor = {

    if (isImpossible(predicates)) {
      return RelationshipValueIndexCursor.EMPTY// leave cursor un-initialized/empty
    }

    // We don't need property values from the index for an exact seek
    exactSeekValues =
      if (needsValues && predicates.forall(_.isInstanceOf[ExactPredicate])) {
        predicates.map(_.asInstanceOf[ExactPredicate].value()).toArray
      } else {
        null
      }
    val needsValuesFromIndexSeek = exactSeekValues == null && needsValues

    seek(index, resources, read, predicates, needsValuesFromIndexSeek)
  }

  private def seek(index: IndexReadSession,
                   resources: QueryResources,
                   read: Read,
                   predicates: Seq[IndexQuery],
                   needsValuesFromIndexSeek: Boolean): RelationshipValueIndexCursor = {
    val pool = resources.cursorPools.relationshipValueIndexCursorPool
    val cursor = pool.allocateAndTrace()
    read.relationshipIndexSeek(index, cursor, IndexQueryConstraints.constrained(indexOrder, needsValuesFromIndexSeek), predicates: _*)
    cursor
  }
}

























