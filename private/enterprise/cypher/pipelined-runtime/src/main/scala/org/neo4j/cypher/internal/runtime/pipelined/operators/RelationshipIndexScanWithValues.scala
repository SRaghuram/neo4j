/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor

/**
 * For index operators that get nodes together with actual property values.
 */
abstract class RelationshipIndexScanWithValues(relOffset: Int,
                                               startOffset: Int,
                                               endOffset: Int,
                                               indexPropertyIndices: Array[Int],
                                               indexPropertySlotOffsets: Array[Int],
                                               argumentSize: SlotConfiguration.Size,
                                               inputMorsel: Morsel)
  extends InputLoopTask(inputMorsel) {

  protected var relCursor: RelationshipValueIndexCursor = _
  protected var scanCursor: RelationshipScanCursor = _

  protected def cacheProperties(outputRow: MorselFullCursor): Unit = {
    var i = 0
    while (i < indexPropertyIndices.length) {
      outputRow.setCachedPropertyAt(indexPropertySlotOffsets(i), relCursor.propertyValue(indexPropertyIndices(i)))
      i += 1
    }
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
    if (relCursor != null) {
      relCursor.setTracer(event)
    }
    if (scanCursor != null) {
      scanCursor.setTracer(event)
    }
  }

  override protected def closeInnerLoop(resources: QueryResources): Unit = {
    if (relCursor != null) {
      resources.cursorPools.relationshipValueIndexCursorPool.free(relCursor)
      relCursor = null
    }
    if (scanCursor != null) {
      resources.cursorPools.relationshipScanCursorPool.free(scanCursor)
      scanCursor = null
    }
  }
}

abstract class DirectedRelationshipIndexScanWithValues(relOffset: Int,
                                               startOffset: Int,
                                               endOffset: Int,
                                               indexPropertyIndices: Array[Int],
                                               indexPropertySlotOffsets: Array[Int],
                                               argumentSize: SlotConfiguration.Size,
                                               inputMorsel: Morsel)
  extends RelationshipIndexScanWithValues(relOffset, startOffset, endOffset, indexPropertyIndices, indexPropertySlotOffsets, argumentSize, inputMorsel) {

  override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
    val read = state.query.transactionalContext.transaction.dataRead
    while (outputRow.onValidRow && relCursor != null && relCursor.next()) {
      val relationship = relCursor.relationshipReference()

      outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
      outputRow.setLongAt(relOffset, relationship)
      read.singleRelationship(relationship, scanCursor)
      require(scanCursor.next())
      outputRow.setLongAt(startOffset, scanCursor.sourceNodeReference())
      outputRow.setLongAt(endOffset, scanCursor.targetNodeReference())
      cacheProperties(outputRow)
      outputRow.next()
    }
  }
}

abstract class UndirectedRelationshipIndexScanWithValues(relOffset: Int,
                                                         startOffset: Int,
                                                         endOffset: Int,
                                                         indexPropertyIndices: Array[Int],
                                                         indexPropertySlotOffsets: Array[Int],
                                                         argumentSize: SlotConfiguration.Size,
                                                         inputMorsel: Morsel)
  extends RelationshipIndexScanWithValues(relOffset, startOffset, endOffset, indexPropertyIndices, indexPropertySlotOffsets, argumentSize, inputMorsel) {

  private var forwardDirection = true

  override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
    val read = state.query.transactionalContext.transaction.dataRead
    while (outputRow.onValidRow && relCursor != null && (!forwardDirection || relCursor.next())) {
      val relationship = relCursor.relationshipReference()

      outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
      outputRow.setLongAt(relOffset, relationship)
      if (forwardDirection) {
        read.singleRelationship(relationship, scanCursor)
        scanCursor.next()
        outputRow.setLongAt(startOffset, scanCursor.sourceNodeReference())
        outputRow.setLongAt(endOffset, scanCursor.targetNodeReference())
        forwardDirection = false
      } else {
        outputRow.setLongAt(startOffset, scanCursor.targetNodeReference())
        outputRow.setLongAt(endOffset, scanCursor.sourceNodeReference())
        forwardDirection = true
      }
      cacheProperties(outputRow)
      outputRow.next()
    }
  }
}
