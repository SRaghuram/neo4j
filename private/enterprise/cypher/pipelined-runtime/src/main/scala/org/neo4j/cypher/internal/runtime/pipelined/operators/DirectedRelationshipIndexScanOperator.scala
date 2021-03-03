/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.internal.kernel.api.IndexQueryConstraints
import org.neo4j.internal.schema.IndexOrder

class DirectedRelationshipIndexScanOperator(val workIdentity: WorkIdentity,
                                            relOffset: Int,
                                            startOffset: Int,
                                            endOffset: Int,
                                            properties: Array[SlottedIndexedProperty],
                                            queryIndexId: Int,
                                            indexOrder: IndexOrder,
                                            argumentSize: SlotConfiguration.Size)
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
      new DirectedRelationshipIndexScanTask(
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
        propertyIds)
    )
  }
}

class DirectedRelationshipIndexScanTask(inputMorsel: Morsel,
                                        val workIdentity: WorkIdentity,
                                        relOffset: Int,
                                        startOffset: Int,
                                        endOffset: Int,
                                        indexPropertyIndices: Array[Int],
                                        indexPropertySlotOffsets: Array[Int],
                                        queryIndexId: Int,
                                        indexOrder: IndexOrder,
                                        argumentSize: SlotConfiguration.Size,
                                        val propertyIds: Array[Int])
  extends DirectedRelationshipIndexScanWithValues(relOffset, startOffset, endOffset, indexPropertyIndices, indexPropertySlotOffsets, argumentSize, inputMorsel) {


  private def needsValues: Boolean = indexPropertyIndices.nonEmpty

  override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
    val index = state.queryIndexes(queryIndexId)
    relCursor = resources.cursorPools.relationshipValueIndexCursorPool.allocateAndTrace()
    val read = state.queryContext.transactionalContext.dataRead
    read.relationshipIndexScan(index, relCursor, IndexQueryConstraints.constrained(indexOrder, needsValues))
    scanCursor = resources.cursorPools.relationshipScanCursorPool.allocateAndTrace()
    true
  }
}
















