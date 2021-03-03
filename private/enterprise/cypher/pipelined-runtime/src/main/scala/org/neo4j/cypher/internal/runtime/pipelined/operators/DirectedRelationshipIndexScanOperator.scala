/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_REL_INDEX_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_REL_SCAN_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.RelScanCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.RelValueIndexCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.indexReadSession
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.relationshipIndexScan
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexQueryConstraints
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.values.storable.Value

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

abstract class BaseRelationshipIndexScanTaskTemplate(inner: OperatorTaskTemplate,
                                                     id: Id,
                                                     innermost: DelegateOperatorTaskTemplate,
                                                     relOffset: Int,
                                                     startOffset: Int,
                                                     endOffset: Int,
                                                     properties: Array[SlottedIndexedProperty],
                                                     argumentSize: SlotConfiguration.Size,
                                                     codeGen: OperatorExpressionCompiler)
  extends BaseRelationshipIndexTaskTemplate(inner, id, innermost, relOffset, startOffset, endOffset, argumentSize, codeGen) {
  private val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2).toArray
  private val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot).toArray
  protected val needsValues: Boolean = indexPropertyIndices.nonEmpty

  override protected def cacheProperties: IntermediateRepresentation = {
    val ops = for {i <- indexPropertyIndices.indices
                   indexPropertyIndex = indexPropertyIndices(i)
                   slot = indexPropertySlotOffsets(i)
                   } yield {

      codeGen.setCachedPropertyAt(slot, getPropertyValueRepresentation(indexPropertyIndex))
    }
    block(ops: _*)
  }

  private def getPropertyValueRepresentation(offset: Int): IntermediateRepresentation =
    invoke(loadField(relIndexCursorField),
      method[RelationshipValueIndexCursor, Value, Int]("propertyValue"), constant(offset))

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   resources.cursorPools.relationshipValueIndexCursorPool.free(relIndexCursor)
     *   resources.cursorPools.relationshipsScanCursorPool.free(relScanCursor)
     *   relIndexCursor = null
     *   relScanCursor = null
     * }}}
     */
    block(
      freeCursor[RelationshipValueIndexCursor](loadField(relIndexCursorField), RelValueIndexCursorPool),
      freeCursor[RelationshipScanCursor](loadField(relScanCursorField), RelScanCursorPool),
      setField(relIndexCursorField, constant(null)),
      setField(relScanCursorField, constant(null))
    )
  }
}

class DirectedRelationshipIndexScanTaskTemplate(inner: OperatorTaskTemplate,
                                                id: Id,
                                                innermost: DelegateOperatorTaskTemplate,
                                                relOffset: Int,
                                                startOffset: Int,
                                                endOffset: Int,
                                                properties: Array[SlottedIndexedProperty],
                                                queryIndexId: Int,
                                                indexOrder: IndexOrder,
                                                argumentSize: SlotConfiguration.Size,
                                                codeGen: OperatorExpressionCompiler)
  extends BaseRelationshipIndexScanTaskTemplate(inner, id, innermost, relOffset, startOffset, endOffset, properties, argumentSize, codeGen) {

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    block(
      setField(canContinue, constant(false)),
      allocateAndTraceCursor(relIndexCursorField, executionEventField, ALLOCATE_REL_INDEX_CURSOR, doProfile),
      allocateAndTraceCursor(relScanCursorField, executionEventField, ALLOCATE_REL_SCAN_CURSOR, doProfile),
      relationshipIndexScan(indexReadSession(queryIndexId), loadField(relIndexCursorField), indexOrder, needsValues),
      setField(canContinue, profilingCursorNext[RelationshipValueIndexCursor](loadField(relIndexCursorField), id, doProfile, codeGen.namer)),
      constant(true)
    )
  }
}














