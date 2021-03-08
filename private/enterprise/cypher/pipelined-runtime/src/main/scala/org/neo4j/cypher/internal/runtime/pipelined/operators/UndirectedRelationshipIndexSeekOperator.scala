/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EntityIndexSeeker
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeek
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekMode
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.schema.IndexOrder

class UndirectedRelationshipIndexSeekOperator(val workIdentity: WorkIdentity,
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
      new UndirectedRelationshipIndexSeekTask(
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

class UndirectedRelationshipIndexSeekTask(inputMorsel: Morsel,
                                          workIdentity: WorkIdentity,
                                          relOffset: Int,
                                          startOffset: Int,
                                          endOffset: Int,
                                          indexPropertyIndices: Array[Int],
                                          indexPropertySlotOffsets: Array[Int],
                                          queryIndexId: Int,
                                          indexOrder: IndexOrder,
                                          argumentSize: SlotConfiguration.Size,
                                          propertyIds: Array[Int],
                                          valueExpr: QueryExpression[Expression],
                                          indexMode: IndexSeekMode = IndexSeek)
      extends DirectedRelationshipIndexSeekTask(inputMorsel, workIdentity, relOffset, startOffset, endOffset, indexPropertyIndices, indexPropertySlotOffsets, queryIndexId, indexOrder, argumentSize, propertyIds, valueExpr, indexMode) with EntityIndexSeeker {
  private var forwardDirection = true

  override final protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
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
/**
 * Code generation template for index seeks of the form `MATCH ()-[r:R]-() WHERE r.prop = 42`
 */
class SingleExactUndirectedSeekQueryRelationshipIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                                    id: Id,
                                                                    innermost: DelegateOperatorTaskTemplate,
                                                                    relOffset: Int,
                                                                    startOffset: Int,
                                                                    endOffset: Int,
                                                                    property: SlottedIndexedProperty,
                                                                    generateSeekValue: () => IntermediateExpression,
                                                                    queryIndexId: Int,
                                                                    argumentSize: SlotConfiguration.Size)
                                                                   (codeGen: OperatorExpressionCompiler)
  extends SingleExactDirectedSeekQueryRelationshipIndexSeekTaskTemplate(inner,
                                                                        id,
                                                                        innermost,
                                                                        relOffset,
                                                                        startOffset,
                                                                        endOffset,
                                                                        property,
                                                                        generateSeekValue,
                                                                        queryIndexId,
                                                                        argumentSize)(codeGen) with UndirectedRelationshipIndexTask {

  override def genMoreFields: Seq[Field] = super.genMoreFields :+ forwardDirection

 }

class SingleUndirectedRangeSeekQueryRelationshipIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                                    id: Id,
                                                                    innermost: DelegateOperatorTaskTemplate,
                                                                    relOffset: Int,
                                                                    startOffset: Int,
                                                                    endOffset: Int,
                                                                    property: SlottedIndexedProperty,
                                                                    seekExpression: SeekExpression,
                                                                    queryIndexId: Int,
                                                                    order: IndexOrder,
                                                                    argumentSize: SlotConfiguration.Size )(codeGen: OperatorExpressionCompiler)
  extends SingleDirectedRangeSeekQueryRelationshipIndexSeekTaskTemplate(inner,
    id,
    innermost,
    relOffset,
    startOffset,
    endOffset,
    property,
    seekExpression,
    queryIndexId,
    order,
    argumentSize)(codeGen) with UndirectedRelationshipIndexTask {

  override def genMoreFields: Seq[Field] = super.genMoreFields :+ forwardDirection
}

/**
* Code generation template for index seeks of the form `MATCH ()-[r:R]->() WHERE r.prop = 1 OR r.prop = 2 OR r.prop =...`
*
* Will use delegate to `unordered`, `ascending`, or `descending` in `CompositeValueIndexCursor` depending on the order
* to get a cursor.
*/
class ManyQueriesUndirectedRelationshipIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                   id: Id,
                                                   innermost: DelegateOperatorTaskTemplate,
                                                   relOffset: Int,
                                                   startOffset: Int,
                                                   endOffset: Int,
                                                   property: SlottedIndexedProperty,
                                                   seekExpression: SeekExpression,
                                                   queryIndexId: Int,
                                                   order: IndexOrder,
                                                   argumentSize: SlotConfiguration.Size)
                                                  (codeGen: OperatorExpressionCompiler)
  extends ManyQueriesDirectedRelationshipIndexSeekTaskTemplate(inner,
    id, innermost, relOffset, startOffset, endOffset, property, seekExpression, queryIndexId, order, argumentSize)(codeGen) with UndirectedRelationshipIndexTask {

  override def genMoreFields: Seq[Field] = super.genMoreFields :+ forwardDirection
}

class CompositeUndirectedRelationshipIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                 id: Id,
                                                 innermost: DelegateOperatorTaskTemplate,
                                                 relOffset: Int,
                                                 startOffset: Int,
                                                 endOffset: Int,
                                                 properties: Seq[SlottedIndexedProperty],
                                                 seekExpressions: Seq[SeekExpression],
                                                 queryIndexId: Int,
                                                 order: IndexOrder,
                                                 argumentSize: SlotConfiguration.Size)
                                                (codeGen: OperatorExpressionCompiler)
  extends CompositeDirectedRelationshipIndexSeekTaskTemplate(inner, id, innermost, relOffset, startOffset, endOffset, properties, seekExpressions, queryIndexId, order, argumentSize)(codeGen) with UndirectedRelationshipIndexTask {
  override def genMoreFields: Seq[Field] = super.genMoreFields :+ forwardDirection
}
