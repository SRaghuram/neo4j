/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.setField
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.singleRelationship
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.util.Preconditions

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


trait UndirectedSeek {
  self: BaseRelationshipIndexSeekTaskTemplate =>

  protected val forwardDirection: Field = field[Boolean](codeGen.namer.nextVariableName(), constant(true))
  override protected def genInnerLoop: IntermediateRepresentation = {
    val localRelVar = codeGen.namer.nextVariableName("rel")
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     ...
     *     if (forwardDirection) {
     *        setLongAt(relOffset, relIndexCursor.relationshipReference())
     *        [set scan cursor to point at relationshipReference]
     *        setLongAt(startOffset, relScanCursor.sourceNodeReference())
     *        setLongAt(endOffset, relScanCursor.targetNodeReference())
     *     } else {
     *        setLongAt(startOffset, relScanCursor.targetNodeReference())
     *        setLongAt(endOffset, relScanCursor.sourceNodeReference())
     *     }
     *     setCachedPropertyAt(relOffset, [getPropertyValue])
     *     << inner.genOperate >>
     *     forwardDirection = !forwardDirection
     *     this.canContinue = !forwardDirection || this.relIndexCursor.next()
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        declareAndAssign(localRelVar, invoke(loadField(relIndexCursorField), method[RelationshipValueIndexCursor, Long]("relationshipReference"))),
        codeGen.setLongAt(relOffset, load[Long](localRelVar)),
        ifElse(loadField(forwardDirection)) {
          block(
            singleRelationship(load[Long](localRelVar), loadField(relScanCursorField)),
            invokeStatic(method[Preconditions, Unit, Boolean, String]("checkState"),
              profilingCursorNext[RelationshipScanCursor](loadField(relScanCursorField), id, doProfile), constant("Missing relationship")),
            codeGen.setLongAt(startOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
            codeGen.setLongAt(endOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("targetNodeReference")))
          )
        } {//else
          block(
            codeGen.setLongAt(startOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("targetNodeReference"))),
            codeGen.setLongAt(endOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("sourceNodeReference")))
          )
        },
        cacheProperties,
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(
          block(
            setField(forwardDirection, not(loadField(forwardDirection))),
            innermost.setUnlessPastLimit(canContinue, or(not(loadField(forwardDirection)), profilingCursorNext[RelationshipValueIndexCursor](loadField(relIndexCursorField), id, doProfile))),
          )
        ),
        endInnerLoop
      )
    )
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
                                                                        argumentSize)(codeGen) with UndirectedSeek {

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
    argumentSize)(codeGen) with UndirectedSeek {

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
    id, innermost, relOffset, startOffset, endOffset, property, seekExpression, queryIndexId, order, argumentSize)(codeGen) with UndirectedSeek {

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
  extends CompositeDirectedRelationshipIndexSeekTaskTemplate(inner, id, innermost, relOffset, startOffset, endOffset, properties, seekExpressions, queryIndexId, order, argumentSize)(codeGen) with UndirectedSeek {
  override def genMoreFields: Seq[Field] = super.genMoreFields :+ forwardDirection
}























