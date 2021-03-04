/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringSuffixPredicate
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.values.storable.TextValue

class DirectedRelationshipIndexEndsWithScanOperator(val workIdentity: WorkIdentity,
                                                    relOffset: Int,
                                                    startOffset: Int,
                                                    endOffset: Int,
                                                    property: SlottedIndexedProperty,
                                                    queryIndexId: Int,
                                                    indexOrder: IndexOrder,
                                                    valueExpr: Expression,
                                                    argumentSize: SlotConfiguration.Size)
  extends StreamingOperator {

  private val indexPropertySlotOffsets: Array[Int] = property.maybeCachedNodePropertySlot.toArray
  private val indexPropertyIndices: Array[Int] = if (property.maybeCachedNodePropertySlot.isDefined) Array(0) else Array.empty
  protected val needsValues: Boolean = indexPropertyIndices.nonEmpty

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    singletonIndexedSeq(
      new DirectedRelationshipIndexEndsWithScanTask(
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
        valueExpr,
        property.propertyKeyId)
    )
  }
}

class DirectedRelationshipIndexEndsWithScanTask(inputMorsel: Morsel,
                                                val workIdentity: WorkIdentity,
                                                relOffset: Int,
                                                startOffset: Int,
                                                endOffset: Int,
                                                indexPropertyIndices: Array[Int],
                                                indexPropertySlotOffsets: Array[Int],
                                                queryIndexId: Int,
                                                indexOrder: IndexOrder,
                                                argumentSize: SlotConfiguration.Size,
                                                valueExpr: Expression,
                                                propertyId: Int)
  extends DirectedRelationshipIndexStringSearchTask(relOffset, startOffset, endOffset, indexPropertyIndices, indexPropertySlotOffsets, queryIndexId, indexOrder, argumentSize, valueExpr, inputMorsel) {
  override protected def predicate(value: TextValue): PropertyIndexQuery.StringPredicate = PropertyIndexQuery.stringSuffix(propertyId, value)
}

class DirectedRelationshipIndexEndsWithScanTaskTemplate(inner: OperatorTaskTemplate,
                                                        id: Id,
                                                        innermost: DelegateOperatorTaskTemplate,
                                                        relOffset: Int,
                                                        startOffset: Int,
                                                        endOffset: Int,
                                                        property: SlottedIndexedProperty,
                                                        queryIndexId: Int,
                                                        indexOrder: IndexOrder,
                                                        argumentSize: SlotConfiguration.Size,
                                                        searchExpressionGen: () => IntermediateExpression,
                                                        codeGen: OperatorExpressionCompiler)
  extends BaseRelationshipIndexStringSearchTaskTemplate(inner, id, innermost, relOffset, startOffset, endOffset, property, queryIndexId, indexOrder, argumentSize, searchExpressionGen, codeGen) {
  override protected def predicate(searchExpression: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(method[PropertyIndexQuery, StringSuffixPredicate, Int, TextValue]("stringSuffix"), constant(property.propertyKeyId), searchExpression)
}











