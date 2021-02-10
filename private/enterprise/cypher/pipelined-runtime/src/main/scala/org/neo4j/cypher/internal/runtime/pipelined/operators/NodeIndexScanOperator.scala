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
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.NodeIndexCursorRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_NODE_INDEX_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NodeValueIndexCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.indexReadSession
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.nodeIndexScan
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexQueryConstraints
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.values.storable.Value

class NodeIndexScanOperator(val workIdentity: WorkIdentity,
                            nodeOffset: Int,
                            properties: Array[SlottedIndexedProperty],
                            queryIndexId: Int,
                            indexOrder: IndexOrder,
                            argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, properties) {

  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    val indexSession = state.queryIndexes(queryIndexId)
    singletonIndexedSeq(new OTask(inputMorsel.nextCopy, indexSession))
  }

  class OTask(inputMorsel: Morsel, index: IndexReadSession) extends InputLoopTask(inputMorsel) {

    override def workIdentity: WorkIdentity = NodeIndexScanOperator.this.workIdentity

    private var cursor: NodeValueIndexCursor = _

    protected override def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {

      cursor = resources.cursorPools.nodeValueIndexCursorPool.allocateAndTrace()
      val read = state.queryContext.transactionalContext.dataRead
      read.nodeIndexScan(index, cursor, IndexQueryConstraints.constrained(indexOrder, needsValues))
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
      iterate(inputCursor, outputRow, cursor, argumentSize)
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      if (cursor != null) {
        cursor.setTracer(event)
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.nodeValueIndexCursorPool.free(cursor)
      cursor = null
    }
  }
}

class NodeIndexScanTaskTemplate(inner: OperatorTaskTemplate,
                                id: Id,
                                innermost: DelegateOperatorTaskTemplate,
                                nodeVarName: String,
                                offset: Int,
                                properties: Array[SlottedIndexedProperty],
                                queryIndexId: Int,
                                indexOrder: IndexOrder,
                                argumentSize: SlotConfiguration.Size)
                               (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {


  private val nodeIndexCursorField = field[NodeValueIndexCursor](codeGen.namer.nextVariableName())
  private val needsValues = properties.exists(_.getValueFromIndex)

  codeGen.registerCursor(nodeVarName, NodeIndexCursorRepresentation(loadField(nodeIndexCursorField)))

  override def genMoreFields: Seq[Field] = Seq(nodeIndexCursorField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(nodeIndexCursorField)))(
        invokeSideEffect(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Unit, KernelReadTracer]("setTracer"), event)
      ),
      inner.genSetExecutionEvent(event)
    )

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   this.nodeIndexCursor = resources.cursorPools.nodeValuIndexCursorPool.allocate()
     *   context.transactionalContext.dataRead.nodeIndexScan(session, cursor, indexOrder, needsValues)
     *   val tmp = nodeCursor.next()
     *   profileRow(tmp)
     *   this.canContinue = tmp
     *   true
     * }}}
     */
    block(
      allocateAndTraceCursor(nodeIndexCursorField, executionEventField, ALLOCATE_NODE_INDEX_CURSOR, doProfile),
      nodeIndexScan(indexReadSession(queryIndexId), loadField(nodeIndexCursorField), indexOrder, needsValues),
      setField(canContinue, profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id, doProfile, codeGen.namer)),
      constant(true)
    )
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
    val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot)

    val cacheProperties = for (i <- indexPropertyIndices.indices) yield
      codeGen.setCachedPropertyAt(indexPropertySlotOffsets(i),
        invoke(loadField(nodeIndexCursorField),
          method[NodeValueIndexCursor, Value, Int]("propertyValue"),
          constant(indexPropertyIndices(i))))
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     setLongAt(offset, nodeIndexCursor.nodeReference())
     *     setCachedPropertyAt(cacheOffset1, nodeIndexCursor.propertyValue(0))
     *     setCachedPropertyAt(cacheOffset2, nodeIndexCursor.propertyValue(1))
     *     ...
     *     << inner.genOperate >>
     *     val tmp = nodeCursor.next()
     *     profileRow(tmp)
     *     this.canContinue = tmp      *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        codeGen.setLongAt(offset, invoke(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Long]("nodeReference"))),
        block(cacheProperties:_*),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(innermost.setUnlessPastLimit(canContinue, profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id, doProfile, codeGen.namer))),
        endInnerLoop
      )
    )
  }


  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   resources.cursorPools.nodeValueIndexCursorPool.free(nodeIndexCursor)
     *   nodeIndexCursor = null
     * }}}
     */
    block(
      freeCursor[NodeValueIndexCursor](loadField(nodeIndexCursorField), NodeValueIndexCursorPool),
      setField(nodeIndexCursorField, constant(null))
    )
  }
}

