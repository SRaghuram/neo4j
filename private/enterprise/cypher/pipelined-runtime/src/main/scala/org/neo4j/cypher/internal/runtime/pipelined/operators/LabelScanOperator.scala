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
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel.UNKNOWN
import org.neo4j.cypher.internal.runtime.pipelined.NodeLabelCursorRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_NODE_LABEL_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NO_TOKEN
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NodeLabelIndexCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.nodeLabelId
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.nodeLabelScan
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.schema.IndexOrder


class LabelScanOperator(val workIdentity: WorkIdentity,
                        offset: Int,
                        label: LazyLabel,
                        argumentSize: SlotConfiguration.Size,
                        indexOrder: IndexOrder)
  extends StreamingOperator {

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    // Single threaded scan
    singletonIndexedSeq(new SingleThreadedScanTask(inputMorsel.nextCopy))
  }

  /**
   * A [[SingleThreadedScanTask]] will iterate over all inputRows and do a full scan for each of them.
   *
   * @param inputMorsel the input row, pointing to the beginning of the input morsel
   */
  class SingleThreadedScanTask(inputMorsel: Morsel) extends InputLoopTask(inputMorsel) {

    override def workIdentity: WorkIdentity = LabelScanOperator.this.workIdentity

    override def toString: String = "LabelScanSerialTask"

    private var cursor: NodeLabelIndexCursor = _

    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      val id = label.getId(state.queryContext)
      if (id == UNKNOWN) false
      else {
        cursor = resources.cursorPools.nodeLabelIndexCursorPool.allocateAndTrace()
        val read = state.queryContext.transactionalContext.dataRead
        read.nodeLabelScan(id, cursor, indexOrder)
        true
      }
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
      while (outputRow.onValidRow() && cursor.next()) {
        outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setLongAt(offset, cursor.nodeReference())
        outputRow.next()
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      if (cursor != null) {
        cursor.setTracer(event)
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.nodeLabelIndexCursorPool.free(cursor)
      cursor = null
    }
  }

}

class SingleThreadedLabelScanTaskTemplate(inner: OperatorTaskTemplate,
                                          id: Id,
                                          innermost: DelegateOperatorTaskTemplate,
                                          nodeVarName: String,
                                          offset: Int,
                                          labelName: String,
                                          maybeLabelId: Option[Int],
                                          argumentSize: SlotConfiguration.Size,
                                          indexOrder: IndexOrder)
                                         (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {


  private val nodeLabelCursorField = field[NodeLabelIndexCursor](codeGen.namer.nextVariableName())
  private val labelField = field[Int](codeGen.namer.nextVariableName(), NO_TOKEN)

  codeGen.registerCursor(nodeVarName, NodeLabelCursorRepresentation(loadField(nodeLabelCursorField)))

  override final def scopeId: String = "labelScan" + id.x

  override def genMoreFields: Seq[Field] = {
    if (maybeLabelId.isDefined) {
      Seq(nodeLabelCursorField)
    } else {
      Seq(nodeLabelCursorField, labelField)
    }
  }

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    maybeLabelId match {
      case Some(labelId) =>

        /**
         * {{{
         *   this.nodeLabelCursor = resources.cursorPools.nodeLabelIndexCursorPool.allocate()
         *   context.transactionalContext.dataRead.nodeLabelScan(id, cursor)
         *   val tmp = nodeLabelCursor.next()
         *   profileRow(tmp)
         *   this.canContinue = tmp
         *   true
         * }}}
         */
        block(
          allocateAndTraceCursor(nodeLabelCursorField, executionEventField, ALLOCATE_NODE_LABEL_CURSOR, doProfile),
          nodeLabelScan(constant(labelId), loadField(nodeLabelCursorField), indexOrder),
          setField(canContinue, profilingCursorNext[NodeLabelIndexCursor](loadField(nodeLabelCursorField), id, doProfile, codeGen.namer)),
          constant(true)
        )

      case None =>
        val hasInnerLoop = codeGen.namer.nextVariableName()
        /**
         * {{{
         *   if (this.label == NO_TOKEN) {
         *     this.label = nodeLabelId(labelName)
         *   }
         *   val hasInnerLoop = this.label != NO_TOKEN
         *   if (hasInnerLoop) {
         *     this.nodeLabelCursor = resources.cursorPools.nodeLabelIndexCursorPool.allocate()
         *     context.transactionalContext.dataRead.nodeLabelScan(id, cursor)
         *     val tmp = nodeLabelCursor.next()
         *     profileRow(tmp)
         *     this.canContinue = tmp
         *   }
         *   hasInnerLoop
         * }}}
         */
        block(
          condition(equal(loadField(labelField), NO_TOKEN)) {
            setField(labelField, nodeLabelId(labelName))
          },
          declareAndAssign(typeRefOf[Boolean], hasInnerLoop, notEqual(loadField(labelField), NO_TOKEN)),
          setField(canContinue, load[Boolean](hasInnerLoop)),
          condition(load[Boolean](hasInnerLoop)) {
            block(
              allocateAndTraceCursor(nodeLabelCursorField, executionEventField, ALLOCATE_NODE_LABEL_CURSOR, doProfile),
              nodeLabelScan(loadField(labelField), loadField(nodeLabelCursorField), indexOrder),
              setField(canContinue, profilingCursorNext[NodeLabelIndexCursor](loadField(nodeLabelCursorField), id, doProfile, codeGen.namer)),
            )
          },
          load[Boolean](hasInnerLoop)
        )
    }
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     ...
     *     setLongAt(offset, nodeLabelCursor.nodeReference())
     *     << inner.genOperate >>
     *     val tmp = nodeLabelCursor.next()
     *     profileRow(tmp)
     *     this.canContinue = tmp
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        codeGen.setLongAt(offset, invoke(loadField(nodeLabelCursorField), method[NodeLabelIndexCursor, Long]("nodeReference"))),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(
          innermost.setUnlessPastLimit(canContinue, profilingCursorNext[NodeLabelIndexCursor](loadField(nodeLabelCursorField), id, doProfile, codeGen.namer))
        ),
        endInnerLoop
      )
    )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   resources.cursorPools.nodeLabelIndexCursorPool.free(nodeLabelCursor)
     *   nodeLabelCursor = null
     * }}}
     */
    block(
      freeCursor[NodeLabelIndexCursor](loadField(nodeLabelCursorField), NodeLabelIndexCursorPool),
      setField(nodeLabelCursorField, constant(null))
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      condition(isNotNull(loadField(nodeLabelCursorField)))(
        invokeSideEffect(loadField(nodeLabelCursorField), method[NodeLabelIndexCursor, Unit, KernelReadTracer]("setTracer"), event)
      ),
      inner.genSetExecutionEvent(event)
    )
  }
}
