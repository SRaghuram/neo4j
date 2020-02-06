/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel.UNKNOWN
import org.neo4j.cypher.internal.runtime.pipelined.NodeLabelCursorRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselCypherRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
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
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor


class LabelScanOperator(val workIdentity: WorkIdentity,
                        offset: Int,
                        label: LazyLabel,
                        argumentSize: SlotConfiguration.Size)
  extends StreamingOperator {

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    // Single threaded scan
    IndexedSeq(new SingleThreadedScanTask(inputMorsel.nextCopy))
  }

  /**
   * A [[SingleThreadedScanTask]] will iterate over all inputRows and do a full scan for each of them.
   *
   * @param inputMorsel the input row, pointing to the beginning of the input morsel
   */
  class SingleThreadedScanTask(val inputMorsel: MorselCypherRow) extends InputLoopTask {

    override def workIdentity: WorkIdentity = LabelScanOperator.this.workIdentity

    override def toString: String = "LabelScanSerialTask"

    private var cursor: NodeLabelIndexCursor = _

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: CypherRow): Boolean = {
      val id = label.getId(context)
      if (id == UNKNOWN) false
      else {
        cursor = resources.cursorPools.nodeLabelIndexCursorPool.allocateAndTrace()
        val read = context.transactionalContext.dataRead
        read.nodeLabelScan(id, cursor)
        true
      }
    }

    override protected def innerLoop(outputRow: MorselCypherRow, context: QueryContext, state: QueryState): Unit = {
      while (outputRow.isValidRow && cursor.next()) {
        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setLongAt(offset, cursor.nodeReference())
        outputRow.moveToNextRow()
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
                                          argumentSize: SlotConfiguration.Size)
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
          allocateAndTraceCursor(nodeLabelCursorField, executionEventField, ALLOCATE_NODE_LABEL_CURSOR),
          nodeLabelScan(constant(labelId), loadField(nodeLabelCursorField)),
          setField(canContinue, profilingCursorNext[NodeLabelIndexCursor](loadField(nodeLabelCursorField), id)),
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
          setField(canContinue, load(hasInnerLoop)),
          condition(load(hasInnerLoop)) {
            block(
              allocateAndTraceCursor(nodeLabelCursorField, executionEventField, ALLOCATE_NODE_LABEL_CURSOR),
              nodeLabelScan(loadField(labelField), loadField(nodeLabelCursorField)),
              setField(canContinue, profilingCursorNext[NodeLabelIndexCursor](loadField(nodeLabelCursorField), id)),
            )
          },
          load(hasInnerLoop)
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
        doIfInnerCantContinue(setField(canContinue, profilingCursorNext[NodeLabelIndexCursor](loadField(nodeLabelCursorField), id))),
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
