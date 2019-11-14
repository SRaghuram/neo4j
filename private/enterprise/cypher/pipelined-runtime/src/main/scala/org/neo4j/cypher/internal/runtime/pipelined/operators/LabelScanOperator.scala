/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel.UNKNOWN
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.{PipelinedExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.{KernelReadTracer, NodeLabelIndexCursor}

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
  class SingleThreadedScanTask(val inputMorsel: PipelinedExecutionContext) extends InputLoopTask {

    override def workIdentity: WorkIdentity = LabelScanOperator.this.workIdentity

    override def toString: String = "LabelScanSerialTask"

    private var cursor: NodeLabelIndexCursor = _

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val id = label.getId(context)
      if (id == UNKNOWN) false
      else {
        cursor = resources.cursorPools.nodeLabelIndexCursorPool.allocateAndTrace()
        val read = context.transactionalContext.dataRead
        read.nodeLabelScan(id, cursor)
        true
      }
    }

    override protected def innerLoop(outputRow: PipelinedExecutionContext, context: QueryContext, state: QueryState): Unit = {
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

  import OperatorCodeGenHelperTemplates._

  private val nodeLabelCursorField = field[NodeLabelIndexCursor](codeGen.namer.nextVariableName())
  private val labelField = field[Int](codeGen.namer.nextVariableName(), NO_TOKEN)

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
