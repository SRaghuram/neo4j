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
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.NodeCursorRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_NODE_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NodeCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allNodeScan
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.Scan

class AllNodeScanOperator(val workIdentity: WorkIdentity,
                          offset: Int,
                          argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    if (parallelism == 1) {
      // Single threaded scan
      singletonIndexedSeq(new SingleThreadedScanTask(inputMorsel.nextCopy))
    } else {
      // Parallel scan
      val scan = state.queryContext.transactionalContext.dataRead.allNodesScan()
      val tasks = new Array[ContinuableOperatorTaskWithMorsel](parallelism)
      var i = 0
      while (i < parallelism) {
        // Each task gets its own cursor which it reuses until it's done.
        val cursor = resources.cursorPools.nodeCursorPool.allocateAndTrace()
        val rowForTask = inputMorsel.nextCopy
        tasks(i) = new ParallelScanTask(rowForTask, scan, cursor, state.morselSize)
        i += 1
      }
      tasks
    }
  }

  /**
   * A [[SingleThreadedScanTask]] will iterate over all inputRows and do a full scan for each of them.
   *
   * @param inputMorsel the input row, pointing to the beginning of the input morsel
   */
  class SingleThreadedScanTask(inputMorsel: Morsel) extends InputLoopTask(inputMorsel) {

    override def workIdentity: WorkIdentity = AllNodeScanOperator.this.workIdentity

    override def toString: String = "AllNodeScanSerialTask"

    private var cursor: NodeCursor = _

    override protected def initializeInnerLoop(state: PipelinedQueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ReadWriteRow): Boolean = {
      cursor = resources.cursorPools.nodeCursorPool.allocateAndTrace()
      state.queryContext.transactionalContext.dataRead.allNodesScan(cursor)
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
      while (outputRow.onValidRow && cursor.next()) {
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
      resources.cursorPools.nodeCursorPool.free(cursor)
      cursor = null
    }
  }

  /**
   * A [[ParallelScanTask]] reserves new batches from the Scan, until there are no more batches. It competes for these batches with other
   * concurrently running [[ParallelScanTask]]s.
   *
   * For each batch, it process all the nodes and combines them with each input row.
   */
  class ParallelScanTask(val inputMorsel: Morsel,
                         scan: Scan[NodeCursor],
                         var cursor: NodeCursor,
                         val batchSizeHint: Int) extends ContinuableOperatorTaskWithMorsel {

    override def workIdentity: WorkIdentity = AllNodeScanOperator.this.workIdentity

    override def toString: String = "AllNodeScanParallelTask"

    private var _canContinue: Boolean = true
    private var deferredRow: Boolean = false
    val inputCursor: MorselReadCursor = inputMorsel.readCursor()

    /**
     * These 2 lines make sure that the first call to [[next]] is correct.
     */
    scan.reserveBatch(cursor, batchSizeHint)
    inputCursor.setToEnd()

    override def operate(outputMorsel: Morsel,
                         queryState: PipelinedQueryState,
                         resources: QueryResources): Unit = {

      val outputCursor = outputMorsel.writeCursor(onFirstRow = true)
      while (next(queryState, resources) && outputCursor.onValidRow) {
        outputCursor.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
        outputCursor.setLongAt(offset, cursor.nodeReference())
        outputCursor.next()
      }

      if (!outputCursor.onValidRow && _canContinue) {
        deferredRow = true
      }

      outputCursor.truncate()
    }


    private def next(queryState: PipelinedQueryState, resources: QueryResources): Boolean = {
      while (true) {
        if (deferredRow) {
          deferredRow = false
          return true
        } else if (inputCursor.hasNext) {
          inputCursor.next()
          return true
        } else if (cursor.next()) {
          inputCursor.setToStart()
        } else if (scan.reserveBatch(cursor, batchSizeHint)) {
          // Do nothing
        } else {
          // We ran out of work
          _canContinue = false
          return false
        }
      }

      throw new IllegalStateException("Unreachable code")
    }

    override def canContinue: Boolean = _canContinue

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      if (cursor != null) {
        cursor.setTracer(event)
      }
    }

    override protected def closeCursors(resources: QueryResources): Unit = {
      resources.cursorPools.nodeCursorPool.free(cursor)
      cursor = null
    }
  }

}

class SingleThreadedAllNodeScanTaskTemplate(inner: OperatorTaskTemplate,
                                            id: Id,
                                            val innermost: DelegateOperatorTaskTemplate,
                                            val nodeVarName: String,
                                            val offset: Int,
                                            val argumentSize: SlotConfiguration.Size)
                                           (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  private val nodeCursorField = field[NodeCursor](codeGen.namer.nextVariableName())

  codeGen.registerCursor(nodeVarName, NodeCursorRepresentation(loadField(nodeCursorField), canBeNull = false, codeGen))

  override final def scopeId: String = "allNodesScan" + id.x

  override def genMoreFields: Seq[Field] = Seq(nodeCursorField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   this.nodeCursor = resources.cursorPools.nodeCursorPool.allocate()
     *   context.transactionalContext.dataRead.allNodesScan(cursor)
     *   val tmp = nodeCursor.next()
     *   profileRow(tmp)
     *   this.canContinue = tmp
     *   true
     * }}}
     */
    block(
      allocateAndTraceCursor(nodeCursorField, executionEventField, ALLOCATE_NODE_CURSOR, doProfile),
      allNodeScan(loadField(nodeCursorField)),
      setField(canContinue, profilingCursorNext[NodeCursor](loadField(nodeCursorField), id, doProfile, codeGen.namer)),
      constant(true)
    )
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     ...
     *     << inner.genOperate >>
     *   val tmp = nodeCursor.next()
     *   profileRow(tmp)
     *   this.canContinue = tmp
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        // If the pipeline ends with a ProduceResult, the prefix range array copy could be skipped entirely
        // since it means nobody is interested in those arguments.
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        codeGen.setLongAt(offset, invoke(loadField(nodeCursorField), method[NodeCursor, Long]("nodeReference"))),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(innermost.setUnlessPastLimit(canContinue, profilingCursorNext[NodeCursor](loadField(nodeCursorField), id, doProfile, codeGen.namer))),
        endInnerLoop
      )
    )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    //resources.cursorPools.nodeCursorPool.free(cursor)
    //cursor = null
    block(
      freeCursor[NodeCursor](loadField(nodeCursorField), NodeCursorPool),
      setField(nodeCursorField, constant(null))
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      condition(isNotNull(loadField(nodeCursorField)))(
        invokeSideEffect(loadField(nodeCursorField), method[NodeCursor, Unit, KernelReadTracer]("setTracer"), loadField(executionEventField))
      ),
      inner.genSetExecutionEvent(event)
    )
  }
}
