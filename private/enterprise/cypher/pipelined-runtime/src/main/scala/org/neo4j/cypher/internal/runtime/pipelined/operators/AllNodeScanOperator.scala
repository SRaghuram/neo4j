/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.{KernelReadTracer, NodeCursor, Scan}

class AllNodeScanOperator(val workIdentity: WorkIdentity,
                          offset: Int,
                          argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    if (parallelism == 1) {
      // Single threaded scan
      IndexedSeq(new SingleThreadedScanTask(inputMorsel.nextCopy))
    } else {
      // Parallel scan
      val scan = queryContext.transactionalContext.dataRead.allNodesScan()
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
  class SingleThreadedScanTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def workIdentity: WorkIdentity = AllNodeScanOperator.this.workIdentity

    override def toString: String = "AllNodeScanSerialTask"

    private var cursor: NodeCursor = _

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      cursor = resources.cursorPools.nodeCursorPool.allocateAndTrace()
      context.transactionalContext.dataRead.allNodesScan(cursor)
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
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
  class ParallelScanTask(val inputMorsel: MorselExecutionContext,
                         scan: Scan[NodeCursor],
                         var cursor: NodeCursor,
                         val batchSizeHint: Int) extends ContinuableOperatorTaskWithMorsel {

    override def workIdentity: WorkIdentity = AllNodeScanOperator.this.workIdentity

    override def toString: String = "AllNodeScanParallelTask"

    private var _canContinue: Boolean = true
    private var deferredRow: Boolean = false

    /**
      * These 2 lines make sure that the first call to [[next]] is correct.
      */
    scan.reserveBatch(cursor, batchSizeHint)
    inputMorsel.setToAfterLastRow()

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         queryState: QueryState,
                         resources: QueryResources): Unit = {

      while (next(queryState, resources) && outputRow.isValidRow) {
        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setLongAt(offset, cursor.nodeReference())
        outputRow.moveToNextRow()
      }

      if (!outputRow.isValidRow && _canContinue) {
        deferredRow = true
      }

      outputRow.finishedWriting()
    }


    private def next(queryState: QueryState, resources: QueryResources): Boolean = {
      while (true) {
        if (deferredRow) {
          deferredRow = false
          return true
        } else if (inputMorsel.hasNextRow) {
          inputMorsel.moveToNextRow()
          return true
        } else if (cursor.next()) {
          inputMorsel.resetToBeforeFirstRow()
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
  import OperatorCodeGenHelperTemplates._

  private val nodeCursorField = field[NodeCursor](codeGen.namer.nextVariableName())

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
      allocateAndTraceCursor(nodeCursorField, executionEventField, ALLOCATE_NODE_CURSOR),
      allNodeScan(loadField(nodeCursorField)),
      setField(canContinue, profilingCursorNext[NodeCursor](loadField(nodeCursorField), id)),
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
        doIfInnerCantContinue(setField(canContinue, profilingCursorNext[NodeCursor](loadField(nodeCursorField), id))),
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
