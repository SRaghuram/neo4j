/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.internal.kernel.api.{NodeCursor, Scan}

class AllNodeScanOperator(val workIdentity: WorkIdentity,
                          offset: Int,
                          argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    if (state.singeThreaded) {
      // Single threaded scan
      IndexedSeq(new SingleThreadedScanTask(inputMorsel.nextCopy))
    } else {
      // Parallel scan
      val scan = queryContext.transactionalContext.dataRead.allNodesScan()
      val tasks = new Array[ContinuableOperatorTaskWithMorsel](state.numberOfWorkers)
      var i = 0
      while (i < state.numberOfWorkers) {
        // Each task gets its own cursor which is reuses until it's done.
        val cursor = resources.cursorPools.nodeCursorPool.allocate()
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

    override def toString: String = "AllNodeScanSerialTask"

    private var cursor: NodeCursor = _

    override protected def initializeInnerLoop(context: QueryContext, state: QueryState, resources: QueryResources): Boolean = {
      cursor = resources.cursorPools.nodeCursorPool.allocate()
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
                         val cursor: NodeCursor,
                         val batchSizeHint: Int) extends ContinuableOperatorTaskWithMorsel {

    override def toString: String = "AllNodeScanParallelTask"

    private var _canContinue: Boolean = true
    private var deferredRow: Boolean = false

    /**
      * These 2 lines make sure that the first call to [[next]] is correct.
      */
    scan.reserveBatch(cursor, batchSizeHint)
    inputMorsel.setToAfterLastRow()

    override def operate(outputRow: MorselExecutionContext, context: QueryContext, queryState: QueryState, resources: QueryResources): Unit = {
      while (next(queryState) && outputRow.isValidRow) {
        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setLongAt(offset, cursor.nodeReference())
        outputRow.moveToNextRow()
      }

      if (!outputRow.isValidRow && _canContinue) {
        deferredRow = true
      }

      outputRow.finishedWriting()
    }


    private def next(queryState: QueryState): Boolean = {
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
          cursor.close()
          _canContinue = false
          return false
        }
      }

      throw new IllegalStateException("Unreachable code")
    }

    override def canContinue: Boolean = _canContinue
  }

}

class SingleThreadedAllNodeScanTaskTemplate(val inner: OperatorTaskTemplate,
                                            val innermost: DelegateOperatorTaskTemplate,
                                            val nodeVarName: String,
                                            val offset: Int,
                                            val argumentSize: SlotConfiguration.Size)
                                           (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, innermost, codeGen) {
  import OperatorCodeGenHelperTemplates._

  private val nodeCursorField = field[NodeCursor](codeGen.namer.nextVariableName())

  override def genFields: Seq[Field] = {
    (super.genFields :+ nodeCursorField) ++ inner.genFields
  }

  override def genLocalVariables: Seq[LocalVariable] = {
    inner.genLocalVariables :+ CURSOR_POOL_V
  }

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    /**
      * {{{
      *   this.nodeCursor = resources.cursorPools.nodeCursorPool.allocate()
      *   context.transactionalContext.dataRead.allNodesScan(cursor)
      *   this.canContinue = nodeCursor.next()
      *   true
      * }}}
      */
    block(
      setField(nodeCursorField, ALLOCATE_NODE_CURSOR),
      allNodeScan(loadField(nodeCursorField)),
      setField(canContinue, cursorNext[NodeCursor](loadField(nodeCursorField))),
      constant(true)
    )
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
     /**
      * {{{
      *   while (hasDemand && this.canContinue) {
      *     ...
      *     << inner.genOperate >>
      *     this.canContinue = this.nodeCursor.next()
      *   }
      * }}}
      */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        // TODO: This argument slot copy is not strictly necessary for slots with locals that are used within this pipeline
        //       We can assume there is a prefix range of 0 to n initial arguments that are not accessed within the pipeline
        //       that needs to be array-copied because a pipeline of an outer nesting may need them later on,
        //       and an suffix range of n+1 to m arguments that are being used in this pipeline, and thus declared as locals.
        //       The suffix range will be written from locals to the context by the innermost template,
        //       (unless this pipeline ends with a ProduceResult, in which case it is written directly to the result),
        //       so we do not need to include it in this copy.
        // If the pipeline ends with a ProduceResult, the prefix range array copy could be skipped entirely
        // since it means nobody is interested in those arguments.
        if (innermost.shouldWriteToContext && (argumentSize.nLongs > 0 || argumentSize.nReferences > 0)) {
          invokeSideEffect(OUTPUT_ROW, method[SinglePARG, Unit, ExecutionContext, Int, Int]("copyFrom"),
                           loadField(INPUT_MORSEL), constant(argumentSize.nLongs), constant(argumentSize.nReferences))
        } else {
          noop()
        },
        codeGen.setLongAt(offset, invoke(loadField(nodeCursorField), method[NodeCursor, Long]("nodeReference"))),
        inner.genOperate,
        setField(canContinue, cursorNext[NodeCursor](loadField(nodeCursorField)))
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

}
