/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException

class ArgumentOperator(val workIdentity: WorkIdentity,
                       argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def toString: String = "Argument"

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    IndexedSeq(new OTask(inputMorsel.nextCopy))

  class OTask(val inputMorsel: MorselExecutionContext) extends ContinuableOperatorTaskWithMorsel {

    override def workIdentity: WorkIdentity = ArgumentOperator.this.workIdentity

    override def toString: String = "ArgumentTask"

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      while (outputRow.isValidRow && inputMorsel.isValidRow) {
        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)

        inputMorsel.moveToNextRow()
        outputRow.moveToNextRow()
      }
      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = false

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
    override protected def closeCursors(resources: QueryResources): Unit = {}
  }
}

class ArgumentOperatorTaskTemplate(override val inner: OperatorTaskTemplate,
                                   override val id: Id,
                                   innermost: DelegateOperatorTaskTemplate,
                                   argumentSize: SlotConfiguration.Size,
                                   final override protected val isHead: Boolean = true)
                                   (protected val codeGen: OperatorExpressionCompiler) extends ContinuableOperatorTaskWithMorselTemplate {
  import OperatorCodeGenHelperTemplates._

  override protected def scopeId: String = "argument" + id.x

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genInit: IntermediateRepresentation = inner.genInit

  override protected def genOperateHead: IntermediateRepresentation = {
    block(
      labeledLoop(OUTER_LOOP_LABEL_NAME, and(invoke(self(), method[ContinuableOperatorTask, Boolean]("canContinue")), innermost.predicate))(
        {
          val body =
            block(
              codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
              profileRow(id),
              inner.genOperateWithExpressions,
              INPUT_ROW_MOVE_TO_NEXT,
              innermost.resetCachedPropertyVariables
            )
          // TODO: Here we still need to fix a way to propagate variables to save in the continuation state like we do in InputLoopTask
          body
        }
      )
    )
  }

  override protected def genOperateMiddle: IntermediateRepresentation = {
    throw new CantCompileQueryException("Cannot compile Input as middle operator")
  }

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty[IntermediateExpression]

  override def genFields: Seq[Field] = Seq.empty[Field]

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty[LocalVariable]

  override def genCanContinue: Option[IntermediateRepresentation] = {
    inner.genCanContinue.map(or(_, INPUT_ROW_IS_VALID)).orElse(Some(INPUT_ROW_IS_VALID))
  }

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors
}
