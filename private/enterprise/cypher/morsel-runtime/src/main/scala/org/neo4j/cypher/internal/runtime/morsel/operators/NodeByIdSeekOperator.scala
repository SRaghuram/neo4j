/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable, Method}
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SeekArgs
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.NodeByIdSeekOperator.{asId, asIdMethod}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.{IndexReadSession, Read}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue


class NodeByIdSeekOperator(val workIdentity: WorkIdentity,
                           offset: Int,
                           nodeIdsExpr: SeekArgs,
                           argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

      IndexedSeq(new NodeByIdTask(inputMorsel.nextCopy))
  }


  class NodeByIdTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = "NodeByIdTask"

    private var ids: java.util.Iterator[AnyValue] = _

    /**
      * Initialize the inner loop for the current input row.
      *
      * @return true iff the inner loop might result it output rows
      */
    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession],
                                         resources.expressionVariables(state.nExpressionSlots),
                                         state.subscriber)
      initExecutionContext.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
      ids = nodeIdsExpr.expressions(initExecutionContext, queryState).iterator()
      true
    }

    override def workIdentity: WorkIdentity = NodeByIdSeekOperator.this.workIdentity

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {

      while (outputRow.isValidRow && ids.hasNext) {
        val nextId = asId(ids.next())
        if (nextId >= 0L && context.transactionalContext.dataRead.nodeExists(nextId)) {
          outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
          outputRow.setLongAt(offset, nextId)
          outputRow.moveToNextRow()
        }
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
     //nothing to do here
    }
  }
}

object NodeByIdSeekOperator {
  def asId(value: AnyValue): Long = value match {
    case d:IntegralValue => d.longValue()
    case _ => -1L
  }

  val asIdMethod: Method = method[NodeByIdSeekOperator, Long, AnyValue]("asId")
}

class SingleNodeByIdSeekTaskTemplate(override val inner: OperatorTaskTemplate,
                                     id: Id,
                                     val innermost: DelegateOperatorTaskTemplate,
                                     val nodeVarName: String,
                                     val offset: Int,
                                     nodeIdExpr: Expression,
                                     val argumentSize: SlotConfiguration.Size)
                                         (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._

  private val idVariable = variable[Long](codeGen.namer.nextVariableName(), constant(-1L))
  private var nodeId: IntermediateExpression= _

  override def genFields: Seq[Field] = {
    nodeId.fields ++ super.genFields ++ inner.genFields
  }

  override def genLocalVariables: Seq[LocalVariable] = {
    nodeId.variables ++ inner.genLocalVariables :+ CURSOR_POOL_V :+ idVariable
  }

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    nodeId = codeGen.intermediateCompileExpression(nodeIdExpr).getOrElse(throw new CantCompileQueryException())

    /**
      * {{{
      *   this.id = asId(<<nodeIdExpr>>)
      *   this.canContinue = id >= 0 && read.nodeExists(id)
      *   true
      * }}}
      */
    block(
      assign(idVariable, invokeStatic(asIdMethod, nullCheckIfRequired(nodeId))),
      setField(canContinue, and(greaterThanOrEqual(load(idVariable), constant(0L)),
                                invoke(loadField(DATA_READ), method[Read, Boolean, Long]("nodeExists"),
                                       load(idVariable)))),
      constant(true))
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
      * {{{
      *   while (hasDemand && this.canContinue) {
      *     ...
      *     setLongAt(offset, id)
      *     << inner.genOperate >>
      *     this.canContinue = false
      *   }
      * }}}
      */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        if (innermost.shouldWriteToContext && (argumentSize.nLongs > 0 || argumentSize.nReferences > 0)) {
          invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit, ExecutionContext, Int, Int]("copyFrom"),
                           loadField(INPUT_MORSEL), constant(argumentSize.nLongs), constant(argumentSize.nReferences))
        } else {
          noop()
        },
        codeGen.setLongAt(offset, load(idVariable)),
        profileRow(id),
        inner.genOperate,
        setField(canContinue, constant(false)))
      )
  }

  //nothing to close
  override protected def genCloseInnerLoop: IntermediateRepresentation = noop()
}



