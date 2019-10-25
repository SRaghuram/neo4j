/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable, Method}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SeekArgs
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.NodeByIdSeekOperator.{asIdMethod, isValidNode}
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates.DATA_READ
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.{IndexReadSession, KernelReadTracer, Read}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue


class NodeByIdSeekOperator(val workIdentity: WorkIdentity,
                           offset: Int,
                           nodeIdsExpr: SeekArgs,
                           argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

      IndexedSeq(new NodeByIdTask(inputMorsel.nextCopy))
  }


  class NodeByIdTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = "NodeByIdTask"

    private var ids: java.util.Iterator[AnyValue] = _
    private var tracer: KernelReadTracer = KernelReadTracer.NONE

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
                                         state.subscriber,
                                         NoMemoryTracker)
      initExecutionContext.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
      ids = nodeIdsExpr.expressions(initExecutionContext, queryState).iterator()
      true
    }

    override def workIdentity: WorkIdentity = NodeByIdSeekOperator.this.workIdentity

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {

      while (outputRow.isValidRow && ids.hasNext) {
        val nextId = NumericHelper.asLongEntityIdPrimitive(ids.next())
        tracer.onNode(nextId)
        if (nextId >= 0L && context.transactionalContext.dataRead.nodeExists(nextId)) {
          outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
          outputRow.setLongAt(offset, nextId)
          outputRow.moveToNextRow()
        }
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      this.tracer = event
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      //nothing to do here
    }
  }
}

object NodeByIdSeekOperator  {

  val asIdMethod: Method = method[NumericHelper, Long, AnyValue]("asLongEntityIdPrimitive")

  def isValidNode(idVariable: String): IntermediateRepresentation =
    and(greaterThanOrEqual(load(idVariable), constant(0L)),
        invoke(loadField(DATA_READ), method[Read, Boolean, Long]("nodeExists"),
               load(idVariable)))
}

class SingleNodeByIdSeekTaskTemplate(inner: OperatorTaskTemplate,
                                     id: Id,
                                     innermost: DelegateOperatorTaskTemplate,
                                     nodeVarName: String,
                                     offset: Int,
                                     nodeIdExpr: Expression,
                                     argumentSize: SlotConfiguration.Size)
                                    (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._

  private val idVariable = variable[Long](codeGen.namer.nextVariableName(), constant(-1L))
  private var nodeId: IntermediateExpression = _

  override def genMoreFields: Seq[Field] = Seq.empty

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V, idVariable)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = noop()

  override def genExpressions: Seq[IntermediateExpression] = Seq(nodeId)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    nodeId = codeGen.intermediateCompileExpression(nodeIdExpr).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $nodeIdExpr"))

    /**
      * {{{
      *   id = asId(<<nodeIdExpr>>)
      *   this.canContinue = id >= 0 && read.nodeExists(id)
      *   tracer.onNode(id)
      *   true
      * }}}
      */
    block(
      assign(idVariable, invokeStatic(asIdMethod, nullCheckIfRequired(nodeId))),
      setField(canContinue, isValidNode(idVariable.name)),
      invoke(loadField(executionEventField), TRACE_ON_NODE, load(idVariable)),
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
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        codeGen.setLongAt(offset, load(idVariable)),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(profileRow(id)),
        setField(canContinue, constant(false)),
        endInnerLoop),
      )
  }

  //nothing to close
  override protected def genCloseInnerLoop: IntermediateRepresentation = noop()
}

class ManyNodeByIdsSeekTaskTemplate(inner: OperatorTaskTemplate,
                                    id: Id,
                                    innermost: DelegateOperatorTaskTemplate,
                                    nodeVarName: String,
                                    offset: Int,
                                    nodeIdsExpr: Expression,
                                    argumentSize: SlotConfiguration.Size)
                                    (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._

  private val idIterator = field[java.util.Iterator[AnyValue]](codeGen.namer.nextVariableName())
  private var nodeIds: IntermediateExpression= _

  override def genMoreFields: Seq[Field] = Seq(idIterator)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genExpressions: Seq[IntermediateExpression] = Seq(nodeIds)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = noop()

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    nodeIds = codeGen.intermediateCompileExpression(nodeIdsExpr).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $nodeIdsExpr"))

    /**
      * {{{
      *   this.idIterator = ((ListValue) <<nodeIdExpr>>)).iterator()
      *   this.canContinue = idIterator.hasNext
      *   true
      * }}}
      */
    block(
      setField(idIterator,
               invoke(cast[ListValue](nullCheckIfRequired(nodeIds)), method[ListValue, util.Iterator[AnyValue]]("iterator"))),
      setField(canContinue, invoke(loadField(idIterator), method[util.Iterator[_], Boolean]("hasNext"))),
      constant(true))
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
   val idVariable = codeGen.namer.nextVariableName()
    /**
      * {{{
      *   while (hasDemand && this.canContinue) {
      *     val id = asId(idIterator.next())
      *     tracer.onNode(id)
      *     if (id >= 0 && read.nodeExists(id)) {
      *       setLongAt(offset, id)
      *       << inner.genOperate >>
      *     }
      *     this.canContinue = itIterator.hasNext()
      *   }
      * }}}
      */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        declareAndAssign(typeRefOf[Long], idVariable,
                         invokeStatic(asIdMethod, cast[AnyValue](
                           invoke(loadField(idIterator),
                                  method[java.util.Iterator[AnyValue], Object]("next"))))),
        invoke(loadField(executionEventField), TRACE_ON_NODE, load(idVariable)),
        condition(isValidNode(idVariable))(
          block(
            codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
            codeGen.setLongAt(offset, load(idVariable)),
            profileRow(id),
            inner.genOperateWithExpressions
            )),
        setField(canContinue, invoke(loadField(idIterator), method[util.Iterator[_], Boolean]("hasNext"))),
        endInnerLoop)
      )
  }

  //nothing to close
  override protected def genCloseInnerLoop: IntermediateRepresentation = noop()
}



