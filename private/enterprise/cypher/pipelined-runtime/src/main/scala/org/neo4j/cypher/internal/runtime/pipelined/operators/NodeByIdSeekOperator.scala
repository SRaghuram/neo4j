/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators
import java.util

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.greaterThanOrEqual
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.Method
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SeekArgs
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeByIdSeekOperator.asIdMethod
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeByIdSeekOperator.isValidNode
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.cursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.onNode
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.Read
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue

class NodeByIdSeekOperator(val workIdentity: WorkIdentity,
                           offset: Int,
                           nodeIdsExpr: SeekArgs,
                           argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    IndexedSeq(new NodeByIdTask(inputMorsel.nextCopy))
  }


  class NodeByIdTask(inputMorsel: Morsel) extends InputLoopTask(inputMorsel) {

    override def toString: String = "NodeByIdTask"

    private var ids: java.util.Iterator[AnyValue] = _
    private var tracer: KernelReadTracer = _

    /**
     * Initialize the inner loop for the current input row.
     *
     * @return true iff the inner loop might result it output rows
     */
    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      val queryState = state.queryStateForExpressionEvaluation(resources)
      initExecutionContext.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
      ids = nodeIdsExpr.expressions(initExecutionContext, queryState).iterator()
      true
    }

    override def workIdentity: WorkIdentity = NodeByIdSeekOperator.this.workIdentity

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {

      while (outputRow.onValidRow && ids.hasNext) {
        val nextId = NumericHelper.asLongEntityIdPrimitive(ids.next())
        if (tracer != null) {
          tracer.onNode(nextId)
        }
        if (nextId >= 0L && state.queryContext.transactionalContext.dataRead.nodeExists(nextId)) {
          outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
          outputRow.setLongAt(offset, nextId)
          outputRow.next()
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

  def isValidNode(nodeId: IntermediateRepresentation): IntermediateRepresentation =
    and(greaterThanOrEqual(nodeId, constant(0L)),
      invoke(loadField(DATA_READ), method[Read, Boolean, Long]("nodeExists"),
        nodeId))
}

class SingleNodeByIdSeekTaskTemplate(inner: OperatorTaskTemplate,
                                     id: Id,
                                     innermost: DelegateOperatorTaskTemplate,
                                     nodeVarName: String,
                                     offset: Int,
                                     nodeIdExpr: Expression,
                                     argumentSize: SlotConfiguration.Size)
                                    (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {


  private val idVariable = variable[Long](codeGen.namer.nextVariableName(), constant(-1L))
  private var nodeId: IntermediateExpression = _

  override def genMoreFields: Seq[Field] = Seq.empty

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V, idVariable)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = noop()

  override def genExpressions: Seq[IntermediateExpression] = Seq(nodeId)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    if (nodeId == null) {
      nodeId = codeGen.compileExpression(nodeIdExpr).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $nodeIdExpr"))
    }

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
      setField(canContinue, isValidNode(load(idVariable.name))),
      onNode(loadField(executionEventField), load(idVariable)),
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
        innermost.setUnlessPastLimit(canContinue, constant(false)),
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


  private val idCursor = field[IteratorCursor](codeGen.namer.nextVariableName())
  private var nodeIds: IntermediateExpression= _

  override def genMoreFields: Seq[Field] = Seq(idCursor)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genExpressions: Seq[IntermediateExpression] = Seq(nodeIds)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = noop()

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    if (nodeIds == null) {
      nodeIds = codeGen.compileExpression(nodeIdsExpr).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $nodeIdsExpr"))
    }

    /**
     * {{{
     *   this.idCursor = IteratorCursor(((ListValue) <<nodeIdExpr>>)).iterator())
     *   this.canContinue = idCursor.next()
     *   true
     * }}}
     */
    block(
      setField(idCursor,
        invokeStatic(method[IteratorCursor, IteratorCursor, java.util.Iterator[_]]("apply"),
          invoke(cast[ListValue](nullCheckIfRequired(nodeIds)), method[ListValue, util.Iterator[AnyValue]]("iterator")))),
      setField(canContinue, cursorNext[IteratorCursor](loadField(idCursor))),
      constant(true))
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    val idVariable = codeGen.namer.nextVariableName()
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     val id = asId(idCursor.value()
     *     tracer.onNode(id)
     *     if (id >= 0 && read.nodeExists(id)) {
     *       setLongAt(offset, id)
     *       << inner.genOperate >>
     *     }
     *     this.canContinue = idCursor.next()
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        declareAndAssign(typeRefOf[Long], idVariable,
          invokeStatic(asIdMethod,
            invoke(loadField(idCursor),
              method[IteratorCursor, AnyValue]("value")))),
        onNode(loadField(executionEventField), load(idVariable)),
        condition(isValidNode(load(idVariable)))(
          block(
            codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
            codeGen.setLongAt(offset, load(idVariable)),
            inner.genOperateWithExpressions,
            doIfInnerCantContinue(profileRow(id))
          )),
        doIfInnerCantContinue(innermost.setUnlessPastLimit(canContinue, cursorNext[IteratorCursor](loadField(idCursor)))),
        endInnerLoop)
    )
  }

  //nothing to close
  override protected def genCloseInnerLoop: IntermediateRepresentation = noop()
}



