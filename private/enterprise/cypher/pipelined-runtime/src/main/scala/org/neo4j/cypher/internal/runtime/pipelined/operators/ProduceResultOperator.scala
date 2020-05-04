/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.add
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.ValuePopulation
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DEMAND
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.PRE_POPULATE_RESULTS
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.PRE_POPULATE_RESULTS_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.SERVED
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.SUBSCRIBER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.SUBSCRIPTION
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.result.QueryResult
import org.neo4j.exceptions.InternalException
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

/**
 * This operator implements both [[StreamingOperator]] and [[OutputOperator]] because it
 * can occur both as the start of a pipeline, and as the final operator of a pipeline.
 */
class ProduceResultOperator(val workIdentity: WorkIdentity,
                            slots: SlotConfiguration,
                            columns: Seq[(String, Expression)])
  extends StreamingOperator
  with OutputOperator {

  private val expressions: Array[Expression] = columns.map(_._2).toArray

  override def toString: String = "ProduceResult"
  override def outputBuffer: Option[BufferId] = None

  //==========================================================================
  // This is called when ProduceResult is the start operator of a new pipeline
  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    Array(new InputOTask(inputMorsel.nextCopy))

  class InputOTask(val inputMorsel: Morsel) extends ContinuableOperatorTaskWithMorsel {

    private val inputCursor = inputMorsel.readCursor(onFirstRow = true)

    override def workIdentity: WorkIdentity = ProduceResultOperator.this.workIdentity

    override def toString: String = "ProduceResultInputTask"
    override def canContinue: Boolean = inputCursor.onValidRow()

    override def operateWithProfile(outputIgnore: Morsel,
                                    state: PipelinedQueryState,
                                    resources: QueryResources,
                                    queryProfiler: QueryProfiler): Unit = {

      val operatorExecutionEvent = queryProfiler.executeOperator(workIdentity.workId)

      try {
        produceOutputWithProfile(inputCursor, state, resources, operatorExecutionEvent)
      } finally {
        if (operatorExecutionEvent != null) {
          operatorExecutionEvent.close()
        }
      }
    }

    override def operate(output: Morsel,
                         state: PipelinedQueryState,
                         resources: QueryResources): Unit = throw new UnsupportedOperationException("ProduceResults should be called via operateWithProfile")

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}

    override protected def closeCursors(resources: QueryResources): Unit = {}
  }

  //==========================================================================
  // This is called when ProduceResult is the final operator of a pipeline

  class OutputOOperatorState extends OutputOperatorState with PreparedOutput {

    private var doneWithPreviousMorsel: Boolean = true
    private var outputCursor: MorselReadCursor = _

    override def toString: String = "ProduceResultOutputTask"

    override def canContinueOutput: Boolean = !doneWithPreviousMorsel

    override def workIdentity: WorkIdentity = ProduceResultOperator.this.workIdentity

    override def prepareOutput(outputMorsel: Morsel,
                               state: PipelinedQueryState,
                               resources: QueryResources,
                               operatorExecutionEvent: OperatorProfileEvent): PreparedOutput = {

      if (doneWithPreviousMorsel) {
        outputCursor = outputMorsel.readCursor(onFirstRow = true)
      }

      produceOutputWithProfile(outputCursor, state, resources, operatorExecutionEvent)
      doneWithPreviousMorsel = !outputCursor.onValidRow()
      this
    }

    override def produce(resources: QueryResources): Unit = {}

    override def trackTime: Boolean = true
  }

  override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState = new OutputOOperatorState

  //==========================================================================

  protected def produceOutputWithProfile(output: MorselReadCursor,
                                         state: PipelinedQueryState,
                                         resources: QueryResources,
                                         operatorExecutionEvent: OperatorProfileEvent): Unit = {
    val numberOfOutputedRows = produceOutput(output, state, resources)
    if (operatorExecutionEvent != null) {
      operatorExecutionEvent.rows(numberOfOutputedRows)
    }
  }

  protected def produceOutput(output: MorselReadCursor,
                              state: PipelinedQueryState,
                              resources: QueryResources): Int = {
    val subscriber: QuerySubscriber = state.subscriber
    var served = 0
    val demand: Long = state.flowControl.getDemandUnlessCancelled
    // Loop over the rows of the morsel and call the visitor for each one
    while (output.onValidRow && served < demand) {
      subscriber.onRecord()
      var i = 0
      while (i < expressions.length) {
        val value = expressions(i)(output, state)
        if (state.prePopulateResults) {
          ValuePopulation.populate(value)
        }
        subscriber.onField(i, value)
        i += 1
      }
      subscriber.onRecordCompleted()
      served += 1

      output.next()
    }
    state.flowControl.addServed(served)
    served
  }
}

class ProduceResultOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                        override val id: Id,
                                        columns: Seq[String],
                                        slots: SlotConfiguration)
                                       (protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  override def toString: String = "ProduceResultTaskTemplate"

  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  // This operates on a single row only
  override def genOperate: IntermediateRepresentation = {
    def getLongAt(offset: Int) = codeGen.getLongAt(offset)
    def getRefAt(offset: Int) = {
      val notPopulated = codeGen.getRefAt(offset)
      ternary(PRE_POPULATE_RESULTS,
        invokeStatic(method[ValuePopulation, AnyValue, AnyValue]("populate"),
          notPopulated), notPopulated)
    }
    def nodeFromSlot(offset: Int) = {
      val notPopulated = invoke(DB_ACCESS, method[DbAccess, NodeValue, Long]("nodeById"), getLongAt(offset))
      ternary(PRE_POPULATE_RESULTS,
        invokeStatic(method[ValuePopulation, NodeValue, NodeValue]("populate"),
          notPopulated), notPopulated)
    }
    def relFromSlot(offset: Int) = {
      val notPopulated = invoke(DB_ACCESS, method[DbAccess, RelationshipValue, Long]("relationshipById"), getLongAt(offset))
      ternary(PRE_POPULATE_RESULTS,
        invokeStatic(method[ValuePopulation, RelationshipValue, RelationshipValue]("populate"),
          notPopulated), notPopulated)
    }

    //figures out how to get a reference to project from a slot, e.g if we have a longSlot that is a node,
    // we create a node, and if it is a relationship we create a relationship and so on
    def getFromSlot(slot: Slot) = slot match {
      case LongSlot(offset, true, symbols.CTNode) =>
        ternary(equal(getLongAt(offset), constant(-1L)), noValue, nodeFromSlot(offset))
      case LongSlot(offset, false, symbols.CTNode) =>
        nodeFromSlot(offset)
      case LongSlot(offset, true, symbols.CTRelationship) =>
        ternary(equal(getLongAt(offset), constant(-1L)), noValue, relFromSlot(offset))
      case LongSlot(offset, false, symbols.CTRelationship) =>
        relFromSlot(offset)
      case RefSlot(offset, _, _) =>
        getRefAt(offset)

      case _ =>
        throw new InternalException(s"Do not know how to project $slot")
    }

    /**
     * For each column to project we generate
     * {{{
     *   subscriber.onField(0, getFromSlot)
     *   subscriber.onField(1, getFromSlot)
     *   ....
     * }}}
     */
    val project = block(columns.zipWithIndex.map {
      case (name, index) =>
        val slot = slots.get(name).getOrElse(
          throw new InternalException(s"Did not find `$name` in the slot configuration")
        )
        invokeSideEffect(load(SUBSCRIBER), method[QuerySubscriber, Unit, Int, AnyValue]("onField"),
          constant(index), getFromSlot(slot))
    }:_ *)

    /**
     * Generates:
     * {{{
     *   subscriber.onRecord()
     *   [[project]]
     *   subscriber.onRecordCompleted()
     *   served += 1L
     *   [[inner]]]
     * }}}
     */
    block(
      invokeSideEffect(load(SUBSCRIBER), method[QuerySubscriber, Unit]("onRecord")),
      project,
      invokeSideEffect(load(SUBSCRIBER), method[QuerySubscriber, Unit]("onRecordCompleted")),
      assign(SERVED, add(load(SERVED), constant(1L))),
      profileRow(id),
      inner.genOperateWithExpressions
    )
  }

  override def genFields: Seq[Field] = Seq.empty

  override def genLocalVariables: Seq[LocalVariable] =
    Seq(PRE_POPULATE_RESULTS_V, SUBSCRIBER, SUBSCRIPTION, DEMAND, SERVED)

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}

class CompiledQueryResultRecord(override val fields: Array[AnyValue]) extends QueryResult.Record
