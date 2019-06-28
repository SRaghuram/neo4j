/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, Slot, SlotConfiguration, _}
import org.neo4j.cypher.internal.profiling.{OperatorProfileEvent, QueryProfiler}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.morsel.{ExecutionState, OperatorExpressionCompiler}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{DbAccess, QueryContext, ValuePopulation}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.{InternalException, symbols}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.internal.kernel.api.{IndexReadSession, KernelReadTracer}
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

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
  override def nextTasks(context: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    Array(new InputOTask(inputMorsel.nextCopy))

  class InputOTask(val inputMorsel: MorselExecutionContext) extends ContinuableOperatorTaskWithMorsel {

    override def workIdentity: WorkIdentity = ProduceResultOperator.this.workIdentity

    override def toString: String = "ProduceResultInputTask"
    override def canContinue: Boolean = inputMorsel.isValidRow

    override def operateWithProfile(outputIgnore: MorselExecutionContext,
                                    context: QueryContext,
                                    state: QueryState,
                                    resources: QueryResources,
                                    queryProfiler: QueryProfiler): Unit = {

      produceOutputWithProfile(inputMorsel, context, state, resources, queryProfiler)
    }

    override def operate(output: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = throw new UnsupportedOperationException("ProduceResults should be called via operateWithProfile")

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}

    override protected def closeCursors(resources: QueryResources): Unit = {}
  }

  //==========================================================================
  // This is called whe ProduceResult is the final operator of a pipeline

  class OutputOOperatorState extends OutputOperatorState with PreparedOutput {

    private var _canContinue: Boolean = false

    override def toString: String = "ProduceResultOutputTask"

    override def canContinue: Boolean = _canContinue

    override def prepareOutput(outputMorsel: MorselExecutionContext,
                               context: QueryContext,
                               state: QueryState,
                               resources: QueryResources,
                               queryProfiler: QueryProfiler): PreparedOutput = {

      produceOutputWithProfile(outputMorsel, context, state, resources, queryProfiler)
      _canContinue = outputMorsel.isValidRow
      this
    }

    override def produce(): Unit = {}
  }

  override def createState(executionState: ExecutionState, pipelineId: PipelineId): OutputOperatorState = new OutputOOperatorState

  //==========================================================================

  protected def produceOutputWithProfile(output: MorselExecutionContext,
                                         context: QueryContext,
                                         state: QueryState,
                                         resources: QueryResources,
                                         queryProfiler: QueryProfiler): Unit = {
    val operatorExecutionEvent = queryProfiler.executeOperator(workIdentity.workId)
    try {
      val rowBefore = output.getCurrentRow
      produceOutput(output, context, state, resources)
      operatorExecutionEvent.rows(output.getCurrentRow - rowBefore)
    } finally {
      operatorExecutionEvent.close()
    }
  }

  protected def produceOutput(output: MorselExecutionContext,
                              context: QueryContext,
                              state: QueryState,
                              resources: QueryResources): Unit = {
    //TODO this is not really needed since all we are doing in the expressions is accessing the ExecutionContext
    val queryState = new OldQueryState(context,
                                       resources = null,
                                       params = state.params,
                                       resources.expressionCursors,
                                       Array.empty[IndexReadSession],
                                       resources.expressionVariables(state.nExpressionSlots),
                                       state.subscriber,
                                       prePopulateResults = state.prepopulateResults)

    val subscriber: QuerySubscriber = state.subscriber
    var served = 0
    val demand: Long = state.flowControl.getDemand
    // Loop over the rows of the morsel and call the visitor for each one
    while (output.isValidRow && served < demand) {
      subscriber.onRecord()
      var i = 0
      while (i < expressions.length) {
        val value = expressions(i)(output, queryState)
        if (state.prepopulateResults) {
          ValuePopulation.populate(value)
        }
        subscriber.onField(i, value)
        i += 1
      }
      subscriber.onRecordCompleted()
      served += 1

      output.moveToNextRow()
    }
    state.flowControl.addServed(served)
  }
}

class ProduceResultOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                        override val id: Id,
                                        columns: Seq[String],
                                        slots: SlotConfiguration)
                                       (codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  import OperatorCodeGenHelperTemplates._
  import org.neo4j.codegen.api.IntermediateRepresentation._

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
      inner.genOperate)
  }

  override def genFields: Seq[Field] = inner.genFields

  override def genLocalVariables: Seq[LocalVariable] =
    inner.genLocalVariables ++ Seq(PRE_POPULATE_RESULTS_V, SUBSCRIBER, SUBSCRIPTION, DEMAND, SERVED)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors
}

class CompiledQueryResultRecord(override val fields: Array[AnyValue]) extends QueryResult.Record
