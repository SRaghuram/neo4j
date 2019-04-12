/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.codegen.api.{Field, InstanceField, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.{WorkIdentity, WorkUnitEvent}
import org.neo4j.cypher.internal.runtime.slotted.{ArrayResultExecutionContextFactory, SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.zombie.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.{DbAccess, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.{InternalException, symbols}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

/**
  * This operator implements both [[StreamingOperator]] and [[ContinuableOperator]] because it
  * can occur both as the start of a pipeline, and as the final operator of a pipeline.
  *
  * @param workIdentity
  * @param slots
  * @param columns
  */
class ProduceResultOperator(val workIdentity: WorkIdentity,
                            slots: SlotConfiguration,
                            columns: Seq[(String, Expression)]) extends StreamingOperator with ContinuableOperator {

  override def toString: String = "ProduceResult"

  //==========================================================================
  // This is called when ProduceResult is the start operator of a new pipeline
  override def nextTasks(context: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    Array(new InputOTask(inputMorsel.nextCopy))

  class InputOTask(val inputMorsel: MorselExecutionContext) extends OTask() with ContinuableOperatorTaskWithMorsel {

    override def canContinue: Boolean = false // will be true sometimes for reactive results

    override def operate(outputIgnore: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {
      produceOutput(inputMorsel, context, state, resources)
    }
  }

  //==========================================================================
  // This is called whe ProduceResult is the final operator of a pipeline
  override def init(context: QueryContext,
                    state: QueryState,
                    resources: QueryResources): ContinuableOperatorTask =
    new OutputOTask()

  class OutputOTask() extends OTask() {

    override def canContinue: Boolean = false // will be true sometimes for reactive results
    override def close(operatorCloser: OperatorCloser): Unit = {}
    override def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = false
    override def producingWorkUnitEvent: WorkUnitEvent = null

    override def operate(output: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {
      produceOutput(output, context, state, resources)
    }
  }

  //==========================================================================
  abstract class OTask() extends ContinuableOperatorTask {

    override def toString: String = "ProduceResultTask"

    override def canContinue: Boolean = false // will be true sometimes for reactive results

    protected def produceOutput(output: MorselExecutionContext,
                                context: QueryContext,
                                state: QueryState,
                                resources: QueryResources): Unit = {
      val resultFactory = ArrayResultExecutionContextFactory(columns)
      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession],
                                         resources.expressionVariables(state.nExpressionSlots))

      // Loop over the rows of the morsel and call the visitor for each one
      while (output.isValidRow) {
        val arrayRow = resultFactory.newResult(output, queryState, queryState.prePopulateResults)
        state.visitor.visit(arrayRow)
        output.moveToNextRow()
      }
    }
  }
}

class ProduceResultOperatorTaskTemplate(val inner: OperatorTaskTemplate, columns: Seq[String], slots: SlotConfiguration)
                                       (codeGen: OperatorExpressionCompiler) extends ContinuableOperatorTaskTemplate {
  import org.neo4j.codegen.api.IntermediateRepresentation._

  override def toString: String = "ProduceResultTaskTemplate"

  override def genInit: IntermediateRepresentation = {
    block(
      setField(RESULT_FIELD_ARRAY, newArray(typeRefOf[AnyValue], columns.length)),
      setField(RESULT_RECORD, newInstance(constructor[CompiledQueryResultRecord, Array[AnyValue]], loadField(RESULT_FIELD_ARRAY))),
      inner.genInit
    )
  }

  override def genCanContinue: IntermediateRepresentation = {
    constant(false) // will be true sometimes for reactive results
  }

  import OperatorCodeGenHelperTemplates._

  // This operates on a single row only
  override def genOperate: IntermediateRepresentation = {
    def getLongAt(offset: Int) = codeGen.getLongAt(offset)
    def getRefAt(offset: Int) = codeGen.getRefAt(offset)

    val nodeFromSlot = (offset: Int) =>
      invoke(DB_ACCESS, method[DbAccess, NodeValue, Long]("nodeById"), getLongAt(offset))
    val relFromSlot = (offset: Int) =>
      invoke(DB_ACCESS, method[DbAccess, RelationshipValue, Long]("relationshipById"), getLongAt(offset))

    def getFromSlot(slot: Slot) = slot match {
      case LongSlot(offset, true, symbols.CTNode) =>
        ternary(equal(getLongAt(offset), constant(-1)), noValue, nodeFromSlot(offset))
      case LongSlot(offset, false, symbols.CTNode) =>
        nodeFromSlot(offset)
      case LongSlot(offset, true, symbols.CTRelationship) =>
        ternary(equal(getLongAt(offset), constant(-1)), noValue, relFromSlot(offset))
      case LongSlot(offset, false,  symbols.CTRelationship) =>
        relFromSlot(offset)

      case RefSlot(offset, _, _) => getRefAt(offset)
      case _ =>
        throw new InternalException(s"Do not know how to project $slot")
    }

    val project = block(columns.zipWithIndex.map {
      case (name, index) =>
        val slot = slots.get(name).getOrElse(
          throw new InternalException(s"Did not find `$name` in the slot configuration")
        )
        arraySet(loadField(RESULT_FIELD_ARRAY), index, getFromSlot(slot))
    }:_ *)

    block(
      project,
      // We should actually check the return value and exit the loop if the visitor returns false to fulfill the contract, but morsel runtime doesn't do that currently
      invokeSideEffect(load("resultVisitor"), method[QueryResultVisitor[Exception], Boolean, QueryResult.Record]("visit"), loadField(RESULT_RECORD)),
      inner.genOperate
    )
  }

  override def genFields: Seq[Field] =
    RESULT_FIELD_ARRAY +: (RESULT_RECORD +: inner.genFields)

  override def genLocalVariables: Seq[LocalVariable] =
    inner.genLocalVariables

  val RESULT_FIELD_ARRAY: InstanceField = field[Array[AnyValue]]("fields")
  val RESULT_RECORD: InstanceField = field[CompiledQueryResultRecord]("resultRecord")
}

class CompiledQueryResultRecord(override val fields: Array[AnyValue]) extends QueryResult.Record