/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.InputOperator.nodeOrNoValue
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.{InputCursor, InputDataStream, QueryContext}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue


class InputOperator(val workIdentity: WorkIdentity,
                    nodeOffsets: Array[Int],
                    refOffsets: Array[Int]) extends StreamingOperator {

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    if (state.singeThreaded)
      IndexedSeq(new InputTask(new MutatingInputCursor(state.input), inputMorsel.nextCopy))
    else
      new Array[InputTask](state.numberOfWorkers).map(_ => new InputTask(new MutatingInputCursor(state.input), inputMorsel.nextCopy))
  }

  /**
    * A [[InputTask]] reserves new batches from the InputStream, until there are no more batches.
    */
  class InputTask(input: MutatingInputCursor, val inputMorsel: MorselExecutionContext) extends ContinuableOperatorTaskWithMorsel {

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         queryState: QueryState,
                         resources: QueryResources): Unit = {

      while (outputRow.isValidRow && input.nextInput()) {
        var i = 0
        while (i < nodeOffsets.length) {
          outputRow.setLongAt(nodeOffsets(i), nodeOrNoValue(input.value(i)))
          i += 1
        }
        i = 0
        while (i < refOffsets.length) {
          outputRow.setRefAt(refOffsets(i), input.value(i))
          i += 1
        }
        outputRow.moveToNextRow()
      }

      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = input.canContinue
  }
}

object InputOperator {
  def nodeOrNoValue(value: AnyValue): Long =
    if (value == Values.NO_VALUE) NullChecker.NULL_ENTITY
    else value.asInstanceOf[VirtualNodeValue].id()
}

class MutatingInputCursor(input: InputDataStream) {
  private var _canContinue = true
  private var cursor: InputCursor = _

  def canContinue: Boolean = _canContinue
  def value(offset: Int): AnyValue = cursor.value(offset)
  def nextInput(): Boolean = {
    while (true) {
      if (cursor == null) {
        cursor = input.nextInputBatch()
        if (cursor == null) {
          // We ran out of work
          _canContinue = false
          return false
        }
      }
      if (cursor.next()) {
        return true
      } else {
        cursor.close()
        cursor = null
      }
    }

    throw new IllegalStateException("Unreachable code")
  }
}

class InputOperatorTemplate(inner: OperatorTaskTemplate,
                            innermost: DelegateOperatorTaskTemplate,
                            nodeOffsets: Array[Int],
                            refOffsets: Array[Int],
                            nullable: Boolean)(codeGen: OperatorExpressionCompiler) extends ContinuableOperatorTaskWithMorselTemplate {
  import IntermediateRepresentation._
  import OperatorCodeGenHelperTemplates._

  private val inputCursorField = field[MutatingInputCursor](codeGen.namer.nextVariableName())
  private val canContinue = field[Boolean](codeGen.namer.nextVariableName())

  override def genCanContinue: Option[IntermediateRepresentation] =
    inner.genCanContinue.map(or(_, loadField(canContinue))).orElse(Some(loadField(canContinue)))

  /**
    * {{{
    *
    *    if (!this.canContinue) {
    *       this.canContinue = input.nextInput();
    *    }
    *    while (hasDemand && this.canContinue) {
    *      outputRow.setLongAt(nodeOffsets(0), nodeOrNoValue(cursor.value(0));
    *      outputRow.setLongAt(nodeOffsets(1), nodeOrNoValue(cursor.value(1));
    *      ...
    *      outputRow.setRefAt(refOffset(10), nodeOrNoValue(cursor.value(10));
    *      outputRow.setRefAt(refOffsets(11), cursor.value(11);
    *      ...
    *      [[inner]]
    *      this.canContinue = input.nextInput()
    *    }
    *    outputRow.finishedWriting()
    * }}}
    */
  override def genOperate: IntermediateRepresentation = {

    val setNodes = nodeOffsets.zipWithIndex.map {
      case (nodeOffset, i) =>
        codeGen.setLongAt(nodeOffset, invokeStatic(method[InputOperator, Long, AnyValue]("nodeOrNoValue"),
                                                   invoke(loadField(inputCursorField), method[MutatingInputCursor, AnyValue, Int]("value"), constant(i))))
    }
    val setRefs = refOffsets.zipWithIndex.map {
      case (refOffset, i) =>
        codeGen.setRefAt(refOffset, invoke(loadField(inputCursorField), method[MutatingInputCursor, AnyValue, Int]("value"), constant(i)))
    }
    val setters = block(setNodes ++ setRefs:_*)
    block(
      condition(isNull(loadField(inputCursorField)))(
        setField(inputCursorField, newInstance(constructor[MutatingInputCursor, InputDataStream],
                                               invoke(QUERY_STATE,
                                                      method[QueryState, InputDataStream]("input"))))),
      condition(not(loadField(canContinue)))(setField(canContinue, invoke(loadField(inputCursorField), method[MutatingInputCursor, Boolean]("nextInput")))),
      loop(and(innermost.predicate, loadField(canContinue)))(
        block(
          setters,
          inner.genOperate,
          setField(canContinue, invoke(loadField(inputCursorField), method[MutatingInputCursor, Boolean]("nextInput")))
          )
        ),
      innermost.onExit)
  }

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genFields: Seq[Field] = inputCursorField +: canContinue +: inner.genFields

  override def genLocalVariables: Seq[LocalVariable] = inner.genLocalVariables
}
