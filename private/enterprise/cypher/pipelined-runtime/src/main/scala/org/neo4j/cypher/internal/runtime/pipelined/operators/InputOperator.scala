/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.isNull
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.labeledLoop
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.InputCursor
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselCypherRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.operators.InputOperator.nodeOrNoValue
import org.neo4j.cypher.internal.runtime.pipelined.operators.InputOperator.relationshipOrNoValue
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTER_LOOP_LABEL_NAME
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

class InputOperator(val workIdentity: WorkIdentity,
                    nodeOffsets: Array[Int],
                    relationshipOffsets: Array[Int],
                    refOffsets: Array[Int]) extends StreamingOperator {

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    if (parallelism == 1)
      IndexedSeq(new InputTask(new MutatingInputCursor(state.input), inputMorsel.nextCopy))
    else
      new Array[InputTask](parallelism).map(_ => new InputTask(new MutatingInputCursor(state.input), inputMorsel.nextCopy))
  }

  /**
   * A [[InputTask]] reserves new batches from the InputStream, until there are no more batches.
   */
  class InputTask(input: MutatingInputCursor, val inputMorsel: MorselCypherRow) extends ContinuableOperatorTaskWithMorsel {

    override def workIdentity: WorkIdentity = InputOperator.this.workIdentity

    override def operate(outputRow: MorselCypherRow,
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
        while (i < relationshipOffsets.length) {
          outputRow.setLongAt(relationshipOffsets(i), relationshipOrNoValue(input.value(i)))
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

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}

    override protected def closeCursors(resources: QueryResources): Unit = input.close()
  }
}

object InputOperator {
  def nodeOrNoValue(value: AnyValue): Long =
    if (value eq Values.NO_VALUE) NullChecker.NULL_ENTITY
    else value.asInstanceOf[VirtualNodeValue].id()

  def relationshipOrNoValue(value: AnyValue): Long =
    if (value eq Values.NO_VALUE) NullChecker.NULL_ENTITY
    else value.asInstanceOf[VirtualRelationshipValue].id()
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

  def close(): Unit =
    if (cursor != null) {
      cursor.close()
      cursor = null
    }
}

class InputOperatorTemplate(override val inner: OperatorTaskTemplate,
                            override val id: Id,
                            innermost: DelegateOperatorTaskTemplate,
                            nodeOffsets: Array[Int],
                            relationshipOffsets: Array[Int],
                            refOffsets: Array[Int],
                            nullable: Boolean,
                            final override protected val isHead: Boolean = true)
                           (protected val codeGen: OperatorExpressionCompiler) extends ContinuableOperatorTaskWithMorselTemplate {

  private val inputCursorField = field[MutatingInputCursor](codeGen.namer.nextVariableName())
  private val canContinue = field[Boolean](codeGen.namer.nextVariableName())

  override protected def scopeId: String = "input" + id.x

  override def genCanContinue: Option[IntermediateRepresentation] =
    inner.genCanContinue.map(or(_, loadField(canContinue))).orElse(Some(loadField(canContinue)))

  override def genCloseCursors: IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(inputCursorField)))(
        invoke(loadField(inputCursorField), method[MutatingInputCursor, Unit]("close"))
      ),
      inner.genCloseCursors
    )

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
  final override protected def genOperateHead: IntermediateRepresentation = {

    val setNodes = nodeOffsets.zipWithIndex.map {
      case (nodeOffset, i) =>
        codeGen.setLongAt(nodeOffset, invokeStatic(method[InputOperator, Long, AnyValue]("nodeOrNoValue"),
          invoke(loadField(inputCursorField), method[MutatingInputCursor, AnyValue, Int]("value"), constant(i))))
    }
    val setRelationships = relationshipOffsets.zipWithIndex.map {
      case (relationshipOffset, i) =>
        codeGen.setLongAt(relationshipOffset, invokeStatic(method[InputOperator, Long, AnyValue]("relationshipOrNoValue"),
          invoke(loadField(inputCursorField), method[MutatingInputCursor, AnyValue, Int]("value"), constant(i))))
    }
    val setRefs = refOffsets.zipWithIndex.map {
      case (refOffset, i) =>
        codeGen.setRefAt(refOffset, invoke(loadField(inputCursorField), method[MutatingInputCursor, AnyValue, Int]("value"), constant(i)))
    }
    val setters = block(setNodes ++ setRelationships ++ setRefs:_*)
    block(
      condition(isNull(loadField(inputCursorField)))(
        setField(inputCursorField, newInstance(constructor[MutatingInputCursor, InputDataStream],
          invoke(QUERY_STATE,
            method[QueryState, InputDataStream]("input"))))),
      condition(not(loadField(canContinue)))(
        block(
          setField(canContinue, invoke(loadField(inputCursorField), method[MutatingInputCursor, Boolean]("nextInput"))),
          profileRow(id, loadField(canContinue)))),
      labeledLoop(OUTER_LOOP_LABEL_NAME, and(innermost.predicate, loadField(canContinue)))(
        block(
          setters,
          inner.genOperateWithExpressions,
          doIfInnerCantContinue(
            block(
              setField(canContinue, invoke(loadField(inputCursorField), method[MutatingInputCursor, Boolean]("nextInput"))),
              profileRow(id, loadField(canContinue))
            )),
          innermost.resetCachedPropertyVariables
        )
      )
    )
  }

  override protected def genOperateMiddle: IntermediateRepresentation = {
    throw new CantCompileQueryException("Cannot compile Input as middle operator")
  }

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genFields: Seq[Field] = Seq(inputCursorField, canContinue)

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}
