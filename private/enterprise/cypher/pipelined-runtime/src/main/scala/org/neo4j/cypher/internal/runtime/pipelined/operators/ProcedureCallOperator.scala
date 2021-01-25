/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.staticConstant
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.Method
import org.neo4j.codegen.api.StaticField
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.ProcedureCallMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.dbHits
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.ProcedureCallOperator.createProcedureCallContext
import org.neo4j.cypher.internal.runtime.pipelined.procedures.Procedures
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.values.AnyValue

class ProcedureCallOperator(val workIdentity: WorkIdentity,
                            signature: ProcedureSignature,
                            callMode: ProcedureCallMode,
                            argExprs: Seq[Expression],
                            originalVariables: Array[String],
                            indexToOffset: Array[(Int, Int)])
  extends StreamingOperator with ListSupport {

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    singletonIndexedSeq(new ProcedureCallTask(inputMorsel.nextCopy, workIdentity, signature, callMode, argExprs, originalVariables, indexToOffset))
  }
}

object ProcedureCallOperator {
  def createProcedureCallContext(id: Int, qtx: QueryContext, originalVariables: Array[String]): ProcedureCallContext = {
    // getting the original name of the yielded variable
    val databaseId = qtx.transactionalContext.databaseId
    new ProcedureCallContext(id, originalVariables, true, databaseId.name(), databaseId.isSystemDatabase)
  }

  val EMPTY_ARGS: Array[AnyValue] = Array.empty
}

class ProcedureCallMiddleOperator(val workIdentity: WorkIdentity,
                                  signature: ProcedureSignature,
                                  callMode: ProcedureCallMode,
                                  argExprs: Seq[Expression],
                                  originalVariables: Array[String]) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)
    val readCursor = morsel.readCursor()
    while (readCursor.next()) {
      val argValues = argExprs.map(arg => arg(readCursor, queryState)).toArray
      callMode.callProcedure(state.query, signature.id, argValues, createProcedureCallContext(signature.id, state.query, originalVariables))
    }
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = if (event != null) {
    //this marks the number of db hits as uncertain
    event.dbHits(OperatorProfile.NO_DATA)
  }
}

class ProcedureCallTask(inputMorsel: Morsel,
                        val workIdentity: WorkIdentity,
                        signature: ProcedureSignature,
                        callMode: ProcedureCallMode,
                        argExprs: Seq[Expression],
                        originalVariables: Array[String],
                        indexToOffset: Array[(Int, Int)]) extends InputLoopTask(inputMorsel) {
  protected var iterator: Iterator[Array[AnyValue]] = _

  override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {

    val queryState = state.queryStateForExpressionEvaluation(resources)
    initExecutionContext.copyFrom(inputCursor, inputMorsel.longsPerRow, inputMorsel.refsPerRow)
    val argValues = argExprs.map(arg => arg(initExecutionContext, queryState)).toArray
    iterator = callMode.callProcedure(state.query, signature.id, argValues, createProcedureCallContext(signature.id, state.query, originalVariables))
    true
  }

  override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
    while (outputRow.onValidRow() && iterator.hasNext) {
      val thisValue = iterator.next()
      outputRow.copyFrom(inputCursor)
      var i = 0
      while (i < indexToOffset.length) {
        val (index, offset) = indexToOffset(i)
        outputRow.setRefAt(offset, thisValue(index))
        i += 1
      }
      outputRow.next()
    }
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
    if (event != null) {
      //this marks the number of db hits as uncertain
      event.dbHits(OperatorProfile.NO_DATA)
    }
  }

  override protected def closeInnerLoop(resources: QueryResources): Unit = {
    iterator = null
  }

  override def canContinue: Boolean = iterator != null || inputCursor.onValidRow()
}

trait ProcedureCaller {
  self => OperatorTaskTemplate

  /**
   * Compiles input args to ir and also handles default arguments by adding static constant fields for all default values.
   */
  def createArgsWithDefaultValue(args: Seq[expressions.Expression],
                                 signature: ProcedureSignature,
                                 codeGen: OperatorExpressionCompiler,
                                 id: Id): Seq[() => IntermediateExpression] = args.map(Some(_)).zipAll(signature.inputSignature.map(_.default), None, None).map {
    case (Some(arg), _) => () =>
      val v = compile(arg, codeGen, id)
      v.copy(ir = nullCheckIfRequired(v))
    case (_, Some(defaultValue)) => () =>
      val defaultValueField = staticConstant[AnyValue](codeGen.namer.nextVariableName(), defaultValue)
      IntermediateExpression(getStatic(defaultValueField),Seq(defaultValueField), Seq.empty, Set.empty, requireNullCheck = false)
    case _ => throw new IllegalStateException("should have been caught by semantic checking")
  }

  def callProcedureWithSideEffect(callModeField: StaticField,
                                  signature: ProcedureSignature,
                                  originalVariablesField: StaticField,
                                  argExpression: Array[IntermediateExpression]): IntermediateRepresentation =
    callProcedure(callModeField, signature, originalVariablesField, argExpression, isSideEffect = true)

  def callProcedure(callModeField: StaticField,
                    signature: ProcedureSignature,
                    originalVariablesField: StaticField,
                    argExpression: Array[IntermediateExpression],
                    isSideEffect: Boolean = false): IntermediateRepresentation = {
    val invoker: (IntermediateRepresentation, Method, Seq[IntermediateRepresentation]) => IntermediateRepresentation =
      if (isSideEffect) invokeSideEffect else invoke
    invoker(
      getStatic(callModeField),
      method[ProcedureCallMode, Iterator[Array[AnyValue]], QueryContext, Int, Array[AnyValue], ProcedureCallContext]("callProcedure"),
      Seq(DB_ACCESS,
        constant(signature.id),
        getProcedureArguments(argExpression),
        invokeStatic(method[ProcedureCallOperator, ProcedureCallContext, Int, QueryContext, Array[String]]("createProcedureCallContext"),
          constant(signature.id),
          DB_ACCESS,
          getStatic(originalVariablesField))))
  }

  private def getProcedureArguments(argExpression: Array[IntermediateExpression]): IntermediateRepresentation = {

    if (argExpression.isEmpty) {
      getStatic[Procedures, Array[AnyValue]]("EMPTY_ARGS")
    } else {
      arrayOf[AnyValue](argExpression.map(_.ir): _*)
    }
  }

  private def compile(e: expressions.Expression,
                      codeGen: OperatorExpressionCompiler,
                      id: Id): IntermediateExpression =
    codeGen.compileExpression(e, id).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $e"))
}

/**
 * Special case template for void procedures, using a void procedure is just adding a call to the procedure
 * without affecting the cardinality of the results.
 */
class VoidProcedureOperatorTemplate(val inner: OperatorTaskTemplate,
                                    override val id: Id,
                                    args: Seq[expressions.Expression],
                                    signature: ProcedureSignature,
                                    callMode: ProcedureCallMode,
                                    originalVariables: Array[String])
                                   (protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate with ProcedureCaller {
  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }
  private val callArgumentsGenerator = createArgsWithDefaultValue(args, signature, codeGen, id)
  private var argExpression: Array[IntermediateExpression] = _
  private val callModeField: StaticField = staticConstant[ProcedureCallMode](codeGen.namer.nextVariableName("callMode"), callMode)
  private val originalVariablesField: StaticField = staticConstant[Array[String]](codeGen.namer.nextVariableName("vars"), originalVariables)

  override def genExpressions: Seq[IntermediateExpression] = argExpression

  override def genOperate: IntermediateRepresentation = {
    if (argExpression == null) {
      argExpression = callArgumentsGenerator.map(_ ()).toArray
    }
    /**
     * {{{
     *   [callProcedure]
     *   <inner>
     * }}}
     */
    block(
      callProcedureWithSideEffect(callModeField, signature, originalVariablesField, argExpression),
      profileRow(id, doProfile),
      inner.genOperateWithExpressions
    )
  }

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genFields: Seq[Field] = Seq(callModeField, originalVariablesField)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      //this marks the number of db hits as uncertain
      dbHits(event, constant(-1L) ),
      inner.genSetExecutionEvent(event)
    )
  }

  override protected def isHead: Boolean = false
}

class ProcedureOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                    id: Id,
                                    innermost: DelegateOperatorTaskTemplate,
                                    isHead: Boolean,
                                    args: Seq[expressions.Expression],
                                    signature: ProcedureSignature,
                                    callMode: ProcedureCallMode,
                                    originalVariables: Array[String],
                                    indexToOffset: Array[(Int, Int)])
                                   (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen, isHead) with ProcedureCaller {

  private val iteratorField = field[Iterator[Array[AnyValue]]](codeGen.namer.nextVariableName("iterator"))
  private val callModeField = staticConstant[ProcedureCallMode](codeGen.namer.nextVariableName("callMode"), callMode)
  private val originalVariablesField = staticConstant[Array[String]](codeGen.namer.nextVariableName("vars"), originalVariables)
  private var argExpression: Array[IntermediateExpression] = _
  private val nextVar = field[Array[AnyValue]](codeGen.namer.nextVariableName("next"))
  private val callArgumentsGenerator = createArgsWithDefaultValue(args, signature, codeGen, id)

  override final def scopeId: String = "procedureCall" + id.x

  override def genMoreFields: Seq[Field] = Seq(iteratorField, callModeField, originalVariablesField, nextVar)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      //this marks the number of db hits as uncertain
      dbHits(event, constant(-1L) ),
      inner.genSetExecutionEvent(event)
    )
  }

  override def genExpressions: Seq[IntermediateExpression] = argExpression

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    if (argExpression == null) {
      argExpression = callArgumentsGenerator.map(_ ()).toArray
    }

    /**
     * {{{
     *   this.iterator = [callProcedure]
     *   this.canContinue = this.iterator.hasNext()
     *   if (this.canContinue) {
     *    this.next = this.iterator.next()
     *    profile(...)
     *   }
     *   true
     * }}}
     */
    block(
      setField(iteratorField, callProcedure(callModeField, signature, originalVariablesField, argExpression)),
      setField(canContinue, invoke(loadField(iteratorField), method[Iterator[_], Boolean]("hasNext"))),
      condition(loadField(canContinue)) {
        block(
          profileRow(id, doProfile),
          setField(nextVar, cast[Array[AnyValue]](invoke(loadField(iteratorField), method[Iterator[_], Object]("next"))))
        )
      },
      constant(true)
    )
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     setRefAt(offset1, nextVar(0))
     *     setRefAt(offset2, nextVar(1))
     *     ....
     *     << inner.genOperate >>
     *     this.canContinue = this.iterator.hasNext()
     *     if (this.canContinue) {
     *      this.next = this.iterator.next()
     *      profile(...)
     *     }
     *   }
     * }}}
     */
    block(
      loop(and(innermost.predicate, loadField(canContinue)))(
        block(
          codeGen.copyFromInput(Math.min(codeGen.inputSlotConfiguration.numberOfLongs, codeGen.slots.numberOfLongs),
            Math.min(codeGen.inputSlotConfiguration.numberOfReferences,
              codeGen.slots.numberOfReferences)),
          block(indexToOffset.map {
            case (index, offset) => codeGen.setRefAt(offset, arrayLoad(loadField(nextVar), index))
          }: _*),
          inner.genOperateWithExpressions,
          doIfInnerCantContinue(
            block(
              innermost.setUnlessPastLimit(canContinue, invoke(loadField(iteratorField), method[Iterator[_], Boolean]("hasNext"))),
              condition(loadField(canContinue)) {
                block(
                  profileRow(id, doProfile),
                  setField(nextVar, cast[Array[AnyValue]](invoke(loadField(iteratorField), method[Iterator[_], Object]("next"))))
                )
              }),
          )
        )
      )
    )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    noop()
  }
}




