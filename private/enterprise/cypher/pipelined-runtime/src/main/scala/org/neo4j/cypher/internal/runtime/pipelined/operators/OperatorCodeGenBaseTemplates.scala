/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.codegen.TypeReference
import org.neo4j.codegen.api.ClassDeclaration
import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.codegen.api.CodeGeneration.CodeGenerationMode
import org.neo4j.codegen.api.CodeGeneration.compileClass
import org.neo4j.codegen.api.Constructor
import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.param
import org.neo4j.codegen.api.IntermediateRepresentation.self
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.staticConstant
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.MethodDeclaration
import org.neo4j.codegen.api.StaticField
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ_CONSTRUCTOR_PARAMETER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.HAS_DEMAND
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.HAS_REMAINING_OUTPUT
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_CURSOR_FIELD
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_MORSEL_CONSTRUCTOR_PARAMETER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NEXT
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NO_KERNEL_TRACER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NO_OPERATOR_PROFILE_EVENT
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_COUNTER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_CURSOR_VAR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_ROW_IS_VALID
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_TRUNCATE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_PROFILER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_RESOURCES
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_RESOURCE_PARAMETER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.SET_TRACER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.SHOULD_BREAK
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.UPDATE_DEMAND
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.UPDATE_OUTPUT_COUNTER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.WORK_IDENTITY_STATIC_FIELD_NAME
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentSlotOffsetFieldName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentVarName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.belowLimitVarName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.closeEvent
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getArgumentSlotOffset
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.Read
import org.neo4j.values.AnyValue

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait CompiledStreamingOperator extends StreamingOperator {
  /**
   * Initialize new tasks for this operator. This code path let's operators create
   * multiple output rows for each row in `inputMorsel`.
   */
  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    Array(compiledNextTask(state.queryContext.transactionalContext.dataRead, inputMorsel.nextCopy, argumentStateMaps))
  }

  protected def compiledNextTask(dataRead: Read,
                                 inputMorsel: Morsel,
                                 argumentStateMaps: ArgumentStateMaps): ContinuableOperatorTaskWithMorsel
}

object CompiledStreamingOperator {

  type CompiledTaskFactory = (Read, MorselParallelizer, ArgumentStateMaps) => IndexedSeq[ContinuableOperatorTaskWithMorsel]

  def getClassDeclaration(packageName: String,
                          className: String,
                          taskClazz: Class[CompiledTask],
                          workIdentityField: StaticField,
                          argumentStates:  Seq[(ArgumentStateMapId, ArgumentStateFactory[_ <: ArgumentState])]): ClassDeclaration[CompiledStreamingOperator] = {

    def staticFieldName(obj: AnyRef) = s"FIELD_${System.identityHashCode(obj)}"

    val argumentStateFields = argumentStates.map {
      case (_, factory) =>
        staticConstant[ArgumentStateFactory[ArgumentState]](staticFieldName(factory), factory)
    }.toIndexedSeq

    val createState = block(
      argumentStates.map {
        case (argumentStateMapId, factory) =>
          /**
           * {{{
           *   argumentStateCreator.createStateMap(mapId, FACTORY_i)
           * }}}
           */
          invokeSideEffect(load("argumentStateCreator"),
            method[ArgumentStateMapCreator,
              ArgumentStateMap[ArgumentState],
              Int,
              ArgumentStateFactory[ArgumentState]]("createArgumentStateMap"),
            constant(argumentStateMapId.x),
            getStatic[ArgumentStateFactory[ArgumentState]](staticFieldName(factory)))
      }: _*)

    ClassDeclaration[CompiledStreamingOperator](
      packageName,
      className,
      extendsClass = None,
      implementsInterfaces = Seq(typeRefOf[CompiledStreamingOperator]),
      constructorParameters = Seq(),
      initializationCode = noop(),
      methods = Seq(
        MethodDeclaration("compiledNextTask",
          returnType = typeRefOf[ContinuableOperatorTaskWithMorsel],
          parameters = Seq(param[Read]("dataRead"),
            param[Morsel]("inputMorsel"),
            param[ArgumentStateMaps]("argumentStateMaps")),
          body = newInstance(Constructor(TypeReference.typeReference(taskClazz),
            Seq(TypeReference.typeReference(classOf[Read]),
              TypeReference.typeReference(classOf[Morsel]),
              TypeReference.typeReference(classOf[ArgumentStateMaps]))),
            load("dataRead"),
            load("inputMorsel"),
            load("argumentStateMaps"))),
        MethodDeclaration("workIdentity",
          returnType = typeRefOf[WorkIdentity],
          parameters = Seq.empty,
          body = getStatic[WorkIdentity](workIdentityField.name)),
        MethodDeclaration("createState",
          returnType = typeRefOf[OperatorState],
          parameters = Seq(param[ArgumentStateMapCreator]("argumentStateCreator"),
            param[StateFactory]("stateFactory"),
            param[PipelinedQueryState]("state"),
            param[QueryResources]("resources")),
          body = block(createState, self()))
      ),
      // NOTE: This has to be called after genOperate!
      genFields = () => argumentStateFields :+ workIdentityField)
  }
}

object ContinuableOperatorTaskWithMorselGenerator {
  private val PACKAGE_NAME = "org.neo4j.codegen"
  private val COUNTER = new AtomicLong(0)

  /**
   * Responsible for generating a tailored class for the OperatorTask, and composing an operator creating new instances of that class
   */
  def compileOperator(template: ContinuableOperatorTaskWithMorselTemplate,
                      workIdentity: WorkIdentity,
                      argumentStates: Seq[(ArgumentStateMapId, ArgumentStateFactory[_ <: ArgumentState])],
                      codeGenerationMode: CodeGenerationMode): CompiledStreamingOperator = {
    val staticWorkIdentity = staticConstant[WorkIdentity](WORK_IDENTITY_STATIC_FIELD_NAME, workIdentity)
    val operatorId = COUNTER.getAndIncrement()
    val generator = CodeGeneration.createGenerator(codeGenerationMode)
    val taskClazz = compileClass(template.genClassDeclaration(PACKAGE_NAME, "OperatorTask"+operatorId, Seq(staticWorkIdentity)), generator)
    val operatorClazz = compileClass(CompiledStreamingOperator.getClassDeclaration(PACKAGE_NAME, "Operator"+operatorId, taskClazz, staticWorkIdentity, argumentStates), generator)

    operatorClazz.getDeclaredConstructor().newInstance()
  }

}

/**
 * We need to specialize this because we don't have support for compiling try-finally.
 */
trait CompiledTask extends ContinuableOperatorTaskWithMorsel
                   with OutputOperator
                   with OutputOperatorState
                   with PreparedOutput
{
  //Fused plans doesn't support time tracking
  override def trackTime: Boolean = false

  override def operateWithProfile(output: Morsel,
                                  state: PipelinedQueryState,
                                  resources: QueryResources,
                                  queryProfiler: QueryProfiler): Unit = {
    initializeProfileEvents(queryProfiler)
    try {
      compiledOperate(output, state.queryContext, state, resources, queryProfiler)
    } finally {
      closeProfileEvents(resources)
    }
  }

  /**
   * Method of [[OutputOperator]] trait. Implementing this allows the same [[CompiledTask]] instance to also act as an [[OutputOperatorState]].
   */
  override final def createState(executionState: ExecutionState): OutputOperatorState = {
    compiledCreateState(executionState)
    this
  }

  /**
   * Method of [[OutputOperator]] trait
   */
  override final def outputBuffer: Option[BufferId] = compiledOutputBuffer() match {
    case -1 => None
    case bufferId => Some(BufferId(bufferId))
  }

  /**
   * Method of [[OutputOperatorState]] trait. Override to prevent creation of profiler event, which is not necessary when output operator is fused.
   */
  override final def prepareOutputWithProfile(output: Morsel,
                                              state: PipelinedQueryState,
                                              resources: QueryResources,
                                              queryProfiler: QueryProfiler): PreparedOutput = this

  /**
   * Method of [[OutputOperatorState]] trait. Implementing this allows the same [[CompiledTask]] instance to also act as a [[PreparedOutput]].
   */
  override protected final def prepareOutput(outputMorsel: Morsel,
                                             state: PipelinedQueryState,
                                             resources: QueryResources,
                                             operatorExecutionEvent: OperatorProfileEvent): PreparedOutput =
    throw new IllegalStateException("Fused operators should be called via prepareOutputWithProfile.")

  /**
   * Method of [[PreparedOutput]] trait. Implementing this allows fused reducing operators to write to [[ExecutionState]].
   */
  override final def produce(): Unit = compiledProduce()

  /**
   * Generated code that initializes the profile events.
   */
  def initializeProfileEvents(queryProfiler: QueryProfiler): Unit

  /**
   * Generated code that executes the operator.
   */
  @throws[Exception]
  def compiledOperate(outputMorsel: Morsel,
                      dbAccess: QueryContext,
                      state: PipelinedQueryState,
                      resources: QueryResources,
                      queryProfiler: QueryProfiler): Unit

  /**
   * Generated code that produces output into the execution state.
   */
  @throws[Exception]
  def compiledProduce(): Unit

  /**
    * Generated code that performs the initialization necessary for performing [[PreparedOutput.produce()]].
    * E.g., retrieving [[org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink]] from [[ExecutionState]].
    */
  @throws[Exception]
  def compiledCreateState(executionState: ExecutionState): Unit

  /**
   * Generated code for [[OutputOperator.outputBuffer]]. Return [[BufferId]] or null.
   * Note, this method return [[Int]] because [[BufferId]] extends [[AnyVal]] causing it to be an [[Int]] in Java.
   */
  @throws[Exception]
  def compiledOutputBuffer(): Int

  /**
   * Generated code that closes all events for profiling.
   */
  def closeProfileEvents(resources: QueryResources): Unit

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = throw new IllegalStateException("Fused operators should be called via operateWithProfile.")

  override def operate(output: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = throw new IllegalStateException("Fused operators should be called via operateWithProfile.")
}

trait OperatorTaskTemplate {
  /**
   * The operator template that this template is wrapping around, or `null`.
   */
  def inner: OperatorTaskTemplate

  /**
   * ID of the Logical plan of this template.
   */
  def id: Id

  /**
   * Uniquely identifies the operator within a fused loop.
   * NOTE: Used in a generated variable name for the continuation state, so the string has to follow Java variable naming rules
   */
  protected def scopeId: String = "operator" + id.x

  protected def codeGen: OperatorExpressionCompiler

  final def map[T](f: OperatorTaskTemplate => T): List[T] = {
    inner match {
      case null => f(this) :: Nil
      case operator => f(this) :: operator.map(f)
    }
  }

  final def flatMap[T](f: OperatorTaskTemplate => Seq[T]): Seq[T] = {
    inner match {
      case null => f(this) ++ Seq.empty
      case operator => f(this) ++ operator.flatMap(f)
    }
  }

  def genClassDeclaration(packageName: String, className: String, staticFields: Seq[StaticField]): ClassDeclaration[CompiledTask] = {
    throw new InternalException("Illegal start operator template")
  }

  def genInit: IntermediateRepresentation

  def executionEventField: InstanceField = field[OperatorProfileEvent]("operatorExecutionEvent_" + id.x)

  def genProfileEventField: Option[Field] = Some(executionEventField)

  def genInitializeProfileEvents: IntermediateRepresentation = {
    block(
      setField(executionEventField,
        invoke(QUERY_PROFILER, method[QueryProfiler, OperatorProfileEvent, Int, Boolean]("executeOperator"), constant(id.x), constant(false))),
      // note: We do not generate resources.setTracer(...) here, because that does not play nice with
      //       fused operators. Instead each template has to call setTracer when it allocates it's cursor.
      genSetExecutionEvent(loadField(executionEventField)),
      inner.genInitializeProfileEvents)
  }

  def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation

  def genCloseProfileEvents: IntermediateRepresentation = {
    block(
      genSetExecutionEvent(NO_OPERATOR_PROFILE_EVENT),
      invokeSideEffect(QUERY_RESOURCES, method[QueryResources, Unit, KernelReadTracer]("setKernelTracer"), NO_KERNEL_TRACER),
      closeEvent(id),
      inner.genCloseProfileEvents
    )
  }

  // TODO: Make separate genOperate and genOperateSingleRow methods to clarify the distinction
  //       between streaming (loop over the whole output morsel) and stateless (processing of single row inlined into outer loop) usage
  /**
   * Responsible for generating the loop code of:
   * {{{
   *     def operate(context: MorselExecutionContext,
   *                 state: QueryState,
   *                 resources: QueryResources,
   *                 queryProfiler: QueryProfiler): Unit
   * }}}
   */
  final def genOperateWithExpressions: IntermediateRepresentation = {
    codeGen.beginScope(scopeId)
    val body = genOperate
    val localState = codeGen.endScope()

    val declarations = localState.declarations
    val assignments = localState.assignments

    val expressionCursors = genExpressions.flatMap(_.variables)
      .intersect(Seq(ExpressionCompiler.vNODE_CURSOR,
        ExpressionCompiler.vPROPERTY_CURSOR,
        ExpressionCompiler.vRELATIONSHIP_CURSOR))
    val setTracerCalls =
      if (expressionCursors.nonEmpty) {
        expressionCursors.map(cursor => invokeSideEffect(load(cursor), SET_TRACER, loadField(executionEventField)))
      } else {
        Seq.empty
      }

    block(setTracerCalls ++ declarations ++ assignments :+ body:_*)
  }

  protected def doIfInnerCantContinue(op: IntermediateRepresentation): IntermediateRepresentation =
    inner.genCanContinue.map(innerCanContinue => condition(not(innerCanContinue)) (op)).getOrElse(op)

  protected def innerCanContinue: IntermediateRepresentation =
    inner.genCanContinue.getOrElse(constant(false))

  protected def genOperate: IntermediateRepresentation

  /**
   * Responsible for generating code that comes before entering the loop of operate()
   */
  def genOperateEnter: IntermediateRepresentation = inner.genOperateEnter

  /**
   * Responsible for generating code that comes after exiting the loop of operate()
   */
  def genOperateExit: IntermediateRepresentation = inner.genOperateExit

  /**
   * Responsible for generating [[PreparedOutput]] method:
   * {{{
   *     def produce(): Unit
   * }}}
   */
  protected def genProduce: IntermediateRepresentation = inner.genProduce

  /**
   * Responsible for generating the body of [[OutputOperator]] method (but is not expected to return anything):
   * {{{
   *     def createState(executionState: ExecutionState): OutputOperatorState
   * }}}
   */
  def genCreateState: IntermediateRepresentation = inner.genCreateState

  /**
   * Responsible for generating:
   * {{{
   *   override def outputBuffer: Option[BufferId]
   * }}}
   */
  def genOutputBuffer: Option[IntermediateRepresentation] = inner.genOutputBuffer

  /**
   * Get the intermediate expressions used by the operator.
   *
   * Should NOT recurse into inner operator templates.
   */
  def genExpressions: Seq[IntermediateExpression]

  /**
   * Get the fields used by the operator, excluding those used by intermediate expressions.
   *
   * Should NOT recurse into inner operator templates.
   */
  def genFields: Seq[Field]

  /**
   * Get the local variables used by the operator, excluding those used by intermediate expressions.
   *
   * Should NOT recurse into inner operator templates.
   */
  def genLocalVariables: Seq[LocalVariable]

  /**
   * Responsible for generating:
   * {{{
   *   override def canContinue: Boolean
   * }}}
   */
  def genCanContinue: Option[IntermediateRepresentation]

  /**
   * Responsible for generating:
   * {{{
   *   override def closeCursors(resources: QueryResources): Unit
   * }}}
   */
  def genCloseCursors: IntermediateRepresentation
}

object OperatorTaskTemplate {
  def empty(withId: Id): OperatorTaskTemplate = new OperatorTaskTemplate {
    override def inner: OperatorTaskTemplate = null
    override protected def codeGen: OperatorExpressionCompiler = null
    override def id: Id = withId
    override def genOperate: IntermediateRepresentation = noop()
    override def genProduce: IntermediateRepresentation = noop()
    override def genCreateState: IntermediateRepresentation = noop()
    override def genFields: Seq[Field] = Seq.empty
    override def genLocalVariables: Seq[LocalVariable] = Seq.empty
    override def genExpressions: Seq[IntermediateExpression] = Seq.empty
    override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = noop()
    override def genCanContinue: Option[IntermediateRepresentation] = None
    override def genCloseCursors: IntermediateRepresentation = noop()
    override def genOutputBuffer: Option[IntermediateRepresentation] = None
    override def genInit: IntermediateRepresentation = noop()
  }

  /**
   * Used from generated code.
   * Advances the cursor to just before the next argument is
   * about to start, so that after one call to `next` the cursor will point at the next
   * argument. Not necessary for correctness but will speed things up considerably
   * when there are many rows per argument.
   */
  def flushToNextArgument(cursor: MorselReadCursor,
                          currentArgument: Long,
                          argumentOffset: Int): Unit = {
    // do nothing if cursor is not on a valid row
    if (cursor.onValidRow()) {
      var endRow = cursor.row
      while (cursor.onValidRow() && currentArgument == cursor.getLongAt(argumentOffset) && cursor.next()) {
        if (currentArgument == cursor.getLongAt(argumentOffset)) {
          endRow = cursor.row
        }
      }

      // set to last valid row
      if (!cursor.onValidRow()) {
        cursor.setRow(endRow)
      }
    }
  }

  /**
   * Used from generated code.
   * If cursor is currently on an invalid (e.g., cancelled) row, it will be advanced.
   * After calling this method the cursor will either point at the next valid row, or the end of the cursor (an invalid row).
   */
 def nextRowIfInvalid(cursor: MorselReadCursor): Unit = {
   if (!cursor.onValidRow()) {
     val row = cursor.row
     if (!cursor.next()) {
       cursor.setRow(row)
     }
   }
 }
}

trait ContinuableOperatorTaskWithMorselTemplate extends OperatorTaskTemplate {

  protected def isHead: Boolean
  protected def genOperateHead: IntermediateRepresentation
  protected def genOperateMiddle: IntermediateRepresentation

  final override def genOperate: IntermediateRepresentation =
    if (isHead) genOperateHead else genOperateMiddle

  protected def genScopeWithLocalDeclarations(scopeId: String, genBody: => IntermediateRepresentation): IntermediateRepresentation = {
    codeGen.beginScope(scopeId)
    val body = genBody
    val localsState = codeGen.endScope()
    block(localsState.declarations ++ localsState.assignments :+ body: _*)
  }

  // We let the generated class extend the abstract class CompiledContinuableOperatorTaskWithMorsel(which extends ContinuableOperatorTaskWithMorsel),
  // which implements the close() and produceWorkUnit() methods from the ContinuableOperatorTask

  // TODO: Use methods of actual interface to generate declaration?
  override def genClassDeclaration(packageName: String, className: String, staticFields: Seq[StaticField]): ClassDeclaration[CompiledTask] = {
    ClassDeclaration[CompiledTask](
      packageName,
      className,
      extendsClass = None,
      implementsInterfaces = Seq(typeRefOf[CompiledTask]),
      constructorParameters = Seq(DATA_READ_CONSTRUCTOR_PARAMETER,
        INPUT_MORSEL_CONSTRUCTOR_PARAMETER,
        ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER),
      initializationCode = genInit,
      methods = Seq(
        MethodDeclaration("initializeProfileEvents",
          returnType = typeRefOf[Unit],
          parameters = Seq(param[QueryProfiler]("queryProfiler")),
          body = genInitializeProfileEvents
        ),
        MethodDeclaration("compiledOperate",
          returnType = typeRefOf[Unit],
          Seq(param[Morsel]("outputMorsel"),
            param[QueryContext]("dbAccess"),
            param[PipelinedQueryState]("state"),
            param[QueryResources]("resources"),
            param[QueryProfiler]("queryProfiler")
          ),
          body = block(
            invokeStatic(method[OperatorTaskTemplate, Unit, MorselReadCursor]("nextRowIfInvalid"), INPUT_CURSOR),
            genOperateEnter,
            genOperateWithExpressions,
            genOperateExit
          ),
          genLocalVariables = () => {
            Seq(
              variable[Array[AnyValue]]("params",
                invoke(QUERY_STATE,
                  method[PipelinedQueryState, Array[AnyValue]]("params"))),
              variable[ExpressionCursors]("cursors",
                invoke(QUERY_RESOURCES,
                  method[QueryResources, ExpressionCursors]("expressionCursors"))),
              variable[Array[AnyValue]]("expressionVariables",
                invoke(QUERY_RESOURCES,
                  method[QueryResources, Array[AnyValue], Int]("expressionVariables"),
                  invoke(QUERY_STATE,
                    method[PipelinedQueryState, Int]("nExpressionSlots")))
              )) ++ flatMap[LocalVariable](op => op.genLocalVariables ++
              op.genExpressions.flatMap(_.variables))
          },
          parameterizedWith = None,
          throws = Some(typeRefOf[Exception])),
        MethodDeclaration("compiledProduce",
          returnType = typeRefOf[Unit],
          parameters = Seq.empty,
          body = genProduce,
          genLocalVariables = () => Seq.empty,
          throws = Some(typeRefOf[Exception])),
        MethodDeclaration("compiledCreateState",
          returnType = typeRefOf[Unit],
          Seq(param[ExecutionState]("executionState")),
                          body = genCreateState,
                          genLocalVariables = () => Seq.empty,
                          throws = Some(typeRefOf[Exception])),
        MethodDeclaration("compiledOutputBuffer",
          returnType = typeRefOf[Int],
          parameters = Seq.empty,
          body = genOutputBuffer.getOrElse(constant(-1))
        ),
        MethodDeclaration("closeProfileEvents",
          returnType = typeRefOf[Unit],
          parameters = Seq(QUERY_RESOURCE_PARAMETER),
          body = genCloseProfileEvents
        ),
        MethodDeclaration("canContinue",
          returnType = typeRefOf[Boolean],
          parameters = Seq.empty,
          body = genCanContinue.getOrElse(constant(false))
        ),
        MethodDeclaration("closeCursors",
          returnType = typeRefOf[Unit],
          parameters = Seq(QUERY_RESOURCE_PARAMETER),
          body = genCloseCursors,
          genLocalVariables = () => Seq(CURSOR_POOL_V)
        ),
        MethodDeclaration("workIdentity",
          returnType = typeRefOf[WorkIdentity],
          parameters = Seq.empty,
          body = getStatic[WorkIdentity](WORK_IDENTITY_STATIC_FIELD_NAME)
        ),
        // This is only needed because we extend an abstract scala class containing `val dataRead`
        MethodDeclaration("dataRead",
          returnType = typeRefOf[Read],
          parameters = Seq.empty,
          body = loadField(DATA_READ)
        ),
        // This is only needed because we extend an abstract scala class containing `val inputMorsel`
        MethodDeclaration("inputMorsel",
          returnType = typeRefOf[Morsel],
          parameters = Seq.empty,
          body = invoke(INPUT_CURSOR, method[MorselReadCursor, Morsel]("morsel"))
        )
      ),
      // NOTE: This has to be called after genOperate!
      genFields = () => staticFields ++ Seq(DATA_READ, INPUT_CURSOR_FIELD) ++ flatMap[Field](op => op.genFields ++
        op.genProfileEventField ++
        op.genExpressions.flatMap(_.fields)))
  }
}

case class LimitNotReachedState(argumentStateMapId: ArgumentStateMapId)

// Used for innermost, e.g. to insert the `outputRow.next` of the start operator at the deepest nesting level
// and also for providing demand operations
class DelegateOperatorTaskTemplate(var shouldWriteToContext: Boolean = true,
                                   var shouldCheckDemand: Boolean = false,
                                   var shouldCheckOutputCounter: Boolean = false,
                                   var shouldCheckBreak: Boolean = false,
                                   val limits: mutable.ArrayBuffer[LimitNotReachedState] = mutable.ArrayBuffer.empty)
                                  (protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  // Reset configuration to the default settings
  def reset(): Unit = {
    shouldWriteToContext = true
    shouldCheckDemand = false
    shouldCheckOutputCounter = false
    shouldCheckBreak = false
  }

  def resetBelowLimit: IntermediateRepresentation = {
    val ops = limits.map {
      case LimitNotReachedState(argumentStateMapId) =>
        block(
          condition(not(load(belowLimitVarName(argumentStateMapId)))) {
            block(
              invokeStatic(method[OperatorTaskTemplate, Unit, MorselReadCursor, Long, Int]("flushToNextArgument"),
                INPUT_CURSOR,
                load(argumentVarName(argumentStateMapId)),
                loadField(field[Int](argumentSlotOffsetFieldName(argumentStateMapId)))),
              assign(belowLimitVarName(argumentStateMapId), constant(true)))
          })
     } :+  block(
      limits.map(limit => assign(argumentVarName(limit.argumentStateMapId), getArgument(limit.argumentStateMapId))):_*
    )
    block(ops:_*)
  }

  def setToNextIfBelowLimit(field: Field, next: IntermediateRepresentation): IntermediateRepresentation = {
    if (limits.isEmpty) {
      setField(field, next)
    }
    else {
      val condition = limits.map(limit => load(belowLimitVarName(limit.argumentStateMapId))).reduceLeft(and)
      ifElse(condition)(setField(field, next))(setField(field, constant(false)))
    }
  }

  override def inner: OperatorTaskTemplate = null

  override val id: Id = Id.INVALID_ID

  override def genInit: IntermediateRepresentation = noop()

  override def genInitializeProfileEvents: IntermediateRepresentation = noop()

  override def genProfileEventField: Option[Field] = None

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = noop()

  override def genCloseProfileEvents: IntermediateRepresentation = noop()

  override protected def genOperate: IntermediateRepresentation = {
    val ops = new ArrayBuffer[IntermediateRepresentation]

    if (shouldWriteToContext) {
      ops += block(codeGen.writeLocalsToSlots(), invokeSideEffect(OUTPUT_CURSOR, NEXT))
    }
    if (shouldCheckOutputCounter) {
      ops += UPDATE_OUTPUT_COUNTER
    }

    if (ops.nonEmpty) {
      block(ops: _*)
    } else {
      noop()
    }
  }

  def resetCachedPropertyVariables: IntermediateRepresentation = {
    block(
      codeGen.getAllLocalsForCachedProperties.map {
        case (_, name) =>
          assign(name, constant(null))
      } :_*)
  }

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  def predicate: IntermediateRepresentation = {
    val conditions = new ArrayBuffer[IntermediateRepresentation]

    if (shouldWriteToContext) {
      conditions += OUTPUT_ROW_IS_VALID
    }
    if (shouldCheckDemand) {
      conditions += HAS_DEMAND
    }
    if (shouldCheckOutputCounter) {
      conditions += HAS_REMAINING_OUTPUT
    }

    if (shouldCheckBreak) {
      conditions += not(load(SHOULD_BREAK))
    }

    and(conditions)
  }

  override def genOperateEnter: IntermediateRepresentation = block(
    limits.map(limit => declareAndAssign(typeRefOf[Long], argumentVarName(limit.argumentStateMapId), constant(-1L))): _*
  )

  /**
   * If we need to care about demand (produceResult part of the fused operator)
   * we need to update the demand after having produced data.
   */
  override def genOperateExit: IntermediateRepresentation = {
    val updates = new ArrayBuffer[IntermediateRepresentation]

    if (shouldWriteToContext) {
      updates += OUTPUT_TRUNCATE
    }
    if (shouldCheckDemand) {
      updates += UPDATE_DEMAND
    }

    if (updates.nonEmpty) {
      block(updates: _*)
    } else {
      noop()
    }
  }

  override def genFields: Seq[Field] =
    limits.map(limit => field[Int](argumentSlotOffsetFieldName(limit.argumentStateMapId), getArgumentSlotOffset(limit.argumentStateMapId)))

  override def genLocalVariables: Seq[LocalVariable] = {
    val seq = Seq.newBuilder[LocalVariable]
    if (shouldCheckOutputCounter) seq += OUTPUT_COUNTER
    if (shouldWriteToContext) seq += OUTPUT_CURSOR_VAR
    limits.foreach {
      case LimitNotReachedState(argumentStateMapId) =>
        seq += variable[Boolean](belowLimitVarName(argumentStateMapId), constant(true))
    }

    seq.result()
  }

  override def genCanContinue: Option[IntermediateRepresentation] = None

  override def genCloseCursors: IntermediateRepresentation = block()

  override protected def genProduce: IntermediateRepresentation = noop()

  override def genCreateState: IntermediateRepresentation = noop()

  override def genOutputBuffer: Option[IntermediateRepresentation] = None

  private def getArgument(argumentStateMapId: ArgumentStateMapId) =
    invoke(INPUT_CURSOR, method[CypherRow, Long, Int]("getLongAt"), loadField(field[Int](argumentSlotOffsetFieldName(argumentStateMapId))))

}
