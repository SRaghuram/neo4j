/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.codegen
import org.neo4j.codegen.ClassHandle
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
import org.neo4j.codegen.api.Parameter
import org.neo4j.codegen.api.StaticField
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateDescriptor
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.DynamicFactoryArgumentStateDescriptor
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.StaticFactoryArgumentStateDescriptor
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_MORSEL_DATA_CONSTRUCTOR_PARAMETER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_MORSEL_DATA_FIELD
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_ROW_IS_VALID
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.MORSEL_DATA_INPUT_CURSOR_FIELD
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.setMemoryTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateBufferFactoryFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
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
    singletonIndexedSeq(compiledNextTask(state.queryContext.transactionalContext.dataRead, inputMorsel.nextCopy, argumentStateMaps))
  }

  protected def compiledNextTask(dataRead: Read,
                                 inputMorsel: Morsel,
                                 argumentStateMaps: ArgumentStateMaps): ContinuableOperatorTaskWithMorsel
}

trait CompiledStreamingOperatorWithMorselData extends Operator with DataInputOperatorState[MorselData] {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = this

  override def nextTasks(state: PipelinedQueryState,
                         input: MorselData,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
    singletonIndexedSeq(compiledNextTask(state.queryContext.transactionalContext.dataRead, input, argumentStateMaps))
  }

  def nextTasks(state: PipelinedQueryState,
                input: Object,
                argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
    singletonIndexedSeq(compiledNextTask(state.queryContext.transactionalContext.dataRead, input.asInstanceOf[MorselData], argumentStateMaps))
  }

  protected def compiledNextTask(dataRead: Read,
                                 inputMorselData: MorselData,
                                 argumentStateMaps: ArgumentStateMaps): ContinuableOperatorTaskWithMorselData

}

object CompiledStreamingOperatorWithMorselTemplate extends CompiledStreamingOperatorTemplate[CompiledStreamingOperator] {
  override protected def genCompiledNextTask(taskClassHandle: ClassHandle): MethodDeclaration = {
    MethodDeclaration("compiledNextTask",
      returnType = typeRefOf[ContinuableOperatorTaskWithMorsel],
      parameters = Seq(param[Read]("dataRead"),
        param[Morsel]("inputMorsel"),
        param[ArgumentStateMaps]("argumentStateMaps")),
      body = newInstance(Constructor(taskClassHandle,
        Seq(TypeReference.typeReference(classOf[Read]),
          TypeReference.typeReference(classOf[Morsel]),
          TypeReference.typeReference(classOf[ArgumentStateMaps]))),
        load("dataRead"),
        load("inputMorsel"),
        load("argumentStateMaps")))
  }

  override protected def inputOperatorType: TypeReference = typeRefOf[CompiledStreamingOperator]
}

object CompiledStreamingOperatorWithMorselDataTemplate extends CompiledStreamingOperatorTemplate[CompiledStreamingOperatorWithMorselData] {
  override protected def genCompiledNextTask(taskClassHandle: ClassHandle): MethodDeclaration = {
    MethodDeclaration("compiledNextTask",
      returnType = typeRefOf[ContinuableOperatorTaskWithMorselData],
      parameters = Seq(param[Read]("dataRead"),
        param[MorselData]("morselData"),
        param[ArgumentStateMaps]("argumentStateMaps")),
      body = newInstance(Constructor(taskClassHandle,
        Seq(TypeReference.typeReference(classOf[Read]),
          TypeReference.typeReference(classOf[MorselData]),
          TypeReference.typeReference(classOf[ArgumentStateMaps]))),
        load("dataRead"),
        load("morselData"),
        load("argumentStateMaps")))
  }

  override protected def inputOperatorType: TypeReference = typeRefOf[CompiledStreamingOperatorWithMorselData]
}

trait CompiledStreamingOperatorTemplate[T <: Operator] {
  private[this] val ARGUMENT_STATE_FACTORY_FIELD_PREFIX = "ARGUMENT_STATE_FACTORY"
  private[this] val ARGUMENT_STATE_FACTORY_FACTORY_FIELD_PREFIX = "ARGUMENT_STATE_FACTORY_FACTORY"

  private def staticFieldName(prefix: String, obj: AnyRef) = s"FIELD_${prefix}_${System.identityHashCode(obj)}"

  def genClassDeclaration(packageName: String,
                          className: String,
                          taskClassHandle: ClassHandle,
                          workIdentityField: StaticField,
                          argumentStates:  Seq[ArgumentStateDescriptor]): ClassDeclaration[T] = {


    val argumentStateFields = argumentStates.map {
      case StaticFactoryArgumentStateDescriptor(_, factory, _) =>
        staticConstant[ArgumentStateFactory[ArgumentState]](staticFieldName(ARGUMENT_STATE_FACTORY_FIELD_PREFIX, factory), factory)
      case DynamicFactoryArgumentStateDescriptor(_, factoryFactory, _, _) =>
        staticConstant[ArgumentStateBufferFactoryFactory](staticFieldName(ARGUMENT_STATE_FACTORY_FACTORY_FIELD_PREFIX, factoryFactory), factoryFactory)
    }.toIndexedSeq

    val createState = block(
      argumentStates.map { argumentStateVariant =>
        /**
         * {{{
         *   argumentStateCreator.createStateMap(mapId, FACTORY_i, ordered)
         * }}}
         */
        invokeSideEffect(load("argumentStateCreator"),
          method[ArgumentStateMapCreator,
            ArgumentStateMap[ArgumentState],
            Int,
            ArgumentStateFactory[ArgumentState],
            Boolean]("createArgumentStateMap"),
          constant(argumentStateVariant.argumentStateMapId.x),
          genArgumentStateFactory(argumentStateVariant),
          constant(argumentStateVariant.ordered))
      }: _*)

    ClassDeclaration[T](
      packageName,
      className,
      extendsClass = None,
      implementsInterfaces = Seq(inputOperatorType),
      constructorParameters = Seq(),
      initializationCode = noop(),
      methods = Seq(
        genCompiledNextTask(taskClassHandle),
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

  private def genArgumentStateFactory(argumentStateFactoryVariant: ArgumentStateDescriptor): IntermediateRepresentation = {
    argumentStateFactoryVariant match {
      case StaticFactoryArgumentStateDescriptor(_, factory, _) =>
        getStatic[ArgumentStateFactory[ArgumentState]](staticFieldName(ARGUMENT_STATE_FACTORY_FIELD_PREFIX, factory))
      case DynamicFactoryArgumentStateDescriptor(_, factoryFactory, operatorId, _) =>
        invoke(
          getStatic[ArgumentStateBufferFactoryFactory](staticFieldName(ARGUMENT_STATE_FACTORY_FACTORY_FIELD_PREFIX, factoryFactory)),
          method[ArgumentStateBufferFactoryFactory, ArgumentStateFactory[ArgumentStateBuffer], StateFactory, Int]("createFactory"),
          load("stateFactory"),
          constant(operatorId.x)
        )
    }
  }

  protected def genCompiledNextTask(taskClassHandle: ClassHandle): MethodDeclaration
  protected def inputOperatorType: codegen.TypeReference
}

object ContinuableOperatorTaskWithMorselGenerator {
  private val PACKAGE_NAME = "org.neo4j.codegen"
  private val COUNTER = new AtomicLong(0)

  /**
   * Responsible for generating a tailored class for the OperatorTask, and composing an operator creating new instances of that class
   */
  def compileOperator(template: OperatorTaskTemplate,
                      workIdentity: WorkIdentity,
                      argumentStates: Seq[ArgumentStateDescriptor],
                      codeGenerationMode: CodeGenerationMode,
                      pipelineId: PipelineId): Operator = {
    val staticWorkIdentity = staticConstant[WorkIdentity](WORK_IDENTITY_STATIC_FIELD_NAME, workIdentity)
    val operatorId = COUNTER.getAndIncrement()
    def className(name: String) = s"${name}Pipeline${pipelineId.x}_$operatorId"
    val generator = CodeGeneration.createGenerator(codeGenerationMode)
    val taskDeclaration = template.genClassDeclaration(PACKAGE_NAME, className("OperatorTask"), Seq(staticWorkIdentity))
    val taskClassHandle = compileClass(taskDeclaration, generator)
    val operatorDeclaration = operatorTemplateFromTaskTemplate(template).genClassDeclaration(
      PACKAGE_NAME,
      className("Operator"),
      taskClassHandle,
      staticWorkIdentity,
      argumentStates
    )
    val operatorClassHandle = compileClass(operatorDeclaration, generator)

    CodeGeneration.loadAndSetConstants(taskClassHandle, taskDeclaration)
    val clazz = CodeGeneration.loadAndSetConstants(operatorClassHandle, operatorDeclaration)
    clazz.getDeclaredConstructor().newInstance()
  }

  private def operatorTemplateFromTaskTemplate(template: OperatorTaskTemplate): CompiledStreamingOperatorTemplate[_ <: Operator] = template match {
    case _: ContinuableOperatorTaskWithMorselTemplate => CompiledStreamingOperatorWithMorselTemplate
    case _: ContinuableOperatorTaskWithMorselDataTemplate => CompiledStreamingOperatorWithMorselDataTemplate
  }
}

/**
 * We need to specialize this because we don't have support for compiling try-finally.
 */
trait CompiledTask extends ContinuableOperatorTask
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
  override final def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState = {
    compiledCreateState(executionState, stateFactory)
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
  override final def produce(resources: QueryResources): Unit = compiledProduce(resources)

  override final def close(): Unit = compiledCloseOutput()

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
  def compiledProduce(resources: QueryResources): Unit

  /**
   * Generated code that closes the produced output.
   */
  @throws[Exception]
  def compiledCloseOutput(): Unit

  /**
   * Generated code that closes additional resources associated with the input.
   * The actual input (i.e. a Morsel) will be closed by ContinuableOperatorTaskWithMorsel.
   */
  @throws[Exception]
  def compiledCloseInput(): Unit

  /**
    * Generated code that performs the initialization necessary for performing [[PreparedOutput.produce()]].
    * E.g., retrieving [[org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink]] from [[ExecutionState]].
    */
  @throws[Exception]
  def compiledCreateState(executionState: ExecutionState, stateFactory: StateFactory): Unit

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

trait CompiledTaskWithMorsel extends CompiledTask with ContinuableOperatorTaskWithMorsel {
  override final def closeInput(operatorCloser: OperatorCloser): Unit = {
    compiledCloseInput()
    // Specifying the concrete super class here, since the extending generated Java class does otherwise not get it right.
    super[ContinuableOperatorTaskWithMorsel].closeInput(operatorCloser)
  }
}

trait CompiledTaskWithMorselData extends CompiledTask with ContinuableOperatorTaskWithMorselData {

  override final def closeInput(operatorCloser: OperatorCloser): Unit = {
    compiledCloseInput()
    // Specifying the concrete super class here, since the extending generated Java class does otherwise not get it right.
    super[ContinuableOperatorTaskWithMorselData].closeInput(operatorCloser)
  }
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
      .intersect(ExpressionCompilation.vCURSORS)
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

  protected def innerCannotContinue: IntermediateRepresentation =
    inner.genCanContinue.map(not).getOrElse(constant(true))

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
   * Responsible for generating [[PreparedOutput]] method:
   * {{{
   *     def close(): Unit
   * }}}
   */
  def genCloseOutput: IntermediateRepresentation = inner.genCloseOutput

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

  /**
   * Responsible for generating [[CompiledTask]] method:
   * {{{
   *     def compiledCloseInput(): Unit
   * }}}
   */
  def genCloseInput: IntermediateRepresentation = inner.genCloseOutput
}

object OperatorTaskTemplate {
  def empty(withId: Id): OperatorTaskTemplate = new OperatorTaskTemplate {
    override def inner: OperatorTaskTemplate = null
    override protected def codeGen: OperatorExpressionCompiler = null
    override def id: Id = withId
    override def genOperate: IntermediateRepresentation = noop()
    override def genProduce: IntermediateRepresentation = noop()
    override def genCloseOutput: IntermediateRepresentation = noop()
    override def genCreateState: IntermediateRepresentation = noop()
    override def genFields: Seq[Field] = Seq.empty
    override def genLocalVariables: Seq[LocalVariable] = Seq.empty
    override def genExpressions: Seq[IntermediateExpression] = Seq.empty
    override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = noop()
    override def genCanContinue: Option[IntermediateRepresentation] = None
    override def genCloseCursors: IntermediateRepresentation = noop()
    override def genCloseInput: IntermediateRepresentation = noop()
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
}

trait ContinuableOperatorTaskWithMorselTemplate extends ContinuableOperatorTaskTemplate {
  override protected def genConstructorParameters(): Seq[Parameter] = Seq(
    DATA_READ_CONSTRUCTOR_PARAMETER,
    INPUT_MORSEL_CONSTRUCTOR_PARAMETER,
    ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER
  )

  override protected def genMoreMethodDeclarations: Seq[MethodDeclaration] = Seq(
    // This is only needed because we extend an abstract scala class containing `val inputMorsel`
    MethodDeclaration("inputMorsel",
      returnType = typeRefOf[Morsel],
      parameters = Seq.empty,
      body = invoke(INPUT_CURSOR, method[MorselReadCursor, Morsel]("morsel"))
    )
  )

  override protected def genImplementsInterfaces: Seq[TypeReference] = Seq(typeRefOf[CompiledTaskWithMorsel])

  override def genInputCursorField: InstanceField = INPUT_CURSOR_FIELD
}

trait ContinuableOperatorTaskWithMorselDataTemplate extends ContinuableOperatorTaskTemplate {

  override protected def genConstructorParameters(): Seq[Parameter] = Seq(
    DATA_READ_CONSTRUCTOR_PARAMETER,
    INPUT_MORSEL_DATA_CONSTRUCTOR_PARAMETER,
    ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER
  )

  override protected def genMoreMethodDeclarations: Seq[MethodDeclaration] = Seq(
    // This is only needed because we extend an abstract scala class containing `val morselData`
    MethodDeclaration("morselData",
      returnType = typeRefOf[MorselData],
      parameters = Seq.empty,
      body = loadField(INPUT_MORSEL_DATA_FIELD)
    )
  )

  override protected def genImplementsInterfaces: Seq[TypeReference] = Seq(typeRefOf[CompiledTaskWithMorselData])

  override def genInputCursorField: InstanceField = MORSEL_DATA_INPUT_CURSOR_FIELD

  override def genFields: Seq[Field] = Seq(INPUT_MORSEL_DATA_FIELD)
}

trait ContinuableOperatorTaskTemplate extends OperatorTaskTemplate {

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

  /**
   * Clear operator state assuming input row has been cancelled.
   *
   * Should NOT recurse into inner operator templates.
   */
  protected def genClearStateOnCancelledRow: IntermediateRepresentation

  def genAdvanceOnCancelledRow: IntermediateRepresentation =
    condition(not(INPUT_ROW_IS_VALID)) {
      block(
        clearStateForEachOperator,
        invokeSideEffect(INPUT_CURSOR, NEXT)
      )
    }

  def clearStateForEachOperator: IntermediateRepresentation = {
    val clearStateCalls = map {
      case x: ContinuableOperatorTaskTemplate => x.genClearStateOnCancelledRow
      case _ => noop()
    }
    block(clearStateCalls.reverse:_*)
  }

  // We let the generated class extend the abstract class CompiledContinuableOperatorTaskWithMorsel(which extends ContinuableOperatorTaskWithMorsel),
  // which implements the close() and produceWorkUnit() methods from the ContinuableOperatorTask

  // TODO: Use methods of actual interface to generate declaration?
  override def genClassDeclaration(packageName: String, className: String, staticFields: Seq[StaticField]): ClassDeclaration[CompiledTask] = {
    ClassDeclaration[CompiledTask](
      packageName,
      className,
      extendsClass = None,
      implementsInterfaces = genImplementsInterfaces,
      constructorParameters = genConstructorParameters,
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
            param[QueryContext](ExpressionCompilation.DB_ACCESS_NAME),
            param[PipelinedQueryState]("state"),
            param[QueryResources]("resources"),
            param[QueryProfiler]("queryProfiler")
          ),
          body = block(
            genOperateEnter,
            genOperateWithExpressions,
            genOperateExit
          ),
          genLocalVariables = () => {
            Seq(
              variable[Array[AnyValue]](ExpressionCompilation.PARAMS_NAME,
                invoke(QUERY_STATE,
                  method[PipelinedQueryState, Array[AnyValue]]("params"))),
              variable[ExpressionCursors](ExpressionCompilation.CURSORS_NAME,
                invoke(QUERY_RESOURCES,
                  method[QueryResources, ExpressionCursors]("expressionCursors"))),
              variable[Array[AnyValue]](ExpressionCompilation.EXPRESSION_VARIABLES_NAME,
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
          parameters = Seq(param[QueryResources]("resources")),
          body = genProduce,
          genLocalVariables = () => Seq.empty,
          throws = Some(typeRefOf[Exception])),
        MethodDeclaration("compiledCloseOutput",
          returnType = typeRefOf[Unit],
          parameters = Seq.empty,
          body = genCloseOutput,
          genLocalVariables = () => Seq.empty,
          throws = Some(typeRefOf[Exception])),
        MethodDeclaration("compiledCloseInput",
          returnType = typeRefOf[Unit],
          parameters = Seq.empty,
          body = genCloseInput,
          genLocalVariables = () => Seq.empty,
          throws = Some(typeRefOf[Exception])),
        MethodDeclaration("compiledCreateState",
          returnType = typeRefOf[Unit],
          Seq(param[ExecutionState]("executionState"),
              param[StateFactory]("stateFactory")),
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
        )
      ) ++ genMoreMethodDeclarations,
      // NOTE: This has to be called after genOperate!
      genFields = () => staticFields ++ Seq(DATA_READ, genInputCursorField) ++ flatMap[Field](op => op.genFields ++
        op.genProfileEventField ++
        op.genExpressions.flatMap(_.fields)))
  }

  protected def genConstructorParameters: Seq[Parameter]

  protected def genMoreMethodDeclarations: Seq[MethodDeclaration]

  protected def genImplementsInterfaces: Seq[codegen.TypeReference]

  protected def genInputCursorField: InstanceField
}

// Used for innermost, e.g. to insert the `outputRow.next` of the start operator at the deepest nesting level
// and also for providing demand operations
class DelegateOperatorTaskTemplate(var shouldWriteToContext: Boolean = true,
                                   var shouldCheckDemand: Boolean = false,
                                   var shouldCheckOutputCounter: Boolean = false,
                                   var shouldCheckBreak: Boolean = false,
                                   val limits: mutable.ArrayBuffer[ArgumentStateMapId] = mutable.ArrayBuffer.empty)
                                  (protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  // Reset configuration to the default settings
  def reset(): Unit = {
    shouldWriteToContext = true
    shouldCheckDemand = false
    shouldCheckOutputCounter = false
    shouldCheckBreak = false
  }

  def resetBelowLimitAndAdvanceToNextArgument: IntermediateRepresentation = {
    val ops = limits.map {(argumentStateMapId) =>
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
      limits.map(argumentStateMapId => assign(argumentVarName(argumentStateMapId), getArgument(argumentStateMapId))):_*
    )
    block(ops:_*)
  }

  def setUnlessPastLimit(field: Field, next: IntermediateRepresentation): IntermediateRepresentation = {
    if (limits.isEmpty) {
      setField(field, next)
    }
    else {
      val condition = limits.map(argumentStateMapId => load(belowLimitVarName(argumentStateMapId))).reduceLeft(and)
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
    limits.map(argumentStateMapId => declareAndAssign(typeRefOf[Long], argumentVarName(argumentStateMapId), constant(-1L))): _*
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
    codeGen.exitOperations.foreach(o => updates += o)

    if (updates.nonEmpty) {
      block(updates: _*)
    } else {
      noop()
    }
  }

  override def genFields: Seq[Field] =
    limits.map(argumentStateMapId => field[Int](argumentSlotOffsetFieldName(argumentStateMapId), getArgumentSlotOffset(argumentStateMapId))) ++ codeGen.memoryTrackers.values

  override def genLocalVariables: Seq[LocalVariable] = {
    val seq = Seq.newBuilder[LocalVariable]
    if (shouldCheckOutputCounter) seq += OUTPUT_COUNTER
    if (shouldWriteToContext) seq += OUTPUT_CURSOR_VAR
    limits.foreach { (argumentStateMapId) =>
        seq += variable[Boolean](belowLimitVarName(argumentStateMapId), constant(true))
    }

    seq.result()
  }

  override def genCanContinue: Option[IntermediateRepresentation] = None

  override def genCloseCursors: IntermediateRepresentation = block()

  override protected def genProduce: IntermediateRepresentation = noop()

  override def genCloseOutput: IntermediateRepresentation = noop()

  override def genCloseInput: IntermediateRepresentation = noop()

  override def genCreateState: IntermediateRepresentation = {
    block(codeGen.memoryTrackers.map {
      case (id, field) => setMemoryTracker(field, id.x)
    }.toSeq :_*)
  }

  override def genOutputBuffer: Option[IntermediateRepresentation] = None

  private def getArgument(argumentStateMapId: ArgumentStateMapId) =
    invoke(INPUT_CURSOR, method[ReadableRow, Long, Int]("getLongAt"), loadField(field[Int](argumentSlotOffsetFieldName(argumentStateMapId))))

}
