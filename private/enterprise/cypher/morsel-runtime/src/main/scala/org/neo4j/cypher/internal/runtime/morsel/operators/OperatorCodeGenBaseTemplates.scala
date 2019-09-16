/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators


import java.util.concurrent.atomic.AtomicLong

import org.neo4j.codegen.TypeReference
import org.neo4j.codegen.api.CodeGeneration.{CodeGenerationMode, compileClass}
import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api._
import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, PipelineId}
import org.neo4j.cypher.internal.profiling.{OperatorProfileEvent, QueryProfiler}
import org.neo4j.cypher.internal.runtime.compiled.expressions.{ExpressionCompiler, IntermediateExpression}
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates._
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory, ArgumentStateMaps}
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentStateMap, MorselParallelizer, StateFactory}
import org.neo4j.cypher.internal.runtime.morsel.{ArgumentStateMapCreator, ExecutionState, OperatorExpressionCompiler}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.{KernelReadTracer, Read}
import org.neo4j.values.AnyValue

import scala.collection.mutable.ArrayBuffer


trait CompiledStreamingOperator extends StreamingOperator {
  /**
    * Initialize new tasks for this operator. This code path let's operators create
    * multiple output rows for each row in `inputMorsel`.
    */
  override protected def nextTasks(context: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    Array(compiledNextTask(context.transactionalContext.dataRead, inputMorsel.nextCopy, argumentStateMaps))
  }

  protected def compiledNextTask(dataRead: Read,
                                 inputMorsel: MorselExecutionContext,
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
                          owner = typeRefOf[CompiledStreamingOperator],
                          returnType = typeRefOf[ContinuableOperatorTaskWithMorsel],
                          parameters = Seq(param[Read]("dataRead"),
                                           param[MorselExecutionContext]("inputMorsel"),
                                           param[ArgumentStateMaps]("argumentStateMaps")),
                          body = newInstance(Constructor(TypeReference.typeReference(taskClazz),
                                                         Seq(TypeReference.typeReference(classOf[Read]),
                                                             TypeReference.typeReference(classOf[MorselExecutionContext]),
                                                             TypeReference.typeReference(classOf[ArgumentStateMaps]))),
                                             load("dataRead"),
                                             load("inputMorsel"),
                                             load("argumentStateMaps"))),
        MethodDeclaration("workIdentity",
                          owner = typeRefOf[CompiledStreamingOperator],
                          returnType = typeRefOf[WorkIdentity],
                          parameters = Seq.empty,
                          body = getStatic[WorkIdentity](workIdentityField.name)),
        MethodDeclaration("createState",
                          owner = typeRefOf[CompiledStreamingOperator],
                          returnType = typeRefOf[OperatorState],
                          parameters = Seq(param[ArgumentStateMapCreator]("argumentStateCreator"),
                                           param[StateFactory]("stateFactory"),
                                           param[QueryContext]("queryContext"),
                                           param[QueryState]("state"),
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
  override def operateWithProfile(output: MorselExecutionContext,
                                  context: QueryContext,
                                  state: QueryState,
                                  resources: QueryResources,
                                  queryProfiler: QueryProfiler): Unit = {
    initializeProfileEvents(queryProfiler)
    try {
      compiledOperate(output, context, state, resources, queryProfiler)
    } finally {
      closeProfileEvents(resources)
    }
  }

  /**
    * Method of [[OutputOperator]] trait. Implementing this allows the same [[CompiledTask]] instance to also act as an [[OutputOperatorState]].
    */
  override final def createState(executionState: ExecutionState,
                                 pipelineId: PipelineId): OutputOperatorState = {
    compiledCreateState(executionState, pipelineId.x)
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
  override final def prepareOutputWithProfile(output: MorselExecutionContext,
                                              context: QueryContext,
                                              state: QueryState,
                                              resources: QueryResources,
                                              queryProfiler: QueryProfiler): PreparedOutput = this

  /**
    * Method of [[OutputOperatorState]] trait. Implementing this allows the same [[CompiledTask]] instance to also act as a [[PreparedOutput]].
    */
  override protected final def prepareOutput(outputMorsel: MorselExecutionContext,
                                             context: QueryContext,
                                             state: QueryState,
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
  def compiledOperate(context: MorselExecutionContext,
                      dbAccess: QueryContext,
                      state: QueryState,
                      resources: QueryResources,
                      queryProfiler: QueryProfiler): Unit

  /**
    * Generated code that produces output into the execution state.
    */
  @throws[Exception]
  def compiledProduce(): Unit

  /**
    * Generated code that performs the initialization necessary for performing [[PreparedOutput.produce()]].
    * E.g., retrieving [[org.neo4j.cypher.internal.runtime.morsel.state.buffers.Sink]] from [[ExecutionState]].
    * Note, <code>pipelineId</code> parameter is [[Int]] because [[PipelineId]] extends [[AnyVal]]
    */
  @throws[Exception]
  def compiledCreateState(executionState: ExecutionState,
                          pipelineId: Int): Unit

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

  override def operate(output: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       resources: QueryResources): Unit = throw new IllegalStateException("Fused operators should be called via operateWithProfile.")
}

trait OperatorTaskTemplate {
  /**
    * The operator template that this template is wrapping around, or `null`.
    */
  def inner: OperatorTaskTemplate

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

  /**
    * ID of the Logical plan of this template.
    */
  def id: Id

  def genClassDeclaration(packageName: String, className: String, staticFields: Seq[StaticField]): ClassDeclaration[CompiledTask] = {
    throw new InternalException("Illegal start operator template")
  }

  def genInit: IntermediateRepresentation = noop()

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
      invokeSideEffect(loadField(field[OperatorProfileEvent]("operatorExecutionEvent_" + id.x)), method[OperatorProfileEvent, Unit]("close")),
      inner.genCloseProfileEvents
    )
  }

  // TODO: Make separate genOperate and genOperateSingleRow methods to clarify the distinction
  //       between streaming (loop over the whole output morsel) and stateless (processing of single row inlined into outer loop) usage
  /**
    * Responsible for generating the loop code of:
    * {{{
    *     def operate(context: MorselExecutionContext,
    *                 dbAccess: QueryContext,
    *                 state: QueryState,
    *                 resources: QueryResources,
    *                 queryProfiler: QueryProfiler): Unit
    * }}}
    */
  final def genOperateWithExpressions: IntermediateRepresentation = {
    val ir = genOperate
    val expressionCursors = genExpressions.flatMap(_.variables)
      .intersect(Seq(ExpressionCompiler.vNODE_CURSOR,
                     ExpressionCompiler.vPROPERTY_CURSOR,
                     ExpressionCompiler.vRELATIONSHIP_CURSOR))
    if (expressionCursors.nonEmpty) {
      val setTracerCalls = expressionCursors.map(cursor => invokeSideEffect(load(cursor), SET_TRACER, loadField(executionEventField)))
      block(
        setTracerCalls :+ ir:_*
      )
    } else {
      ir
    }
  }

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
    *     def createState(executionState: ExecutionState,
    *                     pipelineId: PipelineId): OutputOperatorState
    * }}}
    */
  protected def genCreateState: IntermediateRepresentation = inner.genCreateState

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
  }
}

trait ContinuableOperatorTaskWithMorselTemplate extends OperatorTaskTemplate {
  import IntermediateRepresentation._
  import OperatorCodeGenHelperTemplates._

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
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Unit],
                          parameters = Seq(param[QueryProfiler]("queryProfiler")),
                          body = genInitializeProfileEvents
        ),
        MethodDeclaration("compiledOperate",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Unit],
                          Seq(param[MorselExecutionContext]("context"),
                              param[QueryContext]("dbAccess"),
                              param[QueryState]("state"),
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
                              variable[Array[AnyValue]]("params",
                                                        invoke(QUERY_STATE,
                                                               method[QueryState, Array[AnyValue]]("params"))),
                              variable[ExpressionCursors]("cursors",
                                                          invoke(QUERY_RESOURCES,
                                                                 method[QueryResources, ExpressionCursors]("expressionCursors"))),
                              variable[Array[AnyValue]]("expressionVariables",
                                                        invoke(QUERY_RESOURCES,
                                                               method[QueryResources, Array[AnyValue], Int]("expressionVariables"),
                                                               invoke(QUERY_STATE,
                                                                      method[QueryState, Int]("nExpressionSlots")))
                              )) ++ flatMap[LocalVariable](op => op.genLocalVariables ++
                                                                 op.genExpressions.flatMap(_.variables))
                          },
                          parameterizedWith = None,
                          throws = Some(typeRefOf[Exception])),
        MethodDeclaration("compiledProduce",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Unit],
                          parameters = Seq.empty,
                          body = genProduce,
                          genLocalVariables = () => Seq.empty,
                          throws = Some(typeRefOf[Exception])),
        MethodDeclaration("compiledCreateState",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Unit],
                          Seq(param[ExecutionState]("executionState"),
                              param[Int]("pipelineId")
                          ),
                          body = genCreateState,
                          genLocalVariables = () => Seq.empty,
                          throws = Some(typeRefOf[Exception])),
        MethodDeclaration("compiledOutputBuffer",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Int],
                          parameters = Seq.empty,
                          body = genOutputBuffer.getOrElse(constant(-1))
        ),
        MethodDeclaration("closeProfileEvents",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Unit],
                          parameters = Seq(QUERY_RESOURCE_PARAMETER),
                          body = genCloseProfileEvents
        ),
        MethodDeclaration("canContinue",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Boolean],
                          parameters = Seq.empty,
                          body = genCanContinue.getOrElse(constant(false))
        ),
        MethodDeclaration("closeCursors",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Unit],
                          parameters = Seq(QUERY_RESOURCE_PARAMETER),
                          body = genCloseCursors,
                          genLocalVariables = () => Seq(CURSOR_POOL_V)
        ),
        MethodDeclaration("workIdentity",
          owner = typeRefOf[CompiledTask],
          returnType = typeRefOf[WorkIdentity],
          parameters = Seq.empty,
          body = getStatic[WorkIdentity](WORK_IDENTITY_STATIC_FIELD_NAME)
        ),
        // This is only needed because we extend an abstract scala class containing `val dataRead`
        MethodDeclaration("dataRead",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Read],
                          parameters = Seq.empty,
                          body = loadField(DATA_READ)
        ),
        // This is only needed because we extend an abstract scala class containing `val inputMorsel`
        MethodDeclaration("inputMorsel",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[MorselExecutionContext],
                          parameters = Seq.empty,
                          body = loadField(INPUT_MORSEL)
        )
      ),
      // NOTE: This has to be called after genOperate!
      genFields = () => staticFields ++ Seq(DATA_READ, INPUT_MORSEL) ++ flatMap[Field](op => op.genFields ++
                                                                                             op.genProfileEventField ++
                                                                                             op.genExpressions.flatMap(_.fields)))
  }
}

// Used for innermost, e.g. to insert the `outputRow.moveToNextRow` of the start operator at the deepest nesting level
// and also for providing demand operations
class DelegateOperatorTaskTemplate(var shouldWriteToContext: Boolean = true,
                                   var shouldCheckDemand: Boolean = false,
                                   var shouldCheckOutputCounter: Boolean = false)
                                  (codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  // Reset configuration to the default settings
  def reset(): Unit = {
    shouldWriteToContext = true
    shouldCheckDemand = false
    shouldCheckOutputCounter = false
  }

  override def inner: OperatorTaskTemplate = null

  override val id: Id = Id.INVALID_ID

  override def genInitializeProfileEvents: IntermediateRepresentation = noop()

  override def genProfileEventField: Option[Field] = None

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = noop()

  override def genCloseProfileEvents: IntermediateRepresentation = noop()

  override protected def genOperate: IntermediateRepresentation = {
    val ops = new ArrayBuffer[IntermediateRepresentation]

    if (shouldWriteToContext) {
      ops += block(codeGen.writeLocalsToSlots(), OUTPUT_ROW_MOVE_TO_NEXT)
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
        codeGen.locals.getAllLocalsForCachedProperties.map {
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

    and(conditions)
  }

  override def genOperateEnter: IntermediateRepresentation =
    noop()

  /**
    * If we need to care about demand (produceResult part of the fused operator)
    * we need to update the demand after having produced data.
    */
  override def genOperateExit: IntermediateRepresentation = {
    val updates = new ArrayBuffer[IntermediateRepresentation]

    if (shouldWriteToContext) {
      updates += OUTPUT_ROW_FINISHED_WRITING
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

  override def genFields: Seq[Field] = Seq.empty

  override def genLocalVariables: Seq[LocalVariable] = {
    val localsForSlots =
      codeGen.locals.getAllLocalsForLongSlots.map {
        case (_, name) =>
          variable[Long](name, constant(-1L))
      } ++
      codeGen.locals.getAllLocalsForRefSlots.map {
        case (_, name) =>
          variable[AnyValue](name, constant(null))
      }

    if (shouldCheckOutputCounter) {
      localsForSlots :+ OUTPUT_COUNTER
    } else {
      localsForSlots
    }
  }

  override def genCanContinue: Option[IntermediateRepresentation] = None

  override def genCloseCursors: IntermediateRepresentation = block()

  override protected def genProduce: IntermediateRepresentation = noop()

  override protected def genCreateState: IntermediateRepresentation = noop()

  override def genOutputBuffer: Option[IntermediateRepresentation] = None
}
