/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators


import java.util.concurrent.atomic.AtomicLong

import org.neo4j.codegen.api.CodeGeneration.compileClass
import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api._
import org.neo4j.cypher.internal.profiling.{OperatorProfileEvent, QueryProfiler}
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.ContinuableOperatorTaskWithMorselGenerator.CompiledTaskFactory
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates._
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.Read
import org.neo4j.values.AnyValue

class CompiledStreamingOperator(val workIdentity: WorkIdentity,
                                val taskFactory: CompiledTaskFactory) extends StreamingOperator {
  /**
    * Initialize new tasks for this operator. This code path let's operators create
    * multiple output rows for each row in `inputMorsel`.
    */
  override protected def nextTasks(context: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    taskFactory(context.transactionalContext.dataRead, inputMorsel)
  }
}

object ContinuableOperatorTaskWithMorselGenerator {
  private val PACKAGE_NAME = "org.neo4j.codegen"
  private val COUNTER = new AtomicLong(0)
  private def className(): String = "Operator" + COUNTER.getAndIncrement()

  type CompiledTaskFactory = (Read, MorselParallelizer) => IndexedSeq[ContinuableOperatorTaskWithMorsel]

  /**
    * Responsible for generating a tailored class and creates a factory for creating new instances of the class
    */
  def compileOperator(template: ContinuableOperatorTaskWithMorselTemplate): CompiledTaskFactory = {
    val clazz = compileClass(template.genClassDeclaration(PACKAGE_NAME, className()))
    val constructor = clazz.getDeclaredConstructor(classOf[Read], classOf[MorselExecutionContext])

    (dataRead, inputMorsel) => {
      IndexedSeq(constructor.newInstance(dataRead, inputMorsel.nextCopy))
    }
  }

}

/**
  * We need to specialize this because we don't have support for compiling try-finally.
  */
trait CompiledTask extends ContinuableOperatorTaskWithMorsel {
  override def operateWithProfile(output: MorselExecutionContext,
                                  context: QueryContext,
                                  state: QueryState,
                                  resources: QueryResources,
                                  queryProfiler: QueryProfiler): Unit = {
    initializeProfileEvents(queryProfiler)
    try {
      compiledOperate(output, context, state, resources, queryProfiler)
    } finally {
      closeProfileEvents()
    }
  }

  /**
    * Generated code that initializes the profile events.
    */
  def initializeProfileEvents(queryProfiler: QueryProfiler): Unit

  /**
    * Generated code that executes the operator.
    */
  @throws[Exception]
  def compiledOperate(output: MorselExecutionContext,
                      context: QueryContext,
                      state: QueryState,
                      resources: QueryResources,
                      queryProfiler: QueryProfiler): Unit

  /**
    * Generated code that closes all events for profiling.
    */
  def closeProfileEvents(): Unit

  override def operate(output: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       resources: QueryResources): Unit = throw new IllegalStateException("Fused operators should be called via operateWithProfile.")

  override def workIdentity: WorkIdentity = throw new IllegalStateException("Fused operators do not have a single WorkIdentity.")

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

  def genClassDeclaration(packageName: String, className: String): ClassDeclaration[CompiledTask] = {
    throw new InternalException("Illegal start operator template")
  }

  def genInit: IntermediateRepresentation = noop()

  def genProfileEventFields: Seq[Field] = {
    field[OperatorProfileEvent]("operatorExecutionEvent_" + id.x) +: inner.genProfileEventFields
  }

  def genInitializeProfileEvents: IntermediateRepresentation = {
    block(
      setField(field[OperatorProfileEvent]("operatorExecutionEvent_" + id.x),
        invoke(QUERY_PROFILER, method[QueryProfiler, OperatorProfileEvent, Int, Boolean]("executeOperator"), constant(id.x), constant(false))),
      inner.genInitializeProfileEvents)
  }

  def genCloseProfileEvents: IntermediateRepresentation = {
    block(
      invokeSideEffect(loadField(field[OperatorProfileEvent]("operatorExecutionEvent_" + id.x)), method[OperatorProfileEvent, Unit]("close")),
      inner.genCloseProfileEvents
    )
  }

  // TODO: Make separate genOperate and genOperateSingleRow methods to clarify the distinction
  //       between streaming (loop over the whole output morsel) and stateless (processing of single row inlined into outer loop) usage
  /**
    * Responsible for generating:
    * {{{
    *     def operate(output: MorselExecutionContext,
    *                 context: QueryContext,
    *                 state: QueryState,
    *                 resources: QueryResources,
    *                 queryProfiler: QueryProfiler): Unit
    * }}}
    */
  def genOperate: IntermediateRepresentation

  // TODO: Create implementations of these in the base class that handles the recursive inner.genFields logic etc.?
  def genFields: Seq[Field]
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
    override def genFields: Seq[Field] = Seq.empty
    override def genLocalVariables: Seq[LocalVariable] = Seq.empty
    override def genCanContinue: Option[IntermediateRepresentation] = None
    override def genCloseCursors: IntermediateRepresentation = noop()
  }
}

trait ContinuableOperatorTaskWithMorselTemplate extends OperatorTaskTemplate {
  import IntermediateRepresentation._
  import OperatorCodeGenHelperTemplates._

  // We let the generated class extend the abstract class CompiledContinuableOperatorTaskWithMorsel(which extends ContinuableOperatorTaskWithMorsel),
  // which implements the close() and produceWorkUnit() methods from the ContinuableOperatorTask

  // TODO: Use methods of actual interface to generate declaration?
  override def genClassDeclaration(packageName: String, className: String): ClassDeclaration[CompiledTask] = {

    ClassDeclaration[CompiledTask](packageName, className,
                                                        extendsClass = None,
                                                        implementsInterfaces =  Seq(typeRefOf[CompiledTask]),
                                                        constructorParameters = Seq(DATA_READ_CONSTRUCTOR_PARAMETER,
                                                                                    INPUT_MORSEL_CONSTRUCTOR_PARAMETER),
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
                          body = genOperate,
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
                                                                      method[QueryState, Int]("nExpressionSlots"))
                                                        ))) ++ genLocalVariables
                          },
                          parameterizedWith = None, throws = Some(typeRefOf[Exception])
        ),
        MethodDeclaration("closeProfileEvents",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Unit],
                          parameters = Seq.empty,
                          body = genCloseProfileEvents
        ),
        MethodDeclaration("canContinue",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Boolean],
                          parameters = Seq.empty,
                          body = genCanContinue.getOrElse(falseValue)
        ),
        MethodDeclaration("closeCursors",
                          owner = typeRefOf[CompiledTask],
                          returnType = typeRefOf[Unit],
                          parameters = Seq(QUERY_RESOURCE_PARAMETER),
                          body = genCloseCursors,
                          genLocalVariables = () => Seq(CURSOR_POOL_V)
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
      genFields = () => Seq(DATA_READ, INPUT_MORSEL) ++ genFields ++ genProfileEventFields) // NOTE: This has to be called after genOperate!
  }
}

// Used for innermost, e.g. to insert the `outputRow.moveToNextRow` of the start operator at the deepest nesting level
// and also for providing demand operations
class DelegateOperatorTaskTemplate(var shouldWriteToContext: Boolean = true)
                                  (codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  override def inner: OperatorTaskTemplate = null

  override val id: Id = Id.INVALID_ID

  override def genInitializeProfileEvents: IntermediateRepresentation = noop()

  override def genProfileEventFields: Seq[Field] = Seq.empty

  override def genCloseProfileEvents: IntermediateRepresentation = noop()

  override def genOperate: IntermediateRepresentation = {
    if (shouldWriteToContext) {
      block(
        codeGen.writeLocalsToSlots(),
        OUTPUT_ROW_MOVE_TO_NEXT
      )
    } else {
      noop()
    }
  }

  def predicate: IntermediateRepresentation = if (shouldWriteToContext) OUTPUT_ROW_IS_VALID else HAS_DEMAND

  /**
    * If we need to care about demand (produceResult part of the fused operator)
    * we need to update the demand after having produced data.
    */
  def onExit: IntermediateRepresentation =
    if (shouldWriteToContext) OUTPUT_ROW_FINISHED_WRITING
    else UPDATE_DEMAND

  override def genFields: Seq[Field] = Seq.empty

  override def genLocalVariables: Seq[LocalVariable] = {
    codeGen.locals.getAllLocalsForLongSlots.map {
      case (_, name) =>
        variable[Long](name, constant(-1L))
    } ++
    codeGen.locals.getAllLocalsForRefSlots.map {
      case (_, name) =>
        variable[AnyValue](name, noValue)
    }
  }

  override def genCanContinue: Option[IntermediateRepresentation] = None

  override def genCloseCursors: IntermediateRepresentation = block()
}
