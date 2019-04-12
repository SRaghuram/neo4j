/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators


import org.neo4j.codegen.api.CodeGeneration.compileClass
import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api._
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.zombie.operators.ContinuableOperatorTaskWithMorselGenerator.CompiledTaskFactory
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.{DbAccess, ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
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
                                   resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    taskFactory(context.transactionalContext.dataRead, inputMorsel)
  }
}

object ContinuableOperatorTaskWithMorselGenerator {
  private val PACKAGE_NAME = "org.neo4j.codegen"
  private def className(): String = "Operator" + System.nanoTime()

  type CompiledTaskFactory = (Read, MorselParallelizer) => IndexedSeq[ContinuableOperatorTaskWithMorsel]

  /**
    * Responsible for generating a tailored class and creates a factory for creating new instances of the class
    */
  def generateClassAndTaskFactory(template: ContinuableOperatorTaskWithMorselTemplate): CompiledTaskFactory = {
    val clazz = compileClass(template.genClassDeclaration(PACKAGE_NAME, className()))
    val constructor = clazz.getDeclaredConstructor(classOf[Read], classOf[MorselExecutionContext])

    (dataRead, inputMorsel) => {
      IndexedSeq(constructor.newInstance(dataRead, inputMorsel.nextCopy))
    }
  }

}

trait OperatorTaskTemplate {
  def genClassDeclaration(packageName: String, className: String): ClassDeclaration[ContinuableOperatorTaskWithMorsel] = {
    throw new InternalException("Illegal start operator template")
  }

  def genInit: IntermediateRepresentation = noop()

  // TODO: Make separate genOperate and genOperateSingleRow methods to clarify the distinction
  //       between streaming (loop over the whole output morsel) and stateless (processing of single row inlined into outer loop) usage
  /**
    * Responsible for generating:
    * {{{
    *     def operate(output: MorselExecutionContext,
    *                 context: QueryContext,
    *                 state: QueryState, resources: QueryResources): Unit
    * }}}
    */
  def genOperate: IntermediateRepresentation

  // TODO: Create implementations of these in the base class that handles the recursive inner.genFields logic etc.?
  def genFields: Seq[Field]
  def genLocalVariables: Seq[LocalVariable]
}

trait ContinuableOperatorTaskTemplate extends OperatorTaskTemplate {

  /**
    * Responsible for generating:
    * {{{
    *   override def canContinue: Boolean
    * }}}
    */
  def genCanContinue: IntermediateRepresentation
}

trait ContinuableOperatorTaskWithMorselTemplate extends ContinuableOperatorTaskTemplate {
  import OperatorCodeGenHelperTemplates._

  // We let the generated class extend the abstract class CompiledContinuableOperatorTaskWithMorsel(which extends ContinuableOperatorTaskWithMorsel),
  // which implements the close() and produceWorkUnit() methods from the ContinuableOperatorTask

  // TODO: Use methods of actual interface to generate declaration?
  override def genClassDeclaration(packageName: String, className: String): ClassDeclaration[ContinuableOperatorTaskWithMorsel] = {

    ClassDeclaration[ContinuableOperatorTaskWithMorsel](packageName, className,
      extendsClass = Some(typeRefOf[CompiledContinuableOperatorTaskWithMorsel]),
      implementsInterfaces = Seq.empty,
      constructorParameters = Seq(DATA_READ_CONSTRUCTOR_PARAMETER, INPUT_MORSEL_CONSTRUCTOR_PARAMETER),
      initializationCode = genInit,
      methods = Seq(
        MethodDeclaration("operateCompiled",
          owner = typeRefOf[CompiledContinuableOperatorTaskWithMorsel],
          returnType = typeRefOf[Unit],
          Seq(param[MorselExecutionContext]("context"),
              param[DbAccess]("dbAccess"),
              param[Array[AnyValue]]("params"),
              param[ExpressionCursors]("cursors"),
              param[Array[AnyValue]]("expressionVariables"),
              param[CursorPools]("cursorPools"),
              param("resultVisitor", parameterizedType(typeRefOf[QueryResultVisitor[_]], typeParam("E")))
          ),
          body = genOperate,
          genLocalVariables = () => genLocalVariables, // NOTE: This has to be called after genOperate!
          parameterizedWith = Some(("E", extending[Exception])),
          throws = Some(typeParam("E"))
        ),
        MethodDeclaration("canContinue",
          owner = typeRefOf[CompiledContinuableOperatorTaskWithMorsel],
          returnType = typeRefOf[Boolean],
          parameters = Seq.empty,
          body = genCanContinue
        ),
        // This is only needed because we extend an abstract scala class containing `val dataRead`
        MethodDeclaration("dataRead",
          owner = typeRefOf[CompiledContinuableOperatorTaskWithMorsel],
          returnType = typeRefOf[Read],
          parameters = Seq.empty,
          body = loadField(DATA_READ)
        ),
        // This is only needed because we extend an abstract scala class containing `val inputMorsel`
        MethodDeclaration("inputMorsel",
          owner = typeRefOf[CompiledContinuableOperatorTaskWithMorsel],
          returnType = typeRefOf[MorselExecutionContext],
          parameters = Seq.empty,
          body = loadField(INPUT_MORSEL)
        )
      ), genFields = () => genFields)// NOTE: This has to be called after genOperate!
  }
}

// Used for innermost, e.g. to insert the `outputRow.moveToNextRow` of the start operator at the deepest nesting level
class DelegateOperatorTaskTemplate(var delegate: OperatorTaskTemplate = null,
                                   var shouldWriteToContext: Boolean = true)
                                  (codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  override def genOperate: IntermediateRepresentation = {
    if (shouldWriteToContext) {
      block(
        codeGen.writeLocalsToSlots(),
        delegate.genOperate
      )
    } else {
      delegate.genOperate
    }
  }

  override def genFields: Seq[Field] = delegate.genFields

  override def genLocalVariables: Seq[LocalVariable] = {
    codeGen.locals.getAllLocalsForLongSlots.map {
      case (_, name) =>
        variable[Long](name, constant(-1L))
    } ++
    codeGen.locals.getAllLocalsForRefSlots.map {
      case (_, name) =>
        variable[AnyValue](name, noValue)
    } ++
    delegate.genLocalVariables
  }
}
