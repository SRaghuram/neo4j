/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateRepresentation._
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.operators.ContinuableOperatorTaskWithMorselGenerator.CompiledTaskFactory
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.{DbAccess, ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.internal.kernel.api.{NodeCursor, Read}
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
    * This is responsible for generating a class for the
    */
  def generateClassAndTaskFactory(template: ContinuableOperatorTaskWithMorselTemplate): CompiledTaskFactory = {
    val clazz = generateClass(template)
    val constructor = clazz.getDeclaredConstructor(classOf[Read], classOf[MorselExecutionContext])
    (dataRead: org.neo4j.internal.kernel.api.Read, inputMorsel: MorselParallelizer) => {
      IndexedSeq(constructor.newInstance(dataRead, inputMorsel.nextCopy).asInstanceOf[ContinuableOperatorTaskWithMorsel])
    }
  }

  private def generateClass(template: ContinuableOperatorTaskWithMorselTemplate): Class[_] =
    CodeGeneration.compileClass(template.genClassDeclaration(PACKAGE_NAME, className()))
}

trait OperatorTaskTemplate {
  def genClassDeclaration(packageName: String, className: String): ClassDeclaration = {
    throw new InternalException("Illegal start operator template")
  }

  def genInit: IntermediateRepresentation = noop

  //def operate(output: MorselExecutionContext, context: QueryContext, state: QueryState, resources: QueryResources): Unit
  // TODO: Make separate genOperate and genOperateSingleRow methods to clarify the distinction
  //       between streaming (loop over the whole output morsel) and stateless (processing of single row inlined into outer loop) usage
  def genOperate: IntermediateRepresentation

  // TODO: Create implementations of these in the base class that handles the recursive inner.genFields logic etc.?
  def genFields: Seq[Field]
  def genLocalVariables: Seq[LocalVariable]
}

trait ContinuableOperatorTaskTemplate extends OperatorTaskTemplate {
  //override def canContinue: Boolean
  def genCanContinue: IntermediateRepresentation
}

trait ContinuableOperatorTaskWithMorselTemplate extends ContinuableOperatorTaskTemplate {
  import OperatorCodeGenHelperTemplates._

  // We let the generated class extend the abstract class CompiledContinuableOperatorTaskWithMorsel(which extends ContinuableOperatorTaskWithMorsel),
  // which implements the close() and produceWorkUnit() methods from the ContinuableOperatorTask

  // TODO: Use methods of actual interface to generate declaration?
  override def genClassDeclaration(packageName: String, className: String): ClassDeclaration = {
    val fields = genFields
    val localVariables = genLocalVariables

    ClassDeclaration(packageName, className,
      extendsClass = Some(typeRefOf[CompiledContinuableOperatorTaskWithMorsel]),
      implementsInterfaces = Seq.empty,
      constructorParameters = Seq(DATA_READ_CONSTRUCTOR_PARAMETER, INPUT_MORSEL_CONSTRUCTOR_PARAMETER),
      initializationCode = genInit,
      fields = fields,
      methods = Seq(
        MethodDeclaration("operateCompiled",
          owner = typeRefOf[CompiledContinuableOperatorTaskWithMorsel],
          returnType = typeRefOf[Unit],
          Seq(param[MorselExecutionContext]("context"),
              param[DbAccess]("dbAccess"),
              param[Array[AnyValue]]("params"),
              param[ExpressionCursors]("cursors"),
              param[Array[AnyValue]]("expressionVariables"),
              param[CursorPool[NodeCursor]]("nodeCursorPool"),
              param("resultVisitor", parameterizedType(typeRefOf[QueryResultVisitor[_]], typeParam("E")))
          ),
          body = genOperate,
          localVariables = localVariables,
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
      )
    )
  }
}

// Used for innermost, e.g. to insert the `outputRow.moveToNextRow` of the start operator at the deepest nesting level
class DelegateOperatorTaskTemplate(var delegate: OperatorTaskTemplate = null) extends OperatorTaskTemplate {
  override def genOperate: IntermediateRepresentation = {
    delegate.genOperate
  }

  override def genFields: Seq[Field] = delegate.genFields
  override def genLocalVariables: Seq[LocalVariable] = delegate.genLocalVariables
}
