/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators


import java.util.concurrent.atomic.AtomicLong

import org.neo4j.codegen.api.CodeGeneration.compileClass
import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api._
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.zombie.operators.ContinuableOperatorTaskWithMorselGenerator.CompiledTaskFactory
import org.neo4j.cypher.internal.runtime.zombie.operators.OperatorCodeGenHelperTemplates._
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.InternalException
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
  import IntermediateRepresentation._
  import OperatorCodeGenHelperTemplates._

  // We let the generated class extend the abstract class CompiledContinuableOperatorTaskWithMorsel(which extends ContinuableOperatorTaskWithMorsel),
  // which implements the close() and produceWorkUnit() methods from the ContinuableOperatorTask

  // TODO: Use methods of actual interface to generate declaration?
  override def genClassDeclaration(packageName: String, className: String): ClassDeclaration[ContinuableOperatorTaskWithMorsel] = {

    ClassDeclaration[ContinuableOperatorTaskWithMorsel](packageName, className,
                                                        extendsClass = None,
                                                        implementsInterfaces =  Seq(typeRefOf[ContinuableOperatorTaskWithMorsel]),
                                                        constructorParameters = Seq(DATA_READ_CONSTRUCTOR_PARAMETER, INPUT_MORSEL_CONSTRUCTOR_PARAMETER),
                                                        initializationCode = genInit,
                                                        methods = Seq(
                                                          MethodDeclaration("operate",
          owner = typeRefOf[ContinuableOperatorTaskWithMorsel],
          returnType = typeRefOf[Unit],
          Seq(param[MorselExecutionContext]("context"),
              param[QueryContext]("dbAccess"),
              param[QueryState]("state"),
              param[QueryResources]("resources")
          ),
          body = genOperate,
          genLocalVariables = () => {
            Seq(
              variable[Array[AnyValue]]("params",
                                 invoke(QUERY_STATE, method[QueryState, Array[AnyValue]]("params"))),
              variable[ExpressionCursors]("cursors",
                                          invoke(QUERY_RESOURCES,
                                                 method[QueryResources, ExpressionCursors]("expressionCursors"))),
              variable[Array[AnyValue]]("expressionVariables",
                                        invoke(QUERY_RESOURCES,
                                               method[QueryResources, Array[AnyValue], Int]("expressionVariables"),
                                               invoke(QUERY_STATE, method[QueryState, Int]("nExpressionSlots"))
                                               ))) ++ genLocalVariables},
                          parameterizedWith = None, throws = Some(typeRefOf[Exception])
        ),
                                                          MethodDeclaration("canContinue",
                                                                            owner = typeRefOf[ContinuableOperatorTaskWithMorsel],
                                                                            returnType = typeRefOf[Boolean],
                                                                            parameters = Seq.empty,
                                                                            body = genCanContinue
                                                                            ),
                                                          // This is only needed because we extend an abstract scala class containing `val dataRead`
                                                          MethodDeclaration("dataRead",
          owner = typeRefOf[ContinuableOperatorTaskWithMorsel],
          returnType = typeRefOf[Read],
          parameters = Seq.empty,
          body = loadField(DATA_READ)
        ),
                                                          // This is only needed because we extend an abstract scala class containing `val inputMorsel`
                                                          MethodDeclaration("inputMorsel",
          owner = typeRefOf[ContinuableOperatorTaskWithMorsel],
          returnType = typeRefOf[MorselExecutionContext],
          parameters = Seq.empty,
          body = loadField(INPUT_MORSEL)
        )
                                                          ), genFields = () => Seq(DATA_READ,
                                                                                   INPUT_MORSEL) ++ genFields) // NOTE: This has to be called after genOperate!
  }
}

/**
  * Contains two components, one `predicate` to be used as the condition of a loop and one call to be made at the end of the loop.
  *
  * @param predicate The check to be done in the condition to the loop, e.g. `while(predicate)`
  * @param endOfLoop To be a called at the end of loop, allows the predicate to be updated, e.g. `predicate = cursor.next()`
  */
case class DemandPredicate(predicate: IntermediateRepresentation, endOfLoop: IntermediateRepresentation)

// Used for innermost, e.g. to insert the `outputRow.moveToNextRow` of the start operator at the deepest nesting level
class DelegateOperatorTaskTemplate(var delegate: OperatorTaskTemplate = null,
                                   var shouldWriteToContext: Boolean = true,
                                   var shouldCheckDemand: Boolean = false)
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

  /**
    * In the case where we need to check demand, i.e. produceResult is part of the fused operator, we need to generate
    * loops of the following form.
    *
    * {{{
    *   if (!canContinue) {
    *     canContinue = cursor.next()
    *   }
    *   while (served < demand && input.isValidRow() && canContinue) {
    *     ...
    *     served += 1
    *     canContinue = cursor.next()
    *   }
    * }}}
    *
    * For the other case where we can ignore demand we simply generate loops of the form
    *
    * {{{
    *   while (input.isValidRow() && cursor.next()) {
    *     ...
    *   }
    * }}}
    * @param statefulPredicate   typically a predicate like `cursor.next`
    * @param statelessPredicates idempotent predicates.
    * @return
    */
  def checkDemand(statefulPredicate: IntermediateRepresentation,
                  statelessPredicates: IntermediateRepresentation*): DemandPredicate = {
    if (shouldCheckDemand) {
      DemandPredicate(
        /**
          * {{{
          *   if (!canContinue) {
          *     canContinue = stateFulPredicate
          *   }
          *   while( served < demand && p1 && p2 && ... && canContinue)
          * }}}
          */
        block(
          condition(not(loadField(CAN_CONTINUE)))(setField(CAN_CONTINUE, statefulPredicate)),
          and(HAS_DEMAND +: statelessPredicates :+ loadField(CAN_CONTINUE))),
        setField(CAN_CONTINUE, statefulPredicate))
    }
    // we don't need to care about demand, just push through the data
    else DemandPredicate(and(statelessPredicates :+ statefulPredicate), noop())
  }

  /**
    * If we need to care about demand (produceResult part of the fused operator)
    * we need to update the demand after having produced data.
    */
  def updatedDemand: IntermediateRepresentation =
    if (shouldCheckDemand) UPDATE_DEMAND
    else noop()

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
