/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physicalplanning.{PhysicalPlan, Pipeline, SlotConfiguration}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateRepresentation._
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.morsel.{CursorPool, MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.ContinuableOperatorTaskWithMorselGenerator.CompiledTaskFactory
import org.neo4j.cypher.internal.runtime.zombie.operators._
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.internal.kernel.api.{Cursor, NodeCursor, Read}
import org.neo4j.values.AnyValue

class FuseOperators(operatorFactory: OperatorFactory,
                    physicalPlan: PhysicalPlan,
                    converters: ExpressionConverters,
                    readOnly: Boolean,
                    queryIndexes: QueryIndexes) {

  def compilePipeline(p: Pipeline): Option[ExecutablePipeline] = {
    // First, try to fuse as many middle operators as possible into the head operator
    val (maybeHeadOperator, unhandledMiddlePlans, unhandledProduceResult) = fuseOperators(p.headPlan, p.middlePlans, p.produceResults)

    val headOperator = maybeHeadOperator.getOrElse(operatorFactory.create(p.headPlan))
    val middleOperators = unhandledMiddlePlans.flatMap(operatorFactory.createMiddle)
    val produceResultOperator = unhandledProduceResult.map(operatorFactory.createProduceResults)
    Some(ExecutablePipeline(p.id,
      headOperator,
      middleOperators,
      produceResultOperator,
      p.serial,
      physicalPlan.slotConfigurations(p.headPlan.id),
      p.inputBuffer,
      p.outputBuffer))
  }

  private def fuseOperators(headPlan: LogicalPlan, middlePlans: Seq[LogicalPlan], produceResult: Option[ProduceResult]): (Option[Operator], Seq[LogicalPlan], Option[ProduceResult]) = {
    val id = headPlan.id
    val slots = physicalPlan.slotConfigurations(id)
    val expressionCompiler = new IntermediateCodeGeneration(slots) // NOTE: We assume slots is the same within an entire pipeline
    generateSlotAccessorFunctions(slots)

    // Fold plans in reverse to build-up code generation templates with inner templates
    // Then generate create the taskFactory from the headPlan template
    // Some stateless operators cannot be fused, e.g. SortPreOperator
    // Return these to be built as separate operators

    // E.g. HeadPlan, Seq(MiddlePlan1, MiddlePlan2)
    // MiddlePlan2 -> Template1(innermostTemplate)
    // MiddlePlan1 -> Template2(inner=Template1)
    // HeadPlan    -> Template3(inner=Template2)
    val innermostTemplate = new DelegateOperatorTaskTemplate

    val innerTemplate = produceResult.map(p => new ProduceResultOperatorTaskTemplate(innermostTemplate, p.columns, slots)).getOrElse(innermostTemplate)
    val reversePlans = middlePlans.foldLeft(List(headPlan))((acc, p) => p :: acc)

    //noinspection VariablePatternShadow
    val (operatorTaskTemplate, fusedPlans, unhandledPlans, unhandledProduceResult) =
      reversePlans.foldLeft((innerTemplate: OperatorTaskTemplate, produceResult.toList: List[LogicalPlan], List.empty[LogicalPlan], None: Option[ProduceResult])) {
        case ((innerTemplate, fusedPlans, unhandledPlans, unhandledProduceResult), p) => p match {
          case plans.AllNodesScan(nodeVariableName, _) =>
            val argumentSize = physicalPlan.argumentSizes(id)
            (new SerialAllNodeScanTemplate(innerTemplate, innermostTemplate, nodeVariableName, slots.getLongOffsetFor(nodeVariableName), argumentSize),
              p :: fusedPlans, unhandledPlans, unhandledProduceResult)

          case plans.Selection(predicate, _) =>
            val compiledPredicate: IntermediateExpression = expressionCompiler.compileExpression(predicate).getOrElse(
              return (None, middlePlans, unhandledProduceResult)
            )
            (new FilterOperatorTemplate(innerTemplate, compiledPredicate), p :: fusedPlans, unhandledPlans, unhandledProduceResult)

          case _ =>
            // We cannot handle this plan. Start over from scratch (discard any previously fused plans)
            (innermostTemplate, List.empty, p :: unhandledPlans, produceResult)
        }
      }

    // Did we find any sequence of operators that we can fuse with the headPlan?
    if (fusedPlans.length < 1 /* TODO: This should be 2, but we allow 1 for debugging */) {
      (None, middlePlans, produceResult)
    }
    else {
      // Yes! Generate a class and an operator with a task factory that produces tasks based on the generated class
      println(s"@@@ Fused plans $fusedPlans") // TODO: Disable debug print

      val workIdentity = WorkIdentity.fromFusedPlans(fusedPlans)
      val operatorTaskWithMorselTemplate = operatorTaskTemplate.asInstanceOf[ContinuableOperatorTaskWithMorselTemplate]

      val taskFactory = ContinuableOperatorTaskWithMorselGenerator.generateClassAndTaskFactory(operatorTaskWithMorselTemplate)
      (Some(new CompiledStreamingOperator(workIdentity, taskFactory)), unhandledPlans, unhandledProduceResult)
    }
  }
}


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
  // TODO: Make separate genOperate and genOperateSingleRow methods to distinguish
  // streaming (loop over the whole output morsel) and stateless (processing a single row inlined into outer loop) usage
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
  import OperatorCodeGenTemplates._

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
              param[QueryResultVisitor[_ <: Exception]]("resultVisitor")
              //param[QueryResultVisitor[_]]("resultVisitor")
          ),
          body = genOperate,
          localVariables
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

// Used for innermost
class DelegateOperatorTaskTemplate(var delegate: OperatorTaskTemplate = null) extends OperatorTaskTemplate {
  override def genOperate: IntermediateRepresentation = {
    delegate.genOperate
  }

  override def genFields: Seq[Field] = delegate.genFields
  override def genLocalVariables: Seq[LocalVariable] = delegate.genLocalVariables
}

class FilterOperatorTemplate(val inner: OperatorTaskTemplate, predicate: IntermediateExpression) extends OperatorTaskTemplate {
  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genOperate: IntermediateRepresentation = {
    condition(equal(nullCheck(predicate)(predicate.ir), trueValue)) (
      inner.genOperate
    )
  }

  override def genLocalVariables: Seq[LocalVariable] = {
    predicate.variables ++ inner.genLocalVariables
  }

  override def genFields: Seq[Field] = {
    predicate.fields ++ inner.genFields
  }
}

abstract class InputLoopTaskTemplate extends ContinuableOperatorTaskWithMorselTemplate {
  import OperatorCodeGenTemplates._

  //override val inputMorsel: MorselExecutionContext
  override def genFields: Seq[Field] = {
    Seq(DATA_READ, INPUT_MORSEL, INNER_LOOP)
  }

  //override def canContinue: Boolean
  override def genCanContinue: IntermediateRepresentation = {
    //inputMorsel.isValidRow || innerLoop
    or(INPUT_ROW_IS_VALID, loadField(INNER_LOOP))
  }

  //def operate(output: MorselExecutionContext, context: QueryContext, state: QueryState, resources: QueryResources): Unit
  override def genOperate(): IntermediateRepresentation = {
//    while ((inputMorsel.isValidRow || innerLoop) && outputRow.isValidRow) {
//      if (!innerLoop) {
//        innerLoop = initializeInnerLoop(context, state, resources) <<< genInitializeInnerLoop
//      }
//      // Do we have any output rows for this input row?
//      if (innerLoop) {
//        // Implementor is responsible for advancing both `outputRow` and `innerLoop`.
//        // Typically the loop will look like this:
//        //        while (outputRow.hasMoreRows && cursor.next()) {
//        //          ... // Copy argumentSize #columns from inputRow to outputRow
//        //          ... // Write additional columns to outputRow
//        //          outputRow.moveToNextRow()
//        //        }
//        // The reason the loop itself is not already coded here is to avoid too many fine-grained virtual calls
//        innerLoop(outputRow, context, state) <<< genInnerLoop
//
//        // If we have not filled the output rows, move to the next input row
//        if (outputRow.isValidRow) {
//          // NOTE: There is a small chance that we run out of output rows and innerLoop iterations simultaneously where we would generate
//          // an additional empty work unit that will just close the innerLoop. This could be avoided if we changed the innerLoop interface to something
//          // slightly more complicated, but since innerLoop iterations and output morsel size will have to match exactly for this to happen it is
//          // probably not a big problem in practice, and the additional checks required may not be worthwhile.
//          closeInnerLoop(resources) <<< genCloseInnerLoop
//          innerLoop = false
//          inputMorsel.moveToNextRow()
//        }
//      }
//      else {
//        // Nothing to do for this input row, move to the next
//        inputMorsel.moveToNextRow()
//      }
//    }
//
//    outputRow.finishedWriting()
    block(
      loop(
        and(
          or(INPUT_ROW_IS_VALID, loadField(INNER_LOOP)),
          OUTPUT_ROW_IS_VALID
        )
      )(
        block(
          condition(not(loadField(INNER_LOOP)))(setField(INNER_LOOP, genInitializeInnerLoop)),
          ifElse(loadField(INNER_LOOP))(
            block(
              genInnerLoop,
              condition(OUTPUT_ROW_IS_VALID)(
                block(
                  genCloseInnerLoop,
                  setField(INNER_LOOP, constant(false)),
                  INPUT_ROW_MOVE_TO_NEXT
                )
              )
            )
          )( //else
            INPUT_ROW_MOVE_TO_NEXT
          )
        )
      ),
      OUTPUT_ROW_FINISHED_WRITING
    )
  }

  //protected def initializeInnerLoop(context: QueryContext, state: QueryState, resources: QueryResources): Boolean
  protected def genInitializeInnerLoop: IntermediateRepresentation

  /**
    * Execute the inner loop for the current input row, and write results to the output.
    */
  //protected def innerLoop(outputRow: MorselExecutionContext,
  //                        context: QueryContext,
  //                        state: QueryState): Unit
  protected def genInnerLoop: IntermediateRepresentation

  /**
    * Close any resources used by the inner loop.
    */
  //protected def closeInnerLoop(resources: QueryResources): Unit
  protected def genCloseInnerLoop: IntermediateRepresentation
}

class SerialAllNodeScanTemplate(val inner: OperatorTaskTemplate,
                                val innermost: DelegateOperatorTaskTemplate,
                                val nodeVarName: String,
                                val offset: Int,
                                val argumentSize: SlotConfiguration.Size) extends InputLoopTaskTemplate {
  import OperatorCodeGenTemplates._

  // Setup the innermost output template
  innermost.delegate = new OperatorTaskTemplate {
    override def genOperate: IntermediateRepresentation = {
      OUTPUT_ROW_MOVE_TO_NEXT
    }
    override def genFields: Seq[Field] = Seq.empty
    override def genLocalVariables: Seq[LocalVariable] = Seq.empty
  }

  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genFields: Seq[Field] = {
    (super.genFields :+ CURSOR) ++ inner.genFields
  }

  override def genLocalVariables: Seq[LocalVariable] = {
    inner.genLocalVariables
  }

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    //cursor = resources.cursorPools.nodeCursorPool.allocate()
    //context.transactionalContext.dataRead.allNodesScan(cursor)
    //true

    block(
      setField(CURSOR, ALLOCATE_NODE_CURSOR),
      ALL_NODE_SCAN,
      constant(true)
    )
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    //while (outputRow.isValidRow && cursor.next()) {
    //  outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
    //  outputRow.setLongAt(offset, cursor.nodeReference())
    //  <<< inner.genOperate() >>>
    //  //outputRow.moveToNextRow() // <- This needs to move to the innermost level
    //}
    loop(and(OUTPUT_ROW_IS_VALID, invoke(loadField(CURSOR), method[NodeCursor, Boolean]("next"))))(
      block(
        invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit, ExecutionContext, Int, Int]("copyFrom"),
          loadField(INPUT_MORSEL), constant(argumentSize.nLongs), constant(argumentSize.nReferences)),
        invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit, Int, Long]("setLongAt"),
          constant(offset), invoke(loadField(CURSOR), method[NodeCursor, Long]("nodeReference"))),
        inner.genOperate
      )
    )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    //resources.cursorPools.nodeCursorPool.free(cursor)
    //cursor = null
    block(
      FREE_NODE_CURSOR,
      setField(CURSOR, constant(null))
    )
  }
}

object OperatorCodeGenTemplates {
  // Constructor parameters
  val DATA_READ_CONSTRUCTOR_PARAMETER: Parameter = param[Read]("dataRead")
  val INPUT_MORSEL_CONSTRUCTOR_PARAMETER: Parameter = param[MorselExecutionContext]("inputMorsel")

  // Fields
  val DATA_READ: InstanceField = field[Read]("dataRead", load(DATA_READ_CONSTRUCTOR_PARAMETER.name))
  val CURSOR: InstanceField = field[NodeCursor]("cursor")
  val INPUT_MORSEL: InstanceField = field[MorselExecutionContext]("inputMorsel", load(INPUT_MORSEL_CONSTRUCTOR_PARAMETER.name))
  val INNER_LOOP: InstanceField = field[Boolean]("innerLoop", constant(false))

  // IntermediateRepresentation code
  val OUTPUT_ROW: IntermediateRepresentation =
    load("context")

  val OUTPUT_ROW_MOVE_TO_NEXT: IntermediateRepresentation =
    invokeSideEffect(load("context"), method[MorselExecutionContext, Unit]("moveToNextRow"))

  val DB_ACCESS: IntermediateRepresentation =
    load("dbAccess")

  val ALLOCATE_NODE_CURSOR: IntermediateRepresentation =
    invoke(load("nodeCursorPool"), method[CursorPool[_], Cursor]("allocate"))

  val FREE_NODE_CURSOR: IntermediateRepresentation =
    invokeSideEffect(load("nodeCursorPool"), method[CursorPool[NodeCursor], Unit, Cursor]("free"), loadField(CURSOR))

  val ALL_NODE_SCAN: IntermediateRepresentation = invokeSideEffect(loadField(DATA_READ), method[Read, Unit, NodeCursor]("allNodesScan"), loadField(CURSOR))

  val INPUT_ROW_IS_VALID: IntermediateRepresentation = invoke(loadField(INPUT_MORSEL), method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_IS_VALID: IntermediateRepresentation = invoke(load("context"), method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_FINISHED_WRITING: IntermediateRepresentation = invokeSideEffect(load("context"), method[MorselExecutionContext, Unit]("finishedWriting"))
  val INPUT_ROW_MOVE_TO_NEXT: IntermediateRepresentation = invokeSideEffect(loadField(INPUT_MORSEL), method[MorselExecutionContext, Unit]("moveToNextRow"))
}
