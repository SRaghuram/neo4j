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
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateRepresentation._
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryIndexes}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.operators._
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.internal.kernel.api.NodeCursor

class FuseOperators(operatorFactory: OperatorFactory,
                    physicalPlan: PhysicalPlan,
                    converters: ExpressionConverters,
                    readOnly: Boolean,
                    queryIndexes: QueryIndexes) {

  def compilePipeline(p: Pipeline): Option[ExecutablePipeline] = {
    // First, try to fuse as many middle operators as possible into the head operator
    val (maybeHeadOperator, remainingMiddlePlans) = fuseOperators(p.headPlan, p.middlePlans)

    val headOperator = maybeHeadOperator.getOrElse(operatorFactory.create(p.headPlan))
    val middleOperators = remainingMiddlePlans.flatMap(operatorFactory.createMiddle)
    val produceResultOperator = p.produceResults.map(operatorFactory.createProduceResults)
    Some(ExecutablePipeline(p.id,
      headOperator,
      middleOperators,
      produceResultOperator,
      p.serial,
      physicalPlan.slotConfigurations(p.headPlan.id),
      p.inputBuffer,
      p.outputBuffer))
  }

  private def fuseOperators(headPlan: LogicalPlan, middlePlans: Seq[LogicalPlan]): (Option[Operator], Seq[LogicalPlan]) = {
    val id = headPlan.id
    val slots = physicalPlan.slotConfigurations(id)
    val expressionCompiler = new IntermediateCodeGeneration(slots) // NOTE: We assume slots is the same within an entire pipeline
    generateSlotAccessorFunctions(slots)

    val workIdentity = WorkIdentity.fromFusedPlans(headPlan, middlePlans)

    // Some stateless operators cannot be fused, e.g. SortPreOperator
    // Return these to be built as separate operators
    val remainingMiddlePlans: List[LogicalPlan] = middlePlans.toList

    // TODO: Fold plans in reverse to build-up templates with inner templates
    //       Then create the taskFactory from the headPlan template
    // E.g. HeadPlan, Seq(MiddlePlan1, MiddlePlan2)
    // MiddlePlan2 -> Template1()
    // MiddlePlan1 -> Template2(inner=Template1)
    // HeadPlan    -> Template3(inner=Template2)
    val innermostTemplate = new DelegateOperatorTaskTemplate

    var reversePlans = middlePlans.foldLeft(List(headPlan))((acc, p) => p :: acc)

    val operatorTaskTemplate = reversePlans.foldLeft(innermostTemplate: OperatorTaskTemplate)((acc, p) => p match {
      case plans.AllNodesScan(nodeVariableName, _) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        new SerialAllNodeScanTemplate(acc, innermostTemplate, nodeVariableName, slots.getLongOffsetFor(nodeVariableName), argumentSize)

      case plans.Selection(predicate, _) =>
        val compiledPredicate: IntermediateExpression = expressionCompiler.compileExpression(predicate).getOrElse(
          return (None, middlePlans)
        )
        new FilterOperatorTemplate(acc, compiledPredicate)

    })

    val operatorTaskWithMorselTemplate = operatorTaskTemplate.asInstanceOf[ContinuableOperatorTaskWithMorselTemplate]

    val taskFactory = ContinuableOperatorTaskWithMorselGenerator.generateTaskFactory(operatorTaskWithMorselTemplate)
    (Some(new CompiledStreamingOperator(workIdentity, taskFactory)), remainingMiddlePlans)
  }
}


class CompiledStreamingOperator(val workIdentity: WorkIdentity,
                                val taskFactory: MorselParallelizer => IndexedSeq[ContinuableOperatorTaskWithMorsel]) extends StreamingOperator {
  /**
    * Initialize new tasks for this operator. This code path let's operators create
    * multiple output rows for each row in `inputMorsel`.
    */
  override protected def nextTasks(context: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    taskFactory(inputMorsel)
  }
}

object ContinuableOperatorTaskWithMorselGenerator {
  private val PACKAGE_NAME = "org.neo4j.codegen"
  private def className(): String = "Operator" + System.nanoTime()

  private def generateClass(template: ContinuableOperatorTaskWithMorselTemplate): Class[_] =
    CodeGeneration.compileClass(template.genClassDeclaration(PACKAGE_NAME, className())
  )

  def generateTaskFactory(template: ContinuableOperatorTaskWithMorselTemplate): MorselParallelizer => IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    val clazz = generateClass(template)
    val constructor = clazz.getDeclaredConstructor(classOf[MorselExecutionContext])
    inputMorsel: MorselParallelizer => {
      IndexedSeq(constructor.newInstance(inputMorsel.nextCopy).asInstanceOf[ContinuableOperatorTaskWithMorsel])
    }
  }
}

trait OperatorTaskTemplate {
  def genOperate: IntermediateRepresentation
  def genClassDeclaration(packageName: String, className: String): ClassDeclaration = {
    throw new InternalException("Illegal start operator template")
  }

  // TODO: Create implementations of these in the base class that handles the recursive inner.genFields logic etc.
  def genFields: Seq[Field]
  def genLocalVariables: Seq[LocalVariable]
}

class DelegateOperatorTaskTemplate(var delegate: OperatorTaskTemplate = null) extends OperatorTaskTemplate {
  override def genOperate: IntermediateRepresentation = {
    delegate.genOperate
  }

  override def genFields: Seq[Field] = delegate.genFields
  override def genLocalVariables: Seq[LocalVariable] = delegate.genLocalVariables
}

/*
    AnyValue evaluate( ExecutionContext context,
                       DbAccess dbAccess, // OldQueryContext
                       AnyValue[] params,
                       ExpressionCursors cursors,
                       AnyValue[] expressionVariables );

 */

class FilterOperatorTemplate(val inner: OperatorTaskTemplate, predicate: IntermediateExpression) extends OperatorTaskTemplate {
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


trait ContinuableOperatorTaskWithMorselTemplate extends OperatorTaskTemplate {
  // TODO: Use methods of actual interface to generate declaration?
  override def genClassDeclaration(packageName: String, className: String): ClassDeclaration = {
    // TODO: Use genFields inst
    //val inputMorselField = field[MorselExecutionContext]("inputMorsel")
    val fields = genFields
    val localVariables = genLocalVariables

    ClassDeclaration(packageName, className,
      extendsClass = Some(typeRefOf[CompiledContinuableOperatorTaskWithMorsel]),
      implementsInterfaces = Seq.empty,
//      constructor = ConstructorDeclaration(
//        IntermediateRepresentation.constructor[CompiledContinuableOperatorTaskWithMorsel, MorselExecutionContext],
//        body = {
//          // TODO: Call super constructor
//          // TODO: Extract utility method or automate codegen for constructor completely based on `fields`
//          val fieldInitializations = new ArrayBuffer[IntermediateRepresentation]
//          var i = 1
//          instanceFields.foreach { f =>
//            fieldInitializations += setField(f, load(s"param$i")) // TODO: No hardcoded name, use utility
//            i += 1
//          }
//          Block(fieldInitializations)
//        }
//      ),
      fields = fields,
      methods = Seq(
        MethodDecl(
          method[ContinuableOperatorTaskWithMorsel, Unit, MorselExecutionContext, QueryContext, QueryState, QueryResources]("operate"),
          body = genOperate,
          localVariables = localVariables
        ),
        MethodDecl(
          method[ContinuableOperatorTaskWithMorsel, Boolean]("canContinue"),
          body = genCanContinue
        )
      )
    )
  }

  //def operate(output: MorselExecutionContext, context: QueryContext, state: QueryState, resources: QueryResources): Unit
  def genOperate: IntermediateRepresentation

  //override def canContinue: Boolean
  def genCanContinue: IntermediateRepresentation
}

abstract class InputLoopTaskTemplate extends ContinuableOperatorTaskWithMorselTemplate {
 import InputLoopTaskTemplate._
  //override val inputMorsel: MorselExecutionContext
  override def genFields: Seq[Field] = {
    Seq(INPUT_MORSEL, INNER_LOOP)
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

object InputLoopTaskTemplate {
  val INPUT_MORSEL = field[MorselExecutionContext]("inputMorsel")
  val INNER_LOOP = field[Boolean]("innerLoop", constant(false))
  val INPUT_ROW_IS_VALID = invoke(loadField(INPUT_MORSEL), method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_IS_VALID = invoke(load("context"), method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_FINISHED_WRITING = invokeSideEffect(load("context"), method[MorselExecutionContext, Unit]("finishedWriting"))
  val INPUT_ROW_MOVE_TO_NEXT = invokeSideEffect(loadField(INPUT_MORSEL), method[MorselExecutionContext, Unit]("moveToNextRow"))
}

class SerialAllNodeScanTemplate(val inner: OperatorTaskTemplate,
                                val innermost: DelegateOperatorTaskTemplate,
                                val nodeVarName: String,
                                val offset: Int,
                                val argumentSize: SlotConfiguration.Size) extends InputLoopTaskTemplate {
  import SerialAllNodeScanTemplate._
  import InputLoopTaskTemplate._

  // Setup the innermost output template
  innermost.delegate = new OperatorTaskTemplate {
    override def genOperate: IntermediateRepresentation = {
      OUTPUT_ROW_MOVE_TO_NEXT
    }
    override def genFields: Seq[Field] = Seq.empty
    override def genLocalVariables: Seq[LocalVariable] = Seq.empty
  }

  override def genFields: Seq[Field] = {
    super.genFields ++ inner.genFields :+ CURSOR
  }

  override def genLocalVariables: Seq[LocalVariable] = {
    inner.genLocalVariables
  }

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    //cursor = resources.cursorPools.nodeCursorPool.allocate()
    //context.transactionalContext.dataRead.allNodesScan(cursor)
    //true

    setField(CURSOR, ALLOCATE_NODE_CURSOR)
    ALL_NODE_SCAN
    constant(true)
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
        invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit]("copyFrom"),
          loadField(INPUT_MORSEL), constant(argumentSize.nLongs), constant(argumentSize.nReferences)),
        invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit]("setLongAt"),
          constant(offset), invoke(loadField(CURSOR), method[NodeCursor, Long]("nodeReference"))),
        inner.genOperate
      )
    )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    //resources.cursorPools.nodeCursorPool.free(cursor)
    //cursor = null
    FREE_NODE_CURSOR
    setField(CURSOR, constant(null))
  }
}

object SerialAllNodeScanTemplate {
  val CURSOR = field[NodeCursor]("cursor")
  val OUTPUT_ROW = load("context")
  val OUTPUT_ROW_MOVE_TO_NEXT = invokeSideEffect(load("context"), method[MorselExecutionContext, Unit]("moveToNextRow"))

  val ALLOCATE_NODE_CURSOR = invokeStatic(method[SerialAllNodeScanTemplate, NodeCursor]("allocateNodeCursor"))
  def allocateNodeCursor(resources: QueryResources): NodeCursor = {
    resources.cursorPools.nodeCursorPool.allocate()
  }

  val FREE_NODE_CURSOR = invokeStatic(method[SerialAllNodeScanTemplate, Unit, QueryResources, NodeCursor]("freeNodeCursor"),
                                      load("resources"), loadField(CURSOR))
  def freeNodeCursor(resources: QueryResources, cursor: NodeCursor): Unit = {
    resources.cursorPools.nodeCursorPool.free(cursor)
  }

  val ALL_NODE_SCAN = invokeStatic(method[SerialAllNodeScanTemplate, Unit, QueryContext, NodeCursor]("allNodeScan"),
                                   load("queryContext"), loadField(CURSOR))
  def allNodeScan(queryContext: QueryContext, cursor: NodeCursor): Unit = {
    queryContext.transactionalContext.dataRead.allNodesScan(cursor)
  }
}
