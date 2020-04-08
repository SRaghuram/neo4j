/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.ExtendClass
import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLength
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.arraySet
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.methodDeclaration
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.oneTime
import org.neo4j.codegen.api.IntermediateRepresentation.param
import org.neo4j.codegen.api.IntermediateRepresentation.self
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.trueValue
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.VariablePredicate
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.NO_ENTITY_FUNCTION
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.NO_PREDICATE_OFFSET
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledHelpers
import org.neo4j.cypher.internal.runtime.compiled.expressions.DefaultExpressionCompilerFront
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.VarLengthExpandPipe
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.CursorPools
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_NODE_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.CURSORS
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.EXPRESSION_VARIABLES
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.PARAMS
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

import scala.collection.mutable.ArrayBuffer

class VarExpandOperator(val workIdentity: WorkIdentity,
                        fromSlot: Slot,
                        relOffset: Int,
                        toSlot: Slot,
                        dir: SemanticDirection,
                        projectedDir: SemanticDirection,
                        types: RelationshipTypes,
                        minLength: Int,
                        maxLength: Int,
                        shouldExpandAll: Boolean,
                        tempNodeOffset: Int,
                        tempRelationshipOffset: Int,
                        nodePredicate: Expression,
                        relationshipPredicate: Expression) extends StreamingOperator {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================

  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot, throwOnTypeError = false)
  private val getToNodeFunction =
    if (shouldExpandAll) NO_ENTITY_FUNCTION // We only need this getter in the ExpandInto case
    else makeGetPrimitiveNodeFromSlotFunctionFor(toSlot, throwOnTypeError = false)
  private val toOffset = toSlot.offset
  private val projectBackwards = VarLengthExpandPipe.projectBackwards(dir, projectedDir)

  //===========================================================================
  // Runtime code
  //===========================================================================

  override def toString: String = "VarExpand"

  override def nextTasks(state: PipelinedQueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    IndexedSeq(new OTask(inputMorsel.nextCopy))

  class OTask(inputMorsel: Morsel) extends InputLoopTask(inputMorsel) {

    override def workIdentity: WorkIdentity = VarExpandOperator.this.workIdentity

    override def toString: String = "VarExpandTask"

    private var varExpandCursor: VarExpandCursor = _
    private var predicateState: QueryState = _
    private var executionEvent: OperatorProfileEvent = _

    override protected def enterOperate(state: PipelinedQueryState, resources: QueryResources): Unit = {
      if (tempNodeOffset != NO_PREDICATE_OFFSET || tempRelationshipOffset != NO_PREDICATE_OFFSET) {
        predicateState = state.queryStateForExpressionEvaluation(resources)
      }

      if (varExpandCursor != null) {
        varExpandCursor.enterWorkUnit(resources.cursorPools)
      }
    }

    protected override def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      val fromNode = getFromNodeFunction.applyAsLong(inputCursor)
      val toNode = getToNodeFunction.applyAsLong(inputCursor)

      val nodeVarExpandPredicate =
        if (tempNodeOffset != NO_PREDICATE_OFFSET) {
          new VarExpandPredicate[Long] {
            override def isTrue(nodeId: Long): Boolean = {
              val value = state.queryContext.nodeById(nodeId)
              predicateState.expressionVariables(tempNodeOffset) = value
              nodePredicate(inputCursor, predicateState) eq Values.TRUE
            }
          }
        } else {
          VarExpandPredicate.NO_NODE_PREDICATE
        }

      val relVarExpandPredicate =
        if (tempRelationshipOffset != NO_PREDICATE_OFFSET) {
          new VarExpandPredicate[RelationshipTraversalCursor] {
            override def isTrue(cursor: RelationshipTraversalCursor): Boolean = {
              val value = VarExpandCursor.relationshipFromCursor(state.queryContext, cursor)
              predicateState.expressionVariables(tempRelationshipOffset) = value
              relationshipPredicate(inputCursor, predicateState) eq Values.TRUE
            }
          }
        } else {
          VarExpandPredicate.NO_RELATIONSHIP_PREDICATE
        }

      if (entityIsNull(fromNode) || !nodeVarExpandPredicate.isTrue(fromNode) || (!shouldExpandAll && entityIsNull(toNode))) {
        false
      } else {
        varExpandCursor = VarExpandCursor(dir,
          fromNode,
          toNode,
          resources.cursorPools.nodeCursorPool.allocateAndTrace(),
          projectBackwards,
          types.types(state.queryContext),
          minLength,
          maxLength,
          state.queryContext.transactionalContext.dataRead,
          state.queryContext,
          nodeVarExpandPredicate,
          relVarExpandPredicate)
        varExpandCursor.enterWorkUnit(resources.cursorPools)
        varExpandCursor.setTracer(executionEvent)
        true
      }
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {

      while (outputRow.onValidRow && varExpandCursor.next()) {
        outputRow.copyFrom(inputCursor)
        if (shouldExpandAll) {
          outputRow.setLongAt(toOffset, varExpandCursor.toNode)
        }
        outputRow.setRefAt(relOffset, varExpandCursor.relationships)
        outputRow.next()
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      this.executionEvent = event
      if (varExpandCursor != null) {
        varExpandCursor.setTracer(event)
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      if (varExpandCursor != null) {
        varExpandCursor.free(resources.cursorPools)
        varExpandCursor = null
      }
    }
  }
}

class VarExpandOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                    id: Id,
                                    innermost: DelegateOperatorTaskTemplate,
                                    isHead: Boolean,
                                    fromSlot: Slot,
                                    relOffset: Int,
                                    toSlot: Slot,
                                    dir: SemanticDirection,
                                    projectedDir: SemanticDirection,
                                    types: Array[Int],
                                    missingTypes: Array[String],
                                    minLength: Int,
                                    maxLength: Int,
                                    shouldExpandAll: Boolean,
                                    tempNodeOffset: Int,
                                    tempRelOffset: Int,
                                    maybeNodeVariablePredicate: Option[VariablePredicate],
                                    maybeRelVariablePredicate: Option[VariablePredicate])
                                   (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen, isHead) {

  private val typeField = field[Array[Int]](codeGen.namer.nextVariableName("type"),
    if (types.isEmpty && missingTypes.isEmpty) constant(null)
    else arrayOf[Int](types.map(constant):_*))
  private val missingTypeField = field[Array[String]](codeGen.namer.nextVariableName("missingType"),
    arrayOf[String](missingTypes.map(constant):_*))
  private val varExpandCursorField = field[VarExpandCursor](codeGen.namer.nextVariableName("varExpandCursor"))
  private val toOffset = toSlot.offset
  private val projectBackwards = VarLengthExpandPipe.projectBackwards(dir, projectedDir)
  private var startNodePredicate: Option[IntermediateExpression] = _
  private var nodePredicate: Option[IntermediateExpression] = _
  private var relPredicate: Option[IntermediateExpression] = _

  override final def scopeId: String = "varExpand" + id.x

  override def genMoreFields: Seq[Field] = {
    val localFields =
      ArrayBuffer(typeField,
        varExpandCursorField)
    if (missingTypes.nonEmpty) {
      localFields += missingTypeField
    }
    localFields ++ nodePredicate.map(_.fields).getOrElse(Seq.empty) ++ relPredicate.map(_.fields).getOrElse(Seq.empty)
  }

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genExpressions: Seq[IntermediateExpression] = startNodePredicate.toSeq

  /**
   * In here we create an instance of VarExpandCursor that overrides the `satisfyPredicates` method
   * with the appropriate behavior.
   *
   * {{{
   *   var innerLoop = false
   *   this.canContinue = false
   *   val fromNode = [GET FROM INPUT]
   *   val toNode = [GET FROM INPUT OR -1 if ExpandAll]
   *   if (fromNode != -1L && [fromNode satisfies nodePredicate]) {//for ExandInto we also check toNode
   *     this.varExpandCursor = [SPECIALIZED CURSOR]
   *     this.varExpandCursor.enterWorkUnit(cursorPool)
   *     this.varExpandCursor.setTracer(executionEvent)
   *   }
   * }}}
   *
   */
  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val resultBoolean = codeGen.namer.nextVariableName()
    val fromNode = codeGen.namer.nextVariableName("fromNode")
    val toNode = codeGen.namer.nextVariableName("toNode")

    /**
     * Generate node predicate to be checked on the fromNode
     */
    def generateStartNodePredicate() = {
      if (startNodePredicate == null) {
        startNodePredicate = maybeNodeVariablePredicate
          .map(p => codeGen.intermediateCompileExpression(p.predicate)
            .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile ${p.predicate}")))
      }

      val fromNodePredicate =
        if (tempNodeOffset == NO_PREDICATE_OFFSET) None else startNodePredicate.map { pred =>
          block(
            arraySet(EXPRESSION_VARIABLES, tempNodeOffset,
              invoke(DB_ACCESS,
                method[DbAccess, NodeValue, Long]("nodeById"),
                load(fromNode))),
            equal(trueValue, nullCheckIfRequired(pred)))
        }
      val predicateOnFromNode = fromNodePredicate.foldLeft(notEqual(load(fromNode), constant(-1L))) {
        case (acc, current) => and(acc, current)
      }
      val predicate =
        if (shouldExpandAll) predicateOnFromNode
        else and(predicateOnFromNode, notEqual(load(toNode), constant(-1L)))
      predicate
    }

    block(
      declareAndAssign(typeRefOf[Boolean],resultBoolean,  constant(false)),
      setField(canContinue, constant(false)),
      declareAndAssign(typeRefOf[Long], fromNode, getNodeIdFromSlot(fromSlot)),
      declareAndAssign(typeRefOf[Long], toNode, if (shouldExpandAll) constant(-1L) else getNodeIdFromSlot(toSlot)),
      condition(generateStartNodePredicate()){
        block(
          loadTypes,
          setField(varExpandCursorField, newInstance(createInnerClass(dir),
            load(fromNode),
            load(toNode),
            ALLOCATE_NODE_CURSOR,
            constant(projectBackwards),
            loadField(typeField),
            constant(minLength),
            constant(maxLength),
            loadField(DATA_READ),
            cast[CypherRow](INPUT_CURSOR),
            DB_ACCESS,
            PARAMS,
            CURSORS,
            EXPRESSION_VARIABLES)),
          invokeSideEffect(loadField(varExpandCursorField), method[VarExpandCursor, Unit, CursorPools]("enterWorkUnit"), CURSOR_POOL),
          invokeSideEffect(loadField(varExpandCursorField), method[VarExpandCursor, Unit, OperatorProfileEvent]("setTracer"), loadField(executionEventField)),

          setField(canContinue, profilingCursorNext[VarExpandCursor](loadField(varExpandCursorField), id)),
          assign(resultBoolean, constant(true)),
        )
      },

      load(resultBoolean)
    )
  }

  /**
   * {{{
   *     while (hasDemand && this.canContinue) {
   *         outputRow.copyFrom(inputMorsel)
   *         outputRow.setRefAt(relOffset, this.varExpandCursor.relationships)
   *         outputRow.setLongAt(toOffset, this.varExpandCursor.toNode)//Only for ExpandAll
   *          <<< inner.genOperate() >>>
   *          this.canContinue = relationships.next()
   * }}}
   */
  override protected def genInnerLoop: IntermediateRepresentation = {
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(Math.min(codeGen.inputSlotConfiguration.numberOfLongs, codeGen.slots.numberOfLongs),
          Math.min(codeGen.inputSlotConfiguration.numberOfReferences, codeGen.slots.numberOfReferences)),
        codeGen.setRefAt(relOffset, invoke(loadField(varExpandCursorField),
          method[VarExpandCursor, ListValue]("relationships"))),
        if (shouldExpandAll) codeGen.setLongAt(toOffset, invoke(loadField(varExpandCursorField), method[VarExpandCursor, Long]("toNode")) )
        else noop(),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(
          innermost.setUnlessPastLimit(canContinue, profilingCursorNext[VarExpandCursor](loadField(varExpandCursorField), id))
        ),
        endInnerLoop
      )
    )
  }

  /**
   * {{{
   *    if (varExpandCursor != null) {
   *       varExpandCursor.free(resources.cursorPools)
   *       varExpandCursor = null
   *    }
   * }}}
   */
  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    condition(isNotNull(loadField(varExpandCursorField)))(
      block(
        invokeSideEffect(loadField(varExpandCursorField),
          method[VarExpandCursor, Unit, CursorPools]("free"), CURSOR_POOL),
        setField(varExpandCursorField, constant(null))
      )
    )
  }

  /**
   * {{{
   *    if (varExpandCursor != null) {
   *      varExpandCursor.setTracer(event)
   *    }
   * }}}
   */
  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      condition(isNotNull(loadField(varExpandCursorField)))(
        invokeSideEffect(loadField(varExpandCursorField), method[VarExpandCursor, Unit, OperatorProfileEvent]("setTracer"), loadField(executionEventField)),
      ),
      inner.genSetExecutionEvent(event)
    )
  }

  /**
   * Generate the predicates that are evaluated along the expansion.
   *
   * We have two possible contributions one predicate for nodes and one for relationships.
   *
   * The node contribution will look something like:
   *
   * {{{
   *   expressionVariables(tempNodeOffset) = dbAccess.nodeById(selectionCursor.otherNodeReference())
   *   [evaluate predicate] == Values.TRUE
   * }}}
   *
   * {{{
   *   expressionVariables(tempRelOffset) = VarExpandCursor.relationshipFromCursor(selectionCursor)
   *   [evaluate predicate] == Values.TRUE
   * }}}
   */
  private def generatePredicate = (generateNodePredicate, generateRelationshipPredicate) match {
    case (Some(np), Some(rp)) => and(np, rp)
    case (Some(np), None) => np
    case (None, Some(rp)) => rp
    case (None, None) => constant(true)
  }

  private def generateNodePredicate = {
    if (tempNodeOffset == NO_PREDICATE_OFFSET) None else nodePredicate.map { pred =>
      block(
        oneTime(arraySet(EXPRESSION_VARIABLES, tempNodeOffset, invoke(DB_ACCESS,
          method[DbAccess, NodeValue, Long]("nodeById"),
          invoke(load("selectionCursor"),
            method[RelationshipTraversalCursor, Long](
              "otherNodeReference"))))),
        equal(trueValue, nullCheckIfRequired(pred)))
    }
  }

  private def generateRelationshipPredicate = {
    if (tempRelOffset == NO_PREDICATE_OFFSET) None else relPredicate.map { pred =>
      block(
        oneTime(arraySet(EXPRESSION_VARIABLES, tempRelOffset,
          invokeStatic(
            method[VarExpandCursor, RelationshipValue, DbAccess, RelationshipTraversalCursor]("relationshipFromCursor"),
            DB_ACCESS, load("selectionCursor")))),
        equal(trueValue, nullCheckIfRequired(pred)))
    }
  }

  /**
   * Creates an inner class that extends `VarExpandCursor` with the appropriate node and relationship predicates
   */
  private def createInnerClass(dir: SemanticDirection) =  {
    //since we are evaluating the predicate expression in a separate method in a separate class we cannot
    //use the provided OperatorExpressionCompiler since it will try to read from local variables instead of accessing the context.
    //Here we assume we are always running as start operator of a pipeline and will always read from context unless we are
    //accessing fromNode and toNode which we already have stored in fields.
    val newScopeExpressionCompiler = new DefaultExpressionCompilerFront(codeGen.slots, codeGen.readOnly, codeGen.namer) {
      override protected def getLongAt(offset: Int): IntermediateRepresentation =
        if (fromSlot.isLongSlot && offset == fromSlot.offset) {
          invoke(self(), method[VarExpandCursor, Long]("fromNode"))
        } else if (toSlot.isLongSlot && offset == toSlot.offset) {
          invoke(self(), method[VarExpandCursor, Long]("targetToNode"))
        } else {
          super.getLongAt(offset)
        }
      override protected def getRefAt(offset: Int): IntermediateRepresentation = {
        val tmp = codeGen.namer.nextVariableName()
        if (!fromSlot.isLongSlot && offset == fromSlot.offset) {
          block(
            oneTime(declareAndAssign(typeRefOf[AnyValue], tmp,
              invokeStatic(method[CompiledHelpers, AnyValue, DbAccess, Long]("nodeOrNoValue"),
                DB_ACCESS, invoke(self(), method[VarExpandCursor, Long]("fromNode"))))),
            load(tmp))
        } else if (!toSlot.isLongSlot && offset == toSlot.offset) {
          block(
            oneTime(declareAndAssign(typeRefOf[AnyValue], tmp,
              invokeStatic(method[CompiledHelpers, AnyValue, DbAccess, Long]("nodeOrNoValue"),
                DB_ACCESS, invoke(self(), method[VarExpandCursor, Long]("targetToNode"))))),
            load(tmp))
        } else {
          super.getRefAt(offset)
        }
      }
    }

    if (nodePredicate == null) {
      nodePredicate = maybeNodeVariablePredicate
        .map(p => newScopeExpressionCompiler.intermediateCompileExpression(p.predicate)
          .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile ${p.predicate}")))
    }

    if (relPredicate == null) {
      relPredicate = maybeRelVariablePredicate
        .map(p => newScopeExpressionCompiler.intermediateCompileExpression(p.predicate)
          .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile ${p.predicate}")))
    }

    //Directions governs what class we are extending
    val classToExtend = dir match {
      case SemanticDirection.OUTGOING => typeRefOf[OutgoingVarExpandCursor]
      case SemanticDirection.INCOMING => typeRefOf[IncomingVarExpandCursor]
      case SemanticDirection.BOTH => typeRefOf[AllVarExpandCursor]
    }

    val fields = nodePredicate.map(_.fields).getOrElse(Seq.empty) ++ relPredicate.map(_.fields).getOrElse(Seq.empty)
    val locals = nodePredicate.map(_.variables).getOrElse(Set.empty) ++ relPredicate.map(_.variables).getOrElse(Set.empty)

    ExtendClass(codeGen.namer.nextVariableName().toUpperCase + "VarExpandCursorImpl", classToExtend,
      Seq(param[Long]("fromNode"),
        param[Long]("toNode"),
        param[NodeCursor]("nodeCursor"),
        param[Boolean]("projectBackwards"),
        param[Array[Int]]("types"),
        param[Int]("minLength"),
        param[Int]("maxLength"),
        param[Read]("read"),
        param[CypherRow](ExpressionCompilation.ROW_NAME),
        param[DbAccess](ExpressionCompilation.DB_ACCESS_NAME),
        param[Array[AnyValue]](ExpressionCompilation.PARAMS_NAME),
        param[ExpressionCursors](ExpressionCompilation.CURSORS_NAME),
        param[Array[AnyValue]](ExpressionCompilation.EXPRESSION_VARIABLES_NAME)),
      Seq(methodDeclaration[Boolean]("satisfyPredicates",
        generatePredicate,
        () => locals.toSeq,
        param[CypherRow](ExpressionCompilation.ROW_NAME),
        param[DbAccess](ExpressionCompilation.DB_ACCESS_NAME),
        param[Array[AnyValue]](ExpressionCompilation.PARAMS_NAME),
        param[ExpressionCursors](ExpressionCompilation.CURSORS_NAME),
        param[Array[AnyValue]](ExpressionCompilation.EXPRESSION_VARIABLES_NAME),
        param[RelationshipTraversalCursor]("selectionCursor"))),
      fields)
  }

  private def loadTypes = {
    if (missingTypes.isEmpty) noop()
    else {
      condition(notEqual(arrayLength(loadField(typeField)), constant(types.length + missingTypes.length))) {
        setField(typeField,
          invokeStatic(method[ExpandAllOperatorTaskTemplate, Array[Int], Array[Int], Array[String], DbAccess](
            "computeTypes"),
            loadField(typeField), loadField(missingTypeField), DB_ACCESS))
      }
    }
  }

  private def getNodeIdFromSlot(slot: Slot): IntermediateRepresentation = slot match {
    // NOTE: We do not save the local slot variable, since we are only using it with our own local variable within a local scope
    case LongSlot(offset, _, _) =>
      codeGen.getLongAt(offset)
    case RefSlot(offset, false, _) =>
      invokeStatic(method[CompiledHelpers, Long, AnyValue]("nodeIdOrNullFromAnyValue"),
        codeGen.getRefAt(offset))
    case RefSlot(offset, true, _) =>
      ternary(
        equal(codeGen.getRefAt(offset), noValue),
        constant(-1L),
        invokeStatic(method[CompiledHelpers, Long, AnyValue]("nodeIdOrNullFromAnyValue"),
          codeGen.getRefAt(offset))
      )
    case _ =>
      throw new InternalException(s"Do not know how to get a node id for slot $slot")
  }
}
