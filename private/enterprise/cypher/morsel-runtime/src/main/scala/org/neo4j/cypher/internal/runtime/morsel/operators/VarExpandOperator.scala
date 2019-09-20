/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{ExtendClass, Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.{NO_ENTITY_FUNCTION, makeGetPrimitiveNodeFromSlotFunctionFor}
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.NO_PREDICATE_OFFSET
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, Slot}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.{CompiledHelpers, IntermediateExpression}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{RelationshipTypes, VarLengthExpandPipe}
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{CursorPools, MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.internal.kernel.api.{IndexReadSession, NodeCursor, Read}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{ListValue, NodeValue, RelationshipValue}

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

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    IndexedSeq(new OTask(inputMorsel.nextCopy))

  class OTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def workIdentity: WorkIdentity = VarExpandOperator.this.workIdentity

    override def toString: String = "VarExpandTask"

    private var varExpandCursor: VarExpandCursor = _
    private var predicateState: OldQueryState = _
    private var executionEvent: OperatorProfileEvent = _

    override protected def enterOperate(context: QueryContext, state: QueryState, resources: QueryResources): Unit = {
      if (tempNodeOffset != NO_PREDICATE_OFFSET || tempRelationshipOffset != NO_PREDICATE_OFFSET) {
        predicateState = new OldQueryState(context,
                                           resources = null,
                                           params = state.params,
                                           resources.expressionCursors,
                                           Array.empty[IndexReadSession],
                                           resources.expressionVariables(state.nExpressionSlots),
                                           state.subscriber,
                                           NoMemoryTracker)
      }

      if (varExpandCursor != null) {
        varExpandCursor.enterWorkUnit(resources.cursorPools)
      }
    }

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val fromNode = getFromNodeFunction.applyAsLong(inputMorsel)
      val toNode = getToNodeFunction.applyAsLong(inputMorsel)

      val nodeVarExpandPredicate =
        if (tempNodeOffset != NO_PREDICATE_OFFSET) {
          new VarExpandPredicate[Long] {
            override def isTrue(nodeId: Long): Boolean = {
              val value = context.nodeById(nodeId)
              predicateState.expressionVariables(tempNodeOffset) = value
              nodePredicate(inputMorsel, predicateState) eq Values.TRUE
            }
          }
        } else {
          VarExpandPredicate.NO_NODE_PREDICATE
        }

      val relVarExpandPredicate =
        if (tempRelationshipOffset != NO_PREDICATE_OFFSET) {
          new VarExpandPredicate[RelationshipSelectionCursor] {
            override def isTrue(cursor: RelationshipSelectionCursor): Boolean = {
              val value = VarExpandCursor.relationshipFromCursor(context, cursor)
              predicateState.expressionVariables(tempRelationshipOffset) = value
              relationshipPredicate(inputMorsel, predicateState) eq Values.TRUE
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
                                          resources.cursorPools.nodeCursorPool.allocate(),
                                          projectBackwards,
                                          types.types(context),
                                          minLength,
                                          maxLength,
                                          context.transactionalContext.dataRead,
                                          context,
                                          nodeVarExpandPredicate,
                                          relVarExpandPredicate)
        varExpandCursor.enterWorkUnit(resources.cursorPools)
        varExpandCursor.setTracer(executionEvent)
        true
      }
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                           context: QueryContext,
                           state: QueryState): Unit = {

      while (outputRow.isValidRow && varExpandCursor.next()) {
        outputRow.copyFrom(inputMorsel)
        if (shouldExpandAll) {
          outputRow.setLongAt(toOffset, varExpandCursor.toNode)
        }
        outputRow.setRefAt(relOffset, varExpandCursor.relationships)
        outputRow.moveToNextRow()
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
                                    genNodePredicate: Option[() => IntermediateExpression],
                                    genRelPredicate: Option[() => IntermediateExpression])
                                   (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen, isHead) {
  import OperatorCodeGenHelperTemplates._

  private val typeField = field[Array[Int]](codeGen.namer.nextVariableName(),
                                            if (types.isEmpty && missingTypes.isEmpty) constant(null)
                                            else arrayOf[Int](types.map(constant):_*))
  private val missingTypeField = field[Array[String]](codeGen.namer.nextVariableName(),
                                                      arrayOf[String](missingTypes.map(constant):_*))
  private val varExpandCursorField = field[VarExpandCursor](codeGen.namer.nextVariableName())
  private val toOffset = toSlot.offset
  private val projectBackwards = VarLengthExpandPipe.projectBackwards(dir, projectedDir)
  private var nodePredicate: Option[IntermediateExpression] = _
  private var relPredicate: Option[IntermediateExpression] = _

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

  //we don't add nodePredicate and relPredicate since they are evaluated
  //in a different class and we only pass along its fields
  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  /**
    * In here we create an instance of VarExpandCursor that overrides the `satisfyPredicates` method
    * with the appropriate behavior.
    *
    * {{{
    *   var innerLoop = false
    *   this.canContinue = false
    *   val fromNode = [GET FROM INPUT]
    *   val toNode = [GET FROM INPUT OR -1 if ExpandAll]
    *   if (fromNode != -1L ) {//for ExandInto we also check toNode
    *     this.varExpandCursor = [SPECIALIZED CURSOR]
    *     this.varExpandCursor.enterWorkUnit(cursorPool)
    *     this.varExpandCursor.setTracer(executionEvent)
    *   }
    * }}}
    *
    */
  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    //initialize predicates
    nodePredicate = genNodePredicate.map(_())
    relPredicate = genRelPredicate.map(_())

    val resultBoolean = codeGen.namer.nextVariableName()
    val fromNode = codeGen.namer.nextVariableName()
    val toNode = codeGen.namer.nextVariableName()
    val predicate =
      if (shouldExpandAll) notEqual(load(fromNode), constant(-1L))
      else and(notEqual(load(fromNode), constant(-1L)), notEqual(load(toNode), constant(-1L)))
    block(
      declareAndAssign(typeRefOf[Boolean],resultBoolean,  constant(false)),
      setField(canContinue, constant(false)),
      declareAndAssign(typeRefOf[Long], fromNode, getNodeIdFromSlot(fromSlot)),
      declareAndAssign(typeRefOf[Long], toNode, if (shouldExpandAll) constant(-1L) else getNodeIdFromSlot(toSlot)),
      condition(predicate){
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
                                                     OUTPUT_ROW,
                                                     DB_ACCESS,
                                                     PARAMS,
                                                     EXPRESSION_CURSORS,
                                                     EXPRESSION_VARIABLES)),
          invokeSideEffect(loadField(varExpandCursorField), method[VarExpandCursor, Unit, CursorPools]("enterWorkUnit"), CURSOR_POOL),
          invokeSideEffect(loadField(varExpandCursorField), method[VarExpandCursor, Unit, OperatorProfileEvent]("setTracer"), loadField(executionEventField)),

          setField(canContinue, cursorNext[VarExpandCursor](loadField(varExpandCursorField))),
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
        if (innermost.shouldWriteToContext) {
          invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit, MorselExecutionContext]("copyFrom"),
                           loadField(INPUT_MORSEL))
        } else {
          noop()
        },
        setNode(fromSlot),
        codeGen.setRefAt(relOffset, invoke(loadField(varExpandCursorField),
                                            method[VarExpandCursor, ListValue]("relationships"))),
        if (shouldExpandAll) codeGen.setLongAt(toOffset, invoke(loadField(varExpandCursorField), method[VarExpandCursor, Long]("toNode")) )
        else noop(),
        profileRow(id),
        inner.genOperateWithExpressions,
        setField(canContinue, cursorNext[VarExpandCursor](loadField(varExpandCursorField))),
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
  private def generatePredicate = {
    val maybeNodePred = if (tempNodeOffset == NO_PREDICATE_OFFSET) None else nodePredicate.map { pred =>
      block(
        oneTime(arraySet(EXPRESSION_VARIABLES, tempNodeOffset, invoke(DB_ACCESS,
                                                                      method[DbAccess, NodeValue, Long]("nodeById"),
                                                                      invoke(load("selectionCursor"),
                                                                             method[RelationshipSelectionCursor, Long](
                                                                               "otherNodeReference"))))),
        equal(trueValue, nullCheckIfRequired(pred)))
    }
    val maybeRelPred = if (tempRelOffset == NO_PREDICATE_OFFSET) None else relPredicate.map { pred =>
      block(
        oneTime(arraySet(EXPRESSION_VARIABLES, tempRelOffset,
                         invokeStatic(
                           method[VarExpandCursor, RelationshipValue, DbAccess, RelationshipSelectionCursor]("relationshipFromCursor"),
                           DB_ACCESS, load("selectionCursor")))),
        equal(trueValue, nullCheckIfRequired(pred)))
    }

    (maybeNodePred, maybeRelPred) match {
      case (Some(np), Some(rp)) => and(np, rp)
      case (Some(np), None) => np
      case (None, Some(rp)) => rp
      case (None, None) => constant(true)
    }
  }

  /**
    * Creates an inner class that extends `VarExpandCursor` with the appropriate node and relationship predicates
    */
  private def createInnerClass(dir: SemanticDirection) =  {

    //Directions governs what class we are extending
    val classToExtend = dir match {
      case SemanticDirection.OUTGOING => typeRefOf[OutgoingVarExpandCursor]
      case SemanticDirection.INCOMING => typeRefOf[IncomingVarExpandCursor]
      case SemanticDirection.BOTH => typeRefOf[AllVarExpandCursor]
    }

    val fields = nodePredicate.map(_.fields).getOrElse(Seq.empty) ++ relPredicate.map(_.fields).getOrElse(Seq.empty)
    val locals = nodePredicate.map(_.variables).getOrElse(Set.empty) ++ relPredicate.map(_.variables).getOrElse(Set.empty)

    ExtendClass(codeGen.namer.nextVariableName().toUpperCase, classToExtend,
                Seq(param[Long]("fromNode"),
                   param[Long]("toNode"),
                   param[NodeCursor]("nodeCursor"),
                   param[Boolean]("projectBackwards"),
                   param[Array[Int]]("types"),
                   param[Int]("minLength"),
                   param[Int]("maxLength"),
                   param[Read]("read"),
                   param[ExecutionContext]("executionContext"),
                   param[DbAccess]("dbAccess"),
                   param[Array[AnyValue]]("params"),
                   param[ExpressionCursors]("cursors"),
                   param[Array[AnyValue]]("expressionVariables")),
                Seq(methodDeclaration[Boolean]("satisfyPredicates",
                                              generatePredicate,
                                              () => locals.toSeq,
                                              param[ExecutionContext]("executionContext"),
                                              param[DbAccess]("dbAccess"),
                                              param[Array[AnyValue]]("params"),
                                              param[ExpressionCursors]("cursors"),
                                              param[Array[AnyValue]]("expressionVariables"),
                                              param[RelationshipSelectionCursor]("selectionCursor"))),
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
    case LongSlot(offset, _, _) =>
      codeGen.getLongAtOrElse(offset, codeGen.getLongFromExecutionContext(offset, loadField(INPUT_MORSEL)))
    case RefSlot(offset, false, _) =>
      invokeStatic(method[CompiledHelpers, Long, AnyValue]("nodeIdOrNullFromAnyValue"),
                   codeGen.getRefAtOrElse(offset, codeGen.getRefFromExecutionContext(offset, loadField(INPUT_MORSEL))))
    case RefSlot(offset, true, _) =>
      ternary(
        equal(codeGen.getRefAtOrElse(offset, codeGen.getRefFromExecutionContext(offset, loadField(INPUT_MORSEL))), noValue),
        constant(-1L),
        invokeStatic(method[CompiledHelpers, Long, AnyValue]("nodeIdOrNullFromAnyValue"),
                     codeGen.getRefAtOrElse(offset, codeGen.getRefFromExecutionContext(offset, loadField(INPUT_MORSEL)))))
    case _ =>
      throw new InternalException(s"Do not know how to get a node id for slot $slot")
  }

  private def setNode(slot: Slot): IntermediateRepresentation = slot match {
    case LongSlot(offset, _, _) =>
      if (codeGen.hasLongAt(offset)) noop()
      else codeGen.setLongAt(offset, codeGen.getLongFromExecutionContext(offset, loadField(INPUT_MORSEL)))
    case RefSlot(offset, _, _) =>
      if (codeGen.hasRefAt(offset)) noop()
      else codeGen.setRefAt(offset, codeGen.getRefFromExecutionContext(offset, loadField(INPUT_MORSEL)))
  }
}
