/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Constructor, Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.{NO_ENTITY_FUNCTION, makeGetPrimitiveNodeFromSlotFunctionFor}
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.NO_PREDICATE_OFFSET
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, Slot}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
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
import org.neo4j.cypher.internal.runtime.{DbAccess, ExecutionContext, NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue

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
                                    fromSlot: Slot,
                                    relOffset: Int,
                                    toSlot: Slot,
                                    dir: SemanticDirection,
                                    projectedDir: SemanticDirection,
                                    types: Array[Int],
                                    missingTypes: Array[String],
                                    minLength: Int,
                                    maxLength: Int,
                                    shouldExpandAll: Boolean)
                                   (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {
  import OperatorCodeGenHelperTemplates._

  private val typeField = field[Array[Int]](codeGen.namer.nextVariableName(),
                                            if (types.isEmpty && missingTypes.isEmpty) constant(null)
                                            else arrayOf[Int](types.map(constant):_*))
  private val missingTypeField = field[Array[String]](codeGen.namer.nextVariableName(),
                                                      arrayOf[String](missingTypes.map(constant):_*))
  private val varExpandCursorField = field[VarExpandCursor](codeGen.namer.nextVariableName())
  private val toOffset = toSlot.offset
  private val projectBackwards = VarLengthExpandPipe.projectBackwards(dir, projectedDir)

  override def genMoreFields: Seq[Field] = {
    val localFields =
      ArrayBuffer(typeField,
                  varExpandCursorField)
    if (missingTypes.nonEmpty) {
      localFields += missingTypeField
    }
    localFields
  }

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {

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
          setField(varExpandCursorField, newInstance(getCursorConstructor(dir),
                                                     load(fromNode),
                                                     load(toNode),
                                                     ALLOCATE_NODE_CURSOR,
                                                     constant(projectBackwards),
                                                     loadField(typeField),
                                                     constant(minLength),
                                                     constant(maxLength),
                                                     loadField(DATA_READ),
                                                     DB_ACCESS,
                                                     constant(null),
                                                     constant(null)
                                                     )),
          invokeSideEffect(loadField(varExpandCursorField), method[VarExpandCursor, Unit, CursorPools]("enterWorkUnit"), CURSOR_POOL),
          invokeSideEffect(loadField(varExpandCursorField), method[VarExpandCursor, Unit, OperatorProfileEvent]("setTracer"), loadField(executionEventField)),
          setField(canContinue, cursorNext[VarExpandCursor](loadField(varExpandCursorField))),
          assign(resultBoolean, constant(true)),
          )
      },

      load(resultBoolean)
      )
  }

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
    *
    */
  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      condition(isNotNull(loadField(varExpandCursorField)))(
          invokeSideEffect(loadField(varExpandCursorField), method[VarExpandCursor, Unit, OperatorProfileEvent]("setTracer"), loadField(executionEventField)),
        ),
      inner.genSetExecutionEvent(event)
      )
  }

  private def getCursorConstructor(dir: SemanticDirection): Constructor =  dir match {
    case SemanticDirection.OUTGOING =>
      constructor[
        OutgoingVarExpandCursor,
        Long,
        Long,
        NodeCursor,
        Boolean,
        Array[Int],
        Int,
        Int,
        Read,
        DbAccess,
        VarExpandPredicate[Long],
        VarExpandPredicate[RelationshipSelectionCursor]
        ]
    case SemanticDirection.INCOMING =>
      constructor[
        IncomingVarExpandCursor,
        Long,
        Long,
        NodeCursor,
        Boolean,
        Array[Int],
        Int,
        Int,
        Read,
        DbAccess,
        VarExpandPredicate[Long],
        VarExpandPredicate[RelationshipSelectionCursor]
        ]
    case SemanticDirection.BOTH =>
      constructor[
        AllVarExpandCursor,
        Long,
        Long,
        NodeCursor,
        Boolean,
        Array[Int],
        Int,
        Int,
        Read,
        DbAccess,
        VarExpandPredicate[Long],
        VarExpandPredicate[RelationshipSelectionCursor]
        ]
  }

  private def loadTypes = {
    if (missingTypes.isEmpty) noop()
    else {
      condition(notEqual(arrayLength(loadField(typeField)), constant(types.length + missingTypes.length))){
        setField(typeField,
                 invokeStatic(method[ExpandAllOperatorTaskTemplate, Array[Int], Array[Int], Array[String], DbAccess]("computeTypes"),
                              loadField(typeField), loadField(missingTypeField), DB_ACCESS))
      }
    }
  }

  private def getNodeIdFromSlot(slot: Slot): IntermediateRepresentation = slot match {
    case LongSlot(offset, _, _) =>
      codeGen.getLongFromExecutionContext(offset, loadField(INPUT_MORSEL))
    case RefSlot(offset, false, _) =>
      invokeStatic(method[CompiledHelpers, Long, AnyValue]("nodeIdOrNullFromAnyValue"),
                   codeGen.getRefFromExecutionContext(offset, loadField(INPUT_MORSEL)))
    case RefSlot(offset, true, _) =>
      ternary(
        equal(codeGen.getRefFromExecutionContext(offset, loadField(INPUT_MORSEL)), noValue),
        constant(-1L),
        invokeStatic(method[CompiledHelpers, Long, AnyValue]("nodeIdOrNullFromAnyValue"),
                     codeGen.getRefFromExecutionContext(offset, loadField(INPUT_MORSEL))))
    case _ =>
      throw new InternalException(s"Do not know how to get a node id for slot $slot")
  }

  private def setNode(slot: Slot): IntermediateRepresentation = slot match {
    case LongSlot(offset, _, _) =>
      codeGen.setLongAt(offset, codeGen.getLongFromExecutionContext(offset, loadField(INPUT_MORSEL)))
    case RefSlot(offset, _, _) =>
      codeGen.setRefAt(offset, codeGen.getRefFromExecutionContext(offset, loadField(INPUT_MORSEL)))
  }
}
