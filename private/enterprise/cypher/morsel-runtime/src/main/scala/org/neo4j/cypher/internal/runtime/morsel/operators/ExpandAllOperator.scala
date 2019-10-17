/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, Slot}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.{CompiledHelpers, IntermediateExpression}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{CursorPools, MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.{DbAccess, ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.{allCursor, incomingCursor, outgoingCursor}
import org.neo4j.internal.kernel.api.helpers.{RelationshipSelectionCursor, RelationshipSelections}
import org.neo4j.values.AnyValue

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ExpandAllOperator(val workIdentity: WorkIdentity,
                        fromSlot: Slot,
                        relOffset: Int,
                        toOffset: Int,
                        dir: SemanticDirection,
                        types: RelationshipTypes) extends StreamingOperator {
  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)

  override def toString: String = "ExpandAll"

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    IndexedSeq(new OTask(inputMorsel.nextCopy))

  class OTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def workIdentity: WorkIdentity = ExpandAllOperator.this.workIdentity

    override def toString: String = "ExpandAllTask"

    /*
    This might look wrong, but it's like this by design. This allows the loop to terminate early and still be
    picked up at any point again - all without impacting the tight loop.
    The mutable state is an unfortunate cost for this feature.
     */
    private var nodeCursor: NodeCursor = _
    private var relationships: RelationshipSelectionCursor = _

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val fromNode = getFromNodeFunction.applyAsLong(inputMorsel)
      if (entityIsNull(fromNode))
        false
      else {
        val pools: CursorPools = resources.cursorPools
        nodeCursor = pools.nodeCursorPool.allocateAndTrace()
        relationships = getRelationshipsCursor(context, pools, fromNode, dir, types.types(context))
        relationships.setTracer(resources.getKernelTracer)
        true
      }
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                           context: QueryContext,
                           state: QueryState): Unit = {

      while (outputRow.isValidRow && relationships.next()) {
        val relId = relationships.relationshipReference()
        val otherSide = relationships.otherNodeReference()

        // Now we have everything needed to create a row.
        outputRow.copyFrom(inputMorsel)
        outputRow.setLongAt(relOffset, relId)
        outputRow.setLongAt(toOffset, otherSide)
        outputRow.moveToNextRow()
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      if (relationships != null) {
        nodeCursor.setTracer(event)
        relationships.setTracer(event)
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      val pools = resources.cursorPools
      pools.nodeCursorPool.free(nodeCursor)
      pools.free(relationships)
      nodeCursor = null
      relationships = null
    }

    private def getRelationshipsCursor(context: QueryContext,
                                       pools: CursorPools,
                                       node: Long,
                                       dir: SemanticDirection,
                                       types: Array[Int]): RelationshipSelectionCursor = {

      val read = context.transactionalContext.dataRead
      read.singleNode(node, nodeCursor)
      if (!nodeCursor.next()) RelationshipSelectionCursor.EMPTY
      else {
        dir match {
          case OUTGOING => outgoingCursor(pools, nodeCursor, types)
          case INCOMING => incomingCursor(pools, nodeCursor, types)
          case BOTH => allCursor(pools, nodeCursor, types)
        }
      }
    }
  }
}

class ExpandAllOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                    id: Id,
                                    innermost: DelegateOperatorTaskTemplate,
                                    isHead: Boolean,
                                    fromSlot: Slot,
                                    relOffset: Int,
                                    toOffset: Int,
                                    dir: SemanticDirection,
                                    types: Array[Int],
                                    missingTypes: Array[String])
                                    (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen, isHead) {
  import OperatorCodeGenHelperTemplates._

  private val nodeCursorField = field[NodeCursor](codeGen.namer.nextVariableName() + "nodeCursor")
  private val relationshipsField = field[RelationshipSelectionCursor](codeGen.namer.nextVariableName() + "relationships")
  private val typeField = field[Array[Int]](codeGen.namer.nextVariableName() + "type",
                                            if (types.isEmpty && missingTypes.isEmpty) constant(null)
                                            else arrayOf[Int](types.map(constant):_*)
  )
  private val missingTypeField = field[Array[String]](codeGen.namer.nextVariableName() + "missingType",
                                                      arrayOf[String](missingTypes.map(constant):_*))

  override final def scopeId: String = "expandAll" + id.x

  override def genMoreFields: Seq[Field] = {
    val localFields =
      ArrayBuffer(nodeCursorField, relationshipsField, typeField)
    if (missingTypes.nonEmpty) {
      localFields += missingTypeField
    }

    localFields
  }

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  /**
    * {{{
    *    val fromNode = inputMorsel.getLongAt(fromOffset)
    *    if (entityIsNull(fromNode))
    *      false
    *    else {
    *      nodeCursor = resources.cursorPools.nodeCursorPool.allocate()
    *      groupCursor = resources.cursorPools.relationshipGroupCursorPool.allocate()
    *      traversalCursor = resources.cursorPools.relationshipTraversalCursorPool.allocate()
    *      read.singleNode(node, nodeCursor)
    *      relationships = if (!nodeCursor.next()) RelationshipSelectionCursor.EMPTY
    *                      else {
    *                        //or incomingCursor or allCursor depending on the direction
    *                        outgoingCursor(groupCursor, traversalCursor, nodeCursor, types)
    *                      }
    *      this.canContinue = relationships.next()
    *      true
    *      }
    * }}}
    *
    */
  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val methodToCall = dir match {
      case OUTGOING => method[RelationshipSelections, RelationshipSelectionCursor, CursorFactory, NodeCursor, Array[Int]]("outgoingCursor")
      case INCOMING => method[RelationshipSelections, RelationshipSelectionCursor, CursorFactory, NodeCursor, Array[Int]]("incomingCursor")
      case BOTH => method[RelationshipSelections, RelationshipSelectionCursor, CursorFactory, NodeCursor, Array[Int]]("allCursor")
    }
    val resultBoolean = codeGen.namer.nextVariableName()
    val fromNode = codeGen.namer.nextVariableName() + "fromNode"

    block(
      declareAndAssign(typeRefOf[Boolean],resultBoolean,  constant(false)),
      setField(canContinue, constant(false)),
      declareAndAssign(typeRefOf[Long], fromNode, getNodeIdFromSlot(fromSlot)),
      condition(notEqual(load(fromNode), constant(-1L))){
       block(
         loadTypes,
         allocateAndTraceCursor(nodeCursorField, executionEventField, ALLOCATE_NODE_CURSOR),
         singleNode(load(fromNode), loadField(nodeCursorField)),
         setField(relationshipsField,
                  ///node.next() ? getRelCursor : EMPTY
                  ternary(cursorNext[NodeCursor](loadField(nodeCursorField)),
                          invokeStatic(methodToCall, CURSOR_POOL,
                                       loadField(nodeCursorField), loadField(typeField)),
                          getStatic[RelationshipSelectionCursor, RelationshipSelectionCursor]("EMPTY")
                  )
         ),
         invokeSideEffect(loadField(relationshipsField), method[RelationshipSelectionCursor, Unit, KernelReadTracer]("setTracer"), loadField(executionEventField)),
         assign(resultBoolean, constant(true)),
         setField(canContinue, cursorNext[RelationshipSelectionCursor](loadField(relationshipsField)))
       )
      },

      load(resultBoolean)
    )
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

  /**
    * {{{
    *     while (hasDemand && this.canContinue) {
    *         val relId = relationships.relationshipReference()
    *         val otherSide = relationships.otherNodeReference()
    *
    *         outputRow.copyFrom(inputMorsel)
    *         outputRow.setLongAt(relOffset, relId)
    *         outputRow.setLongAt(toOffset, otherSide)
    *          <<< inner.genOperate() >>>
    *          this.canContinue = relationships.next()
    * }}}
    */
  override protected def genInnerLoop: IntermediateRepresentation = {
    val otherNode = dir match {
      case OUTGOING => method[RelationshipSelectionCursor, Long]("targetNodeReference")
      case INCOMING => method[RelationshipSelectionCursor, Long]("sourceNodeReference")
      case BOTH => method[RelationshipSelectionCursor, Long]("otherNodeReference")
    }
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(Math.min(codeGen.inputSlotConfiguration.numberOfLongs, codeGen.slots.numberOfLongs),
                              Math.min(codeGen.inputSlotConfiguration.numberOfReferences, codeGen.slots.numberOfReferences)),
        codeGen.setLongAt(relOffset, invoke(loadField(relationshipsField),
                                            method[RelationshipSelectionCursor, Long]("relationshipReference"))),
        codeGen.setLongAt(toOffset, invoke(loadField(relationshipsField), otherNode)),
        profileRow(id),
        inner.genOperateWithExpressions,
        setField(canContinue, cursorNext[RelationshipSelectionCursor](loadField(relationshipsField))),
        endInnerLoop
      )
    )
  }

  /**
    * {{{
    *     val pools = resources.cursorPools
    *     pools.nodeCursorPool.free(nodeCursor)
    *     pools.relationshipGroupCursorPool.free(groupCursor)
    *     pools.relationshipTraversalCursorPool.free(traversalCursor)
    *     nodeCursor = null
    *     groupCursor = null
    *     traversalCursor = null
    *     relationships = null
    * }}}
    */
  override protected def genCloseInnerLoop: IntermediateRepresentation = {
   block(
     freeCursor[NodeCursor](loadField(nodeCursorField), NodeCursorPool),
     invokeSideEffect(CURSOR_POOL, method[CursorPools, Unit, RelationshipSelectionCursor]("free"),
                      loadField(relationshipsField)),
     setField(nodeCursorField, constant(null)),
     setField(relationshipsField, constant(null))
   )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      condition(isNotNull(loadField(relationshipsField)))(
        block(
          invokeSideEffect(loadField(nodeCursorField), method[NodeCursor, Unit, KernelReadTracer]("setTracer"), loadField(executionEventField)),
          invokeSideEffect(loadField(relationshipsField), method[RelationshipSelectionCursor, Unit, KernelReadTracer]("setTracer"), loadField(executionEventField)),
        )
      ),
      inner.genSetExecutionEvent(event)
    )
  }

  private def getNodeIdFromSlot(slot: Slot): IntermediateRepresentation = slot match {
    // NOTE: We do not save the local slot variable, since we are only using it with our own local variable within a local scope
    case LongSlot(offset, _, _) =>
      codeGen.getLongAt(offset)
    case RefSlot(offset, false, _) =>
      invokeStatic(method[CompiledHelpers, Long, AnyValue]("nodeFromAnyValue"), codeGen.getRefAt(offset))
    case RefSlot(offset, true, _) =>
      ternary(
        equal(codeGen.getRefAt(offset), noValue),
        constant(-1L),
        invokeStatic(method[CompiledHelpers, Long, AnyValue]("nodeFromAnyValue"), codeGen.getRefAt(offset))
      )
    case _ =>
      throw new InternalException(s"Do not know how to get a node id for slot $slot")
  }
}

object ExpandAllOperatorTaskTemplate {
  def computeTypes(computed: Array[Int], missing: Array[String], dbAccess: DbAccess): Array[Int] = {
    val newTokens = mutable.ArrayBuffer(computed:_*)
    missing.foreach(s => {
      val token = dbAccess.relationshipType(s)
      if (token != TokenRead.NO_TOKEN && !newTokens.contains(token)) {
        newTokens.append(token)
      }
    })
    newTokens.toArray
  }
}
