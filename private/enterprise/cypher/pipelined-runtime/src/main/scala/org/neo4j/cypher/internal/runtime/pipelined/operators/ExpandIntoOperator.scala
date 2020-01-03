/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable, Method}
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.execution.{CursorPools, MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate.{getNodeIdFromSlot, loadTypes}
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandIntoOperatorTaskTemplate._
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.{OperatorExpressionCompiler, RelationshipCursorRepresentation}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.{CachingExpandInto, RelationshipSelectionCursor}

import scala.collection.mutable.ArrayBuffer

class ExpandIntoOperator(val workIdentity: WorkIdentity,
                        fromSlot: Slot,
                        relOffset: Int,
                        toSlot: Slot,
                        dir: SemanticDirection,
                        types: RelationshipTypes) extends StreamingOperator {
  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)
  private val getToNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(toSlot)

  override def toString: String = "ExpandInto"

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    IndexedSeq(new OTask(inputMorsel.nextCopy))

  class OTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def workIdentity: WorkIdentity = ExpandIntoOperator.this.workIdentity

    override def toString: String = "ExpandIntoTask"

    private var nodeCursor: NodeCursor = _
    private var groupCursor: RelationshipGroupCursor = _
    private var traversalCursor: RelationshipTraversalCursor = _
    private var relationships: RelationshipSelectionCursor = _
    private var expandInto: CachingExpandInto = _

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      if (expandInto == null) {
        expandInto = new CachingExpandInto(context.transactionalContext.dataRead,
                                           kernelDirection(dir))
      }
      val fromNode = getFromNodeFunction.applyAsLong(inputMorsel)
      val toNode = getToNodeFunction.applyAsLong(inputMorsel)
      if (entityIsNull(fromNode) || entityIsNull(toNode))
        false
      else {
        val pools: CursorPools = resources.cursorPools
        nodeCursor = pools.nodeCursorPool.allocateAndTrace()
        groupCursor = pools.relationshipGroupCursorPool.allocateAndTrace()
        traversalCursor = pools.relationshipTraversalCursorPool.allocateAndTrace()
        relationships = expandInto.connectingRelationships(nodeCursor,
                                                           groupCursor, traversalCursor,
                                                           fromNode,
                                                           types.types(context),
                                                           toNode)
        true
      }
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {

      while (outputRow.isValidRow && relationships.next()) {
        val relId = relationships.relationshipReference()

        // Now we have everything needed to create a row.
        outputRow.copyFrom(inputMorsel)
        outputRow.setLongAt(relOffset, relId)
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
      pools.relationshipGroupCursorPool.free(groupCursor)
      pools.relationshipTraversalCursorPool.free(traversalCursor)
      nodeCursor = null
      groupCursor = null
      traversalCursor = null
      relationships = null
      expandInto = null
    }
  }
  private def kernelDirection(dir: SemanticDirection): Direction = dir match {
    case SemanticDirection.OUTGOING => Direction.OUTGOING
    case SemanticDirection.INCOMING => Direction.INCOMING
    case SemanticDirection.BOTH => Direction.BOTH
  }
}

class ExpandIntoOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                     id: Id,
                                     innermost: DelegateOperatorTaskTemplate,
                                     isHead: Boolean,
                                     fromSlot: Slot,
                                     relName: String,
                                     relOffset: Int,
                                     toSlot: Slot,
                                     dir: SemanticDirection,
                                     types: Array[Int],
                                     missingTypes: Array[String])
                                     (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen, isHead) {
  import OperatorCodeGenHelperTemplates._

  private val nodeCursorField = field[NodeCursor](codeGen.namer.nextVariableName() + "nodeCursor")
  private val groupCursorField = field[RelationshipGroupCursor](codeGen.namer.nextVariableName() + "group")
  private val traversalCursorField = field[RelationshipTraversalCursor](codeGen.namer.nextVariableName() + "traversal")
  private val relationshipsField = field[RelationshipSelectionCursor](codeGen.namer.nextVariableName() + "relationships")
  private val typeField = field[Array[Int]](codeGen.namer.nextVariableName() + "type",
                                            if (types.isEmpty && missingTypes.isEmpty) constant(null)
                                            else arrayOf[Int](types.map(constant):_*)
                                            )
  private val missingTypeField = field[Array[String]](codeGen.namer.nextVariableName() + "missingType",
                                                      arrayOf[String](missingTypes.map(constant):_*))
  private val expandInto = field[CachingExpandInto](codeGen.namer.nextVariableName() + "expandInto")

  codeGen.registerCursor(relName, RelationshipCursorRepresentation(loadField(relationshipsField)))

  override final def scopeId: String = "expandInto" + id.x

  override def genMoreFields: Seq[Field] = {
    val localFields =
      ArrayBuffer(nodeCursorField, groupCursorField, traversalCursorField, relationshipsField, typeField, expandInto)
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
    *    val toNode = inputMorsel.getLongAt(toOffset)
    *    var innerLoop = false
    *    if (fromNode != -1L && toNode != -1L) ) {
    *      nodeCursor = resources.cursorPools.nodeCursorPool.allocate()
    *      groupCursor = resources.cursorPools.relationshipGroupCursorPool.allocate()
    *      traversalCursor = resources.cursorPools.relationshipTraversalCursorPool.allocate()
    *      read.singleNode(node, nodeCursor)
    *      relationships = ExpandIntoCursors(read, nodeCursor, groupCursor, traversalCursor, fromNode, toNode, types)
    *      this.canContinue = relationships.next()
    *      true
    *    }
    * }}}
    *
    */
  override protected def genInitializeInnerLoop: IntermediateRepresentation = {

    val resultBoolean = codeGen.namer.nextVariableName()
    val fromNode = codeGen.namer.nextVariableName() + "fromNode"
    val toNode = codeGen.namer.nextVariableName() + "toNode"

    block(
      declareAndAssign(typeRefOf[Boolean], resultBoolean, constant(false)),
      setField(canContinue, constant(false)),
      declareAndAssign(typeRefOf[Long], fromNode, getNodeIdFromSlot(fromSlot, codeGen)),
      declareAndAssign(typeRefOf[Long], toNode, getNodeIdFromSlot(toSlot, codeGen)),
      condition(and(notEqual(load(fromNode), constant(-1L)), notEqual(load(toNode), constant(-1L)))){
        block(
          loadTypes(types, missingTypes, typeField, missingTypeField),
          condition(isNull(loadField(expandInto)))(
            setField(expandInto, newInstance(constructor[CachingExpandInto, Read, Direction],
                                             loadField(DATA_READ), directionRepresentation(dir)))),
          allocateAndTraceCursor(nodeCursorField, executionEventField, ALLOCATE_NODE_CURSOR),
          allocateAndTraceCursor(groupCursorField, executionEventField, ALLOCATE_GROUP_CURSOR),
          allocateAndTraceCursor(traversalCursorField, executionEventField, ALLOCATE_TRAVERSAL_CURSOR),
          setField(relationshipsField, invoke(loadField(expandInto),
                                              CONNECTING_RELATIONSHIPS,
                                              loadField(nodeCursorField),
                                              loadField(groupCursorField),
                                              loadField(traversalCursorField),
                                              load(fromNode),
                                              loadField(typeField),
                                              load(toNode))),
          invokeSideEffect(loadField(relationshipsField), method[RelationshipSelectionCursor, Unit, KernelReadTracer]("setTracer"), loadField(executionEventField)),
          assign(resultBoolean, constant(true)),
          setField(canContinue, profilingCursorNext[RelationshipSelectionCursor](loadField(relationshipsField), id)),
          )
      },
      load(resultBoolean)
      )
  }

  /**
    * {{{
    *     while (hasDemand && this.canContinue) {
    *       val relId = relationships.relationshipReference()
    *       outputRow.copyFrom(inputMorsel)
    *       outputRow.setLongAt(relOffset, relId)
    *       <<< inner.genOperate() >>>
    *       val tmp = relationship.next()
    *       profileRow(tmp)
    *       this.canContinue = tmp
    *       }
    *     }
    * }}}
    */
  override protected def genInnerLoop: IntermediateRepresentation = {
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(Math.min(codeGen.inputSlotConfiguration.numberOfLongs, codeGen.slots.numberOfLongs),
                              Math.min(codeGen.inputSlotConfiguration.numberOfReferences, codeGen.slots.numberOfReferences)),
        codeGen.setLongAt(relOffset, invoke(loadField(relationshipsField),
                                            method[RelationshipSelectionCursor, Long]("relationshipReference"))),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(setField(canContinue, profilingCursorNext[RelationshipSelectionCursor](loadField(relationshipsField), id))),
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
      freeCursor[RelationshipGroupCursor](loadField(groupCursorField), GroupCursorPool),
      freeCursor[RelationshipTraversalCursor](loadField(traversalCursorField), TraversalCursorPool),
      setField(nodeCursorField, constant(null)),
      setField(groupCursorField, constant(null)),
      setField(traversalCursorField, constant(null)),
      setField(relationshipsField, constant(null)),
      setField(expandInto, constant(null))
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
}

object ExpandIntoOperatorTaskTemplate {

  val CONNECTING_RELATIONSHIPS: Method = method[CachingExpandInto,
      RelationshipSelectionCursor,
      NodeCursor,
      RelationshipGroupCursor,
      RelationshipTraversalCursor,
      Long,
      Array[Int],
      Long]("connectingRelationships")
}




