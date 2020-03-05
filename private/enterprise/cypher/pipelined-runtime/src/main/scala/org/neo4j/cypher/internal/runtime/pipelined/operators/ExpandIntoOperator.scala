/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators
import java.util.function.ToLongFunction

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.isNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.Method
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.RelationshipCursorRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.execution.CursorPools
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate.getNodeIdFromSlot
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate.loadTypes
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandIntoOperatorTaskTemplate.CONNECTING_RELATIONSHIPS
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_GROUP_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_NODE_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_TRAVERSAL_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.GroupCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NodeCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.TraversalCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.directionRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipGroupCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.CachingExpandInto
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor

import scala.collection.mutable.ArrayBuffer

class ExpandIntoOperator(val workIdentity: WorkIdentity,
                         fromSlot: Slot,
                         relOffset: Int,
                         toSlot: Slot,
                         dir: SemanticDirection,
                         types: RelationshipTypes) extends StreamingOperator {


  override def toString: String = "ExpandInto"

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    IndexedSeq(new ExpandIntoTask(inputMorsel.nextCopy,
      workIdentity,
      fromSlot,
      relOffset,
      toSlot,
      dir,
      types))

}

class ExpandIntoTask(inputMorsel: Morsel,
                     val workIdentity: WorkIdentity,
                     fromSlot: Slot,
                     relOffset: Int,
                     toSlot: Slot,
                     dir: SemanticDirection,
                     types: RelationshipTypes) extends InputLoopTask(inputMorsel) {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  protected val getFromNodeFunction: ToLongFunction[ReadableRow] = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)
  protected val getToNodeFunction: ToLongFunction[ReadableRow] = makeGetPrimitiveNodeFromSlotFunctionFor(toSlot)

  override def toString: String = "ExpandIntoTask"

  protected var nodeCursor: NodeCursor = _
  protected var groupCursor: RelationshipGroupCursor = _
  protected var traversalCursor: RelationshipTraversalCursor = _
  protected var relationships: RelationshipSelectionCursor = _
  protected var expandInto: CachingExpandInto = _

  protected override def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
    if (expandInto == null) {
      expandInto = new CachingExpandInto(state.queryContext.transactionalContext.dataRead,
        kernelDirection(dir))
    }
    val fromNode = getFromNodeFunction.applyAsLong(inputCursor)
    val toNode = getToNodeFunction.applyAsLong(inputCursor)
    if (entityIsNull(fromNode) || entityIsNull(toNode))
      false
    else {
      setupCursors(state.queryContext, resources, fromNode, toNode)
      true
    }
  }

  protected def setupCursors(context: QueryContext,
                             resources: QueryResources,
                             fromNode: Long, toNode: Long) = {
    val pools: CursorPools = resources.cursorPools
    nodeCursor = pools.nodeCursorPool.allocateAndTrace()
    groupCursor = pools.relationshipGroupCursorPool.allocateAndTrace()
    traversalCursor = pools.relationshipTraversalCursorPool.allocateAndTrace()
    relationships = expandInto.connectingRelationships(nodeCursor,
      groupCursor, traversalCursor,
      fromNode,
      types.types(context),
      toNode)
  }

  override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {

    while (outputRow.onValidRow && relationships.next()) {
      val relId = relationships.relationshipReference()

      // Now we have everything needed to create a row.
      outputRow.copyFrom(inputCursor)
      outputRow.setLongAt(relOffset, relId)
      outputRow.next()
    }
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
    if (nodeCursor != null) {
      nodeCursor.setTracer(event)
    }
    if (groupCursor != null) {
      groupCursor.setTracer(event)
    }
    if (traversalCursor != null) {
      traversalCursor.setTracer(event)
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

  protected def kernelDirection(dir: SemanticDirection): Direction = dir match {
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

  private val nodeCursorField = field[NodeCursor](codeGen.namer.nextVariableName("nodeCursor"))
  private val groupCursorField = field[RelationshipGroupCursor](codeGen.namer.nextVariableName("group"))
  private val traversalCursorField = field[RelationshipTraversalCursor](codeGen.namer.nextVariableName("traversal"))
  protected val relationshipsField = field[RelationshipSelectionCursor](codeGen.namer.nextVariableName("relationships"))
  private val typeField = field[Array[Int]](codeGen.namer.nextVariableName("type"),
    if (types.isEmpty && missingTypes.isEmpty) constant(null)
    else arrayOf[Int](types.map(constant):_*)
  )
  private val missingTypeField = field[Array[String]](codeGen.namer.nextVariableName("missingType"),
    arrayOf[String](missingTypes.map(constant):_*))
  private val expandInto = field[CachingExpandInto](codeGen.namer.nextVariableName("expandInto"))

  codeGen.registerCursor(relName, RelationshipCursorRepresentation(loadField(relationshipsField)))

  override def scopeId: String = "expandInto" + id.x

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
    val fromNode = codeGen.namer.nextVariableName("fromNode")
    val toNode = codeGen.namer.nextVariableName("toNode")

    block(
      declareAndAssign(typeRefOf[Boolean], resultBoolean, constant(false)),
      setField(canContinue, constant(false)),
      declareAndAssign(typeRefOf[Long], fromNode, getNodeIdFromSlot(fromSlot, codeGen)),
      declareAndAssign(typeRefOf[Long], toNode, getNodeIdFromSlot(toSlot, codeGen)),
      condition(and(notEqual(load(fromNode), constant(-1L)), notEqual(load(toNode), constant(-1L)))){
        block(
          setUpCursors(fromNode, toNode),
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
        writeRow(getRelationship),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(setField(canContinue, profilingCursorNext[RelationshipSelectionCursor](loadField(relationshipsField), id))),
        endInnerLoop
      )
    )
  }

  /**
   * Writes the relationship
   */
  protected def writeRow(relationship: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      codeGen.copyFromInput(Math.min(codeGen.inputSlotConfiguration.numberOfLongs, codeGen.slots.numberOfLongs),
        Math.min(codeGen.inputSlotConfiguration.numberOfReferences, codeGen.slots.numberOfReferences)),
      codeGen.setLongAt(relOffset, relationship))
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
      condition(isNotNull(loadField(groupCursorField)))(
        invokeSideEffect(loadField(groupCursorField), method[RelationshipGroupCursor, Unit, KernelReadTracer]("setTracer"),
          loadField(executionEventField)),
      ),
      condition(isNotNull(loadField(traversalCursorField)))(
        invokeSideEffect(loadField(traversalCursorField), method[RelationshipTraversalCursor, Unit, KernelReadTracer]("setTracer"),
          loadField(executionEventField)),
      ),
      condition(isNotNull(loadField(nodeCursorField)))(
        invokeSideEffect(loadField(nodeCursorField), method[NodeCursor, Unit, KernelReadTracer]("setTracer"),
          loadField(executionEventField)),
      ),
      inner.genSetExecutionEvent(event)
    )
  }

  protected def getRelationship: IntermediateRepresentation = invoke(loadField(relationshipsField),
    method[RelationshipSelectionCursor, Long]("relationshipReference"))

  protected def setUpCursors(fromNode: String, toNode: String): IntermediateRepresentation = {
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




