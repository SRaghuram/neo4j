/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators


import java.util.function.ToLongFunction

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLength
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.Method
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.NodeCursorRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.RelationshipCursorRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.execution.CursorPools
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate.loadTypes
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_NODE_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_TRAVERSAL_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NodeCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.TraversalCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.cursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getNodeIdFromSlot
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.singleNode
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections
import org.neo4j.kernel.impl.newapi.Cursors
import org.neo4j.kernel.impl.newapi.Cursors.emptyTraversalCursor

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ExpandAllOperator(val workIdentity: WorkIdentity,
                        fromSlot: Slot,
                        relOffset: Int,
                        toOffset: Int,
                        dir: SemanticDirection,
                        types: RelationshipTypes) extends StreamingOperator {

  override def toString: String = "ExpandAll"

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    singletonIndexedSeq(new ExpandAllTask(inputMorsel.nextCopy,
      workIdentity,
      fromSlot,
      relOffset,
      toOffset,
      dir,
      types))

}

class ExpandAllTask(inputMorsel: Morsel,
                    val workIdentity: WorkIdentity,
                    fromSlot: Slot,
                    relOffset: Int,
                    toOffset: Int,
                    dir: SemanticDirection,
                    types: RelationshipTypes) extends InputLoopTask(inputMorsel) {

  override def toString: String = "ExpandAllTask"

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  protected val getFromNodeFunction: ToLongFunction[ReadableRow] =
  makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)

  /*
  This might look wrong, but it's like this by design. This allows the loop to terminate early and still be
  picked up at any point again - all without impacting the tight loop.
  The mutable state is an unfortunate cost for this feature.
   */
  protected var nodeCursor: NodeCursor = _
  private var traversalCursor: RelationshipTraversalCursor = _
  protected var relationships: RelationshipTraversalCursor = _

  protected override def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
    val fromNode = getFromNodeFunction.applyAsLong(inputCursor)
    if (entityIsNull(fromNode))
      false
    else {
      val pools: CursorPools = resources.cursorPools
      nodeCursor = pools.nodeCursorPool.allocateAndTrace()
      relationships = getRelationshipsCursor(state.queryContext, pools, fromNode, dir, types.types(state.queryContext))
      true
    }
  }

  override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {

    while (outputRow.onValidRow && relationships.next()) {
      val relId = relationships.relationshipReference()
      val otherSide = relationships.otherNodeReference()

      // Now we have everything needed to create a row.
      outputRow.copyFrom(inputCursor)
      outputRow.setLongAt(relOffset, relId)
      outputRow.setLongAt(toOffset, otherSide)
      outputRow.next()
    }
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
    if (nodeCursor != null) {
      nodeCursor.setTracer(event)
    }
    if (traversalCursor != null) {
      traversalCursor.setTracer(event)
    }
  }

  override protected def closeInnerLoop(resources: QueryResources): Unit = {
    val pools = resources.cursorPools
    pools.nodeCursorPool.free(nodeCursor)
    pools.relationshipTraversalCursorPool.free(traversalCursor)
    nodeCursor = null
    traversalCursor = null
  }

  protected def getRelationshipsCursor(context: QueryContext,
                                       pools: CursorPools,
                                       node: Long,
                                       dir: SemanticDirection,
                                       types: Array[Int]): RelationshipTraversalCursor = {

    val read = context.transactionalContext.dataRead
    read.singleNode(node, nodeCursor)
    if (!nodeCursor.next()) emptyTraversalCursor(read)
    else {
      traversalCursor = pools.relationshipTraversalCursorPool.allocateAndTrace()
      dir match {
        case OUTGOING =>
          RelationshipSelections.outgoingCursor(traversalCursor, nodeCursor, types)
        case INCOMING =>
          RelationshipSelections.incomingCursor(traversalCursor, nodeCursor, types)
        case BOTH =>
          RelationshipSelections.allCursor(traversalCursor, nodeCursor, types)
      }
    }
  }
}

class ExpandAllOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                    id: Id,
                                    innermost: DelegateOperatorTaskTemplate,
                                    isHead: Boolean,
                                    fromName: String,
                                    fromSlot: Slot,
                                    relName: String,
                                    relOffset: Int,
                                    toOffset: Int,
                                    dir: SemanticDirection,
                                    types: Array[Int],
                                    missingTypes: Array[String])
                                   (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen, isHead) {

  protected val nodeCursorField: InstanceField = field[NodeCursor](codeGen.namer.nextVariableName("nodeCursor"))
  private val traversalCursorField: InstanceField = field[RelationshipTraversalCursor](codeGen.namer.nextVariableName("traversal"))
  protected val relationshipsField: InstanceField = field[RelationshipTraversalCursor](codeGen.namer.nextVariableName("relationships"))
  private val typeField = field[Array[Int]](codeGen.namer.nextVariableName("type"),
    if (types.isEmpty && missingTypes.isEmpty) constant(null)
    else arrayOf[Int](types.map(constant):_*))
  private val missingTypeField: InstanceField = field[Array[String]](codeGen.namer.nextVariableName("missingType"),
    arrayOf[String](missingTypes.map(constant):_*))

  codeGen.registerCursor(relName, RelationshipCursorRepresentation(loadField(relationshipsField)))

  override def scopeId: String = "expandAll" + id.x

  override def genMoreFields: Seq[Field] = {
    val localFields =
      ArrayBuffer[Field](nodeCursorField, traversalCursorField, relationshipsField, typeField)
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
   *     val tmp = nodeCursor.next()
   *     profileRow(tmp)
   *     this.canContinue = tmp
   *     true
   *    }
   * }}}
   *
   */
  override protected def genInitializeInnerLoop: IntermediateRepresentation = {

    val resultBoolean = codeGen.namer.nextVariableName()
    val fromNode = codeGen.namer.nextVariableName("fromNode")

    block(
      declareAndAssign(typeRefOf[Boolean], resultBoolean, constant(false)),
      setField(canContinue, constant(false)),
      declareAndAssign(fromNode, getNodeIdFromSlot(fromSlot, codeGen)),
      condition(notEqual(fromNode, constant(-1L))){
        block(
          assign(resultBoolean, constant(true)),
          setUpCursors(fromNode, canBeNull = false),
          setField(canContinue, profilingCursorNext[RelationshipTraversalCursor](loadField(relationshipsField), id, doProfile, codeGen.namer))
        )
      },

      load[Boolean](resultBoolean)
    )
  }

  /**
   * {{{
   *     while (hasDemand && this.canContinue) {
   *       val relId = relationships.relationshipReference()
   *       val otherSide = relationships.otherNodeReference()
   *       outputRow.copyFrom(inputMorsel)
   *       outputRow.setLongAt(relOffset, relId)
   *       outputRow.setLongAt(toOffset, otherSide)
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
        writeRow(getRelationship, getOtherNode),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(
          innermost.setUnlessPastLimit(canContinue, profilingCursorNext[RelationshipTraversalCursor](loadField(relationshipsField), id, doProfile, codeGen.namer))),
        endInnerLoop
      )
    )
  }

  /**
   * Writes the relationship and the target node
   */
  protected def writeRow(relationship: IntermediateRepresentation,
                         otherNode: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      codeGen.copyFromInput(Math.min(codeGen.inputSlotConfiguration.numberOfLongs, codeGen.slots.numberOfLongs),
        Math.min(codeGen.inputSlotConfiguration.numberOfReferences,
          codeGen.slots.numberOfReferences)),
      codeGen.setLongAt(relOffset, relationship),
      codeGen.setLongAt(toOffset, otherNode)
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
      freeCursor[RelationshipTraversalCursor](loadField(traversalCursorField), TraversalCursorPool),
      setField(nodeCursorField, constant(null)),
      setField(traversalCursorField, constant(null)),
      setField(relationshipsField, constant(null))
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = {
    block(
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

  protected def getOtherNode: IntermediateRepresentation = invoke(loadField(relationshipsField), otherNodeMethod)
  protected def getRelationship: IntermediateRepresentation = invoke(loadField(relationshipsField),
    method[RelationshipTraversalCursor, Long]("relationshipReference"))
  protected def setUpCursors(fromNode: String, canBeNull: Boolean): IntermediateRepresentation = {
    //look if there is already a registered nodeCursor otherwise create and register one
    val externalCursor: Option[NodeCursorRepresentation] = codeGen.cursorFor(fromName) match {
      case Some(cursor: NodeCursorRepresentation) => Some(cursor)
      case _ =>
        codeGen.registerCursor(fromName, NodeCursorRepresentation(loadField(nodeCursorField), canBeNull, codeGen))
        None
    }

    block(
      loadTypes(types, missingTypes, typeField, missingTypeField),
      externalCursor
        //specialize if we have an external NodeCursor already pointing at the correct offset
        .map(expandUsingExistingNodeCursor)
        //otherwise we need to set up a cursor to point at the from node
        .getOrElse(expandWithNewNodeCursor(fromNode)),
      invokeSideEffect(loadField(relationshipsField),
        method[RelationshipTraversalCursor, Unit, KernelReadTracer]("setTracer"),
        loadField(executionEventField))
    )
  }

  /**
   * There is an existing nodeCursor pointing at the correct node, we use that when setting up the `RelationshipSelectionCursor`
   */
  private def expandUsingExistingNodeCursor(cursor: NodeCursorRepresentation): IntermediateRepresentation = {
    val expandMethod = findExpansionMethod

    block(
      allocateAndTraceCursor(traversalCursorField, executionEventField, ALLOCATE_TRAVERSAL_CURSOR, doProfile),
      setField(relationshipsField, invokeStatic(expandMethod,
        loadField(traversalCursorField),
        cursor.target,
        loadField(typeField))))
  }

  /**
   *
   * Allocate a nodeCursor and point at the correct node, use that when setting up the `RelationshipSelectionCursor`
   */
  private def expandWithNewNodeCursor(fromNode: String): IntermediateRepresentation = {
    val expandMethod = findExpansionMethod
    block(allocateAndTraceCursor(nodeCursorField, executionEventField, ALLOCATE_NODE_CURSOR, doProfile),
      singleNode(load[Long](fromNode), loadField(nodeCursorField)),
      ifElse(cursorNext[NodeCursor](loadField(nodeCursorField)))(
        block(
          allocateAndTraceCursor(traversalCursorField, executionEventField, ALLOCATE_TRAVERSAL_CURSOR, doProfile),
          setField(relationshipsField, invokeStatic(expandMethod,
            loadField(traversalCursorField),
            loadField(nodeCursorField),
            loadField(typeField))))
      )( //else
        setField(relationshipsField,
          invokeStatic(method[Cursors, RelationshipTraversalCursor, Read]("emptyTraversalCursor"), loadField(DATA_READ)))
      ))
  }

  /**
   * Figures out which expansion methods to use depending on the direction of the direction
   */
  private def findExpansionMethod: Method = {
    dir match {
      case OUTGOING =>
        method[RelationshipSelections, RelationshipTraversalCursor, RelationshipTraversalCursor, NodeCursor, Array[Int]](
          "outgoingCursor")
      case INCOMING =>
        method[RelationshipSelections, RelationshipTraversalCursor, RelationshipTraversalCursor, NodeCursor, Array[Int]](
          "incomingCursor")
      case BOTH =>
        method[RelationshipSelections, RelationshipTraversalCursor, RelationshipTraversalCursor, NodeCursor, Array[Int]](
          "allCursor")
    }
  }

  private def otherNodeMethod: Method = {
    val otherNode = dir match {
      case OUTGOING => method[RelationshipTraversalCursor, Long]("targetNodeReference")
      case INCOMING => method[RelationshipTraversalCursor, Long]("sourceNodeReference")
      case BOTH => method[RelationshipTraversalCursor, Long]("otherNodeReference")
    }
    otherNode
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

  def loadTypes(types: Seq[Int], missingTypes: Seq[String], typeField: Field, missingTypeField: Field): IntermediateRepresentation = {
    if (missingTypes.isEmpty) {
      noop()
    } else {
      condition(notEqual(arrayLength(loadField(typeField)), constant(types.length + missingTypes.length))) {
        setField(typeField,
          invokeStatic(method[ExpandAllOperatorTaskTemplate, Array[Int], Array[Int], Array[String], DbAccess]("computeTypes"),
            loadField(typeField), loadField(missingTypeField), DB_ACCESS))
      }
    }
  }
}
