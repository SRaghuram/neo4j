/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.function.ToLongFunction

import org.neo4j.codegen.api.IntermediateRepresentation.{condition, _}
import org.neo4j.codegen.api._
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, Slot}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.{CompiledHelpers, IntermediateExpression}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.execution.{CursorPools, MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate.{getNodeIdFromSlot, loadTypes}
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DB_ACCESS
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.{NodeCursorRepresentation, OperatorExpressionCompiler, RelationshipCursorRepresentation}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.{DbAccess, ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections._
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

  override def toString: String = "ExpandAll"

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    IndexedSeq(new ExpandAllTask(inputMorsel.nextCopy,
                                 workIdentity,
                                 fromSlot,
                                 relOffset,
                                 toOffset,
                                 dir,
                                 types))

}

class ExpandAllTask(val inputMorsel: MorselExecutionContext,
                    val workIdentity: WorkIdentity,
                    fromSlot: Slot,
                    relOffset: Int,
                    toOffset: Int,
                    dir: SemanticDirection,
                    types: RelationshipTypes) extends InputLoopTask {

  override def toString: String = "ExpandAllTask"

  protected val getFromNodeFunction: ToLongFunction[ExecutionContext] =
    makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)

  /*
  This might look wrong, but it's like this by design. This allows the loop to terminate early and still be
  picked up at any point again - all without impacting the tight loop.
  The mutable state is an unfortunate cost for this feature.
   */
  protected var nodeCursor: NodeCursor = _
  private var groupCursor: RelationshipGroupCursor = _
  private var traversalCursor: RelationshipTraversalCursor = _
  protected var relationships: RelationshipSelectionCursor = _

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
  }

  protected def getRelationshipsCursor(context: QueryContext,
                                       pools: CursorPools,
                                       node: Long,
                                       dir: SemanticDirection,
                                       types: Array[Int]): RelationshipSelectionCursor = {

    val read = context.transactionalContext.dataRead
    read.singleNode(node, nodeCursor)
    if (!nodeCursor.next()) RelationshipSelectionCursor.EMPTY
    else {
      dir match {
        case OUTGOING =>
          if (nodeCursor.isDense) {
            groupCursor = pools.relationshipGroupCursorPool.allocateAndTrace()
            traversalCursor = pools.relationshipTraversalCursorPool.allocateAndTrace()
            outgoingDenseCursor(groupCursor, traversalCursor, nodeCursor, types)
          }
          else {
            traversalCursor = pools.relationshipTraversalCursorPool.allocateAndTrace()
            outgoingSparseCursor(traversalCursor, nodeCursor, types)
          }
        case INCOMING =>
          if (nodeCursor.isDense) {
            groupCursor = pools.relationshipGroupCursorPool.allocateAndTrace()
            traversalCursor = pools.relationshipTraversalCursorPool.allocateAndTrace()
            incomingDenseCursor(groupCursor, traversalCursor, nodeCursor, types)
          }
          else {
            traversalCursor = pools.relationshipTraversalCursorPool.allocateAndTrace()
            incomingSparseCursor(traversalCursor, nodeCursor, types)
          }
        case BOTH =>
          if (nodeCursor.isDense) {
            groupCursor = pools.relationshipGroupCursorPool.allocateAndTrace()
            traversalCursor = pools.relationshipTraversalCursorPool.allocateAndTrace()
            allDenseCursor(groupCursor, traversalCursor, nodeCursor, types)
          }
          else {
            traversalCursor = pools.relationshipTraversalCursorPool.allocateAndTrace()
            allSparseCursor(traversalCursor, nodeCursor, types)
          }
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
  import OperatorCodeGenHelperTemplates._

  protected val nodeCursorField: InstanceField = field[NodeCursor](codeGen.namer.nextVariableName() + "nodeCursor")
  private val groupCursorField = field[RelationshipGroupCursor](codeGen.namer.nextVariableName() + "group")
  private val traversalCursorField = field[RelationshipTraversalCursor](codeGen.namer.nextVariableName() + "traversal")
  protected val relationshipsField: InstanceField = field[RelationshipSelectionCursor](codeGen.namer.nextVariableName() + "relationships")
  private val typeField = field[Array[Int]](codeGen.namer.nextVariableName() + "type",
                                                                                        if (types.isEmpty && missingTypes.isEmpty) constant(null)
                                            else arrayOf[Int](types.map(constant):_*)
                                                                                        )
  private val missingTypeField: InstanceField = field[Array[String]](codeGen.namer.nextVariableName() + "missingType",
                                                                                                  arrayOf[String](missingTypes.map(constant):_*))

  codeGen.registerCursor(relName, RelationshipCursorRepresentation(loadField(relationshipsField)))

  override def scopeId: String = "expandAll" + id.x

  override def genMoreFields: Seq[Field] = {
    val localFields =
      ArrayBuffer(nodeCursorField, groupCursorField, traversalCursorField, relationshipsField, typeField)
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
    val fromNode = codeGen.namer.nextVariableName() + "fromNode"

    block(
      declareAndAssign(typeRefOf[Boolean], resultBoolean, constant(false)),
      setField(canContinue, constant(false)),
      declareAndAssign(typeRefOf[Long], fromNode, getNodeIdFromSlot(fromSlot, codeGen)),
      condition(notEqual(load(fromNode), constant(-1L))){
       block(
         assign(resultBoolean, constant(true)),
         setUpCursors(fromNode),
         setField(canContinue, profilingCursorNext[RelationshipSelectionCursor](loadField(relationshipsField), id))
         )
      },

      load(resultBoolean)
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
          setField(canContinue, profilingCursorNext[RelationshipSelectionCursor](loadField(relationshipsField), id))),
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
      freeCursor[RelationshipGroupCursor](loadField(groupCursorField), GroupCursorPool),
      freeCursor[RelationshipTraversalCursor](loadField(traversalCursorField), TraversalCursorPool),
      setField(nodeCursorField, constant(null)),
      setField(groupCursorField, constant(null)),
      setField(traversalCursorField, constant(null)),
      setField(relationshipsField, constant(null))
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

  protected def getOtherNode: IntermediateRepresentation = invoke(loadField(relationshipsField), otherNodeMethod)
  protected def getRelationship: IntermediateRepresentation = invoke(loadField(relationshipsField),
                                                                     method[RelationshipSelectionCursor, Long]("relationshipReference"))
  protected def setUpCursors(fromNode: String): IntermediateRepresentation = {
    //look if there is already a registered nodeCursor otherwise create and register one
    val externalCursor: Option[NodeCursorRepresentation] = codeGen.cursorFor(fromName) match {
      case Some(cursor: NodeCursorRepresentation) => Some(cursor)
      case _ =>
        codeGen.registerCursor(fromName, NodeCursorRepresentation(loadField(nodeCursorField)))
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
                       method[RelationshipSelectionCursor, Unit, KernelReadTracer]("setTracer"),
                       loadField(executionEventField))
      )
  }

  /**
    * There is an existing nodeCursor pointing at the correct node, we use that when setting up the `RelationshipSelectionCursor`
    */
  private def expandUsingExistingNodeCursor(cursor: NodeCursorRepresentation): IntermediateRepresentation = {
    val (denseMethod, sparseMethod) = findExpansionMethods

    ifElse(cursor.isDense)(
      block(allocateAndTraceCursor(groupCursorField, executionEventField, ALLOCATE_GROUP_CURSOR),
            allocateAndTraceCursor(traversalCursorField, executionEventField, ALLOCATE_TRAVERSAL_CURSOR),
            setField(relationshipsField, invokeStatic(denseMethod,
                                                      loadField(groupCursorField),
                                                      loadField(traversalCursorField),
                                                      cursor.target,
                                                      loadField(typeField))))
      )( //else
         block(
           allocateAndTraceCursor(traversalCursorField, executionEventField, ALLOCATE_TRAVERSAL_CURSOR),
           setField(relationshipsField, invokeStatic(sparseMethod,
                                                     loadField(traversalCursorField),
                                                     cursor.target,
                                                     loadField(typeField)))))
  }

  /**
    *
    * Allocate a nodeCursor and point at the correct node, use that when setting up the `RelationshipSelectionCursor`
    */
  private def expandWithNewNodeCursor(fromNode: String): IntermediateRepresentation = {
    val (denseMethod, sparseMethod) = findExpansionMethods
    block(allocateAndTraceCursor(nodeCursorField, executionEventField, ALLOCATE_NODE_CURSOR),
          singleNode(load(fromNode), loadField(nodeCursorField)),
          ifElse(cursorNext[NodeCursor](loadField(nodeCursorField)))(
            ifElse(invoke(loadField(nodeCursorField), method[NodeCursor, Boolean]("isDense")))(
              block(allocateAndTraceCursor(groupCursorField, executionEventField, ALLOCATE_GROUP_CURSOR),
                    allocateAndTraceCursor(traversalCursorField, executionEventField,
                                           ALLOCATE_TRAVERSAL_CURSOR),
                    setField(relationshipsField, invokeStatic(denseMethod,
                                                              loadField(groupCursorField),
                                                              loadField(traversalCursorField),
                                                              loadField(nodeCursorField),
                                                              loadField(typeField))))
              )( //else
                 block(
                   allocateAndTraceCursor(traversalCursorField, executionEventField,
                                          ALLOCATE_TRAVERSAL_CURSOR),
                   setField(relationshipsField, invokeStatic(sparseMethod,
                                                             loadField(traversalCursorField),
                                                             loadField(nodeCursorField),
                                                             loadField(typeField))))
                 )

            )( //else
               setField(relationshipsField,
                        getStatic[RelationshipSelectionCursor, RelationshipSelectionCursor]("EMPTY"))
               ))
  }

  /**
    * Figures out which expansion methods to use depending on the direction of the direction
    */
  private def findExpansionMethods: (Method, Method) = {
    dir match {
      case OUTGOING =>
        (method[RelationshipSelections, RelationshipSelectionCursor, RelationshipGroupCursor, RelationshipTraversalCursor, NodeCursor, Array[Int]](
          "outgoingDenseCursor"),
          method[RelationshipSelections, RelationshipSelectionCursor, RelationshipTraversalCursor, NodeCursor, Array[Int]](
            "outgoingSparseCursor"))
      case INCOMING =>
        (method[RelationshipSelections, RelationshipSelectionCursor, RelationshipGroupCursor, RelationshipTraversalCursor, NodeCursor, Array[Int]](
          "incomingDenseCursor"),
          method[RelationshipSelections, RelationshipSelectionCursor, RelationshipTraversalCursor, NodeCursor, Array[Int]](
            "incomingSparseCursor"))
      case BOTH =>
        (method[RelationshipSelections, RelationshipSelectionCursor, RelationshipGroupCursor, RelationshipTraversalCursor, NodeCursor, Array[Int]](
          "allDenseCursor"),
          method[RelationshipSelections, RelationshipSelectionCursor, RelationshipTraversalCursor, NodeCursor, Array[Int]](
            "allSparseCursor"))
    }
  }

  private def otherNodeMethod: Method = {
    val otherNode = dir match {
      case OUTGOING => method[RelationshipSelectionCursor, Long]("targetNodeReference")
      case INCOMING => method[RelationshipSelectionCursor, Long]("sourceNodeReference")
      case BOTH => method[RelationshipSelectionCursor, Long]("otherNodeReference")
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

   def loadTypes( types: Array[Int], missingTypes: Array[String], typeField: Field, missingTypeField: Field ): IntermediateRepresentation = {
    if (missingTypes.isEmpty) noop()
    else {
      condition(notEqual(arrayLength(loadField(typeField)), constant(types.length + missingTypes.length))){
        setField(typeField,
                 invokeStatic(method[ExpandAllOperatorTaskTemplate, Array[Int], Array[Int], Array[String], DbAccess]("computeTypes"),
                              loadField(typeField), loadField(missingTypeField), DB_ACCESS))
      }
    }
  }

  def getNodeIdFromSlot(slot: Slot, codeGen: OperatorExpressionCompiler): IntermediateRepresentation = slot match {
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
