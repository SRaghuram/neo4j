/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.Method
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledHelpers
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.PROPERTY_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.vPROPERTY_CURSOR
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate.getNodeIdFromSlot
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate.loadTypes
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_NODE_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_TRAVERSAL_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NO_TOKEN
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NodeCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.TraversalCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.cursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.singleNode
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPropertyKeys
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ExpandAllOperator(val workIdentity: WorkIdentity,
                        fromSlot: Slot,
                        relOffset: Int,
                        toOffset: Int,
                        dir: SemanticDirection,
                        types: RelationshipTypes,
                        nodePropsToRead: Option[SlottedPropertyKeys],
                        relsPropsToRead: Option[SlottedPropertyKeys]) extends StreamingOperator {

  override def toString: String = "ExpandAll"

  override protected def nextTasks(state: PipelinedQueryState,
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
      types,
      nodePropsToRead,
      relsPropsToRead))

}

class ExpandAllTask(inputMorsel: Morsel,
                    val workIdentity: WorkIdentity,
                    fromSlot: Slot,
                    relOffset: Int,
                    toOffset: Int,
                    dir: SemanticDirection,
                    types: RelationshipTypes,
                    nodePropsToRead: Option[SlottedPropertyKeys],
                    relsPropsToRead: Option[SlottedPropertyKeys]) extends InputLoopTask(inputMorsel) {

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
  private var propertyCursor: PropertyCursor = _

  protected override def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
    val fromNode = getFromNodeFunction.applyAsLong(inputCursor)
    if (entityIsNull(fromNode))
      false
    else {
      val pools: CursorPools = resources.cursorPools
      nodeCursor = pools.nodeCursorPool.allocateAndTrace()
      relationships = getRelationshipsCursor(state.queryContext, pools, fromNode, dir, types.types(state.queryContext))
      if (nodePropsToRead.isDefined || relsPropsToRead.isDefined) {
        propertyCursor = resources.expressionCursors.propertyCursor
      }
      true
    }
  }

  override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {

    cacheNodeProperties(outputRow, state.queryContext)
    while (outputRow.onValidRow && relationships.next()) {
      cacheRelationshipProperties(outputRow, state.queryContext)
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
    if (!nodeCursor.next()) RelationshipTraversalCursor.EMPTY
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

  private def cacheNodeProperties(outputRow: MorselFullCursor, queryContext: QueryContext): Unit = {
    nodePropsToRead.foreach(p => {
      nodeCursor.properties(propertyCursor)
      while (propertyCursor.next() && p.accept(queryContext, propertyCursor.propertyKey())) {
        outputRow.setCachedPropertyAt(p.offset, propertyCursor.propertyValue())
      }
    })
  }

  private def cacheRelationshipProperties(outputRow: MorselFullCursor, queryContext: QueryContext): Unit = {
    relsPropsToRead.foreach(p => {
      relationships.properties(propertyCursor)
      while (propertyCursor.next() && p.accept(queryContext, propertyCursor.propertyKey())) {
        outputRow.setCachedPropertyAt(p.offset, propertyCursor.propertyValue())
      }
    })
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
                                    missingTypes: Array[String],
                                    nodePropsToRead: Option[SlottedPropertyKeys],
                                    relsPropsToRead: Option[SlottedPropertyKeys])
                                   (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen, isHead) {

  protected val nodeCursorField: InstanceField = field[NodeCursor](codeGen.namer.nextVariableName("nodeCursor"))
  private val traversalCursorField: InstanceField = field[RelationshipTraversalCursor](codeGen.namer.nextVariableName("traversal"))
  protected val relationshipsField: InstanceField = field[RelationshipTraversalCursor](codeGen.namer.nextVariableName("relationships"))
  private val typeField = field[Array[Int]](codeGen.namer.nextVariableName("type"),
    if (types.isEmpty && missingTypes.isEmpty) constant(null)
    else arrayOf[Int](types.map(constant):_*))
  private val missingTypeField: InstanceField = field[Array[String]](codeGen.namer.nextVariableName("missingType"),
    arrayOf[String](missingTypes.map(constant):_*))
 private val missingProperties = (nodePropsToRead.map(_.unresolved).getOrElse(Seq.empty[(String,Int)]) ++ relsPropsToRead.map(_.unresolved).getOrElse(Seq.empty[(String,Int)])).toMap.map {
   case (name, _) => name -> field[Int](codeGen.namer.nextVariableName(name), NO_TOKEN)
 }

  nodePropsToRead.map(k => {
    k.resolved
  })

  codeGen.registerCursor(relName, RelationshipCursorRepresentation(loadField(relationshipsField)))

  override def scopeId: String = "expandAll" + id.x

  override def genMoreFields: Seq[Field] = {
    val localFields =
      ArrayBuffer[Field](nodeCursorField, traversalCursorField, relationshipsField, typeField)
    if (missingTypes.nonEmpty) {
      localFields += missingTypeField
    }
    missingProperties.foreach {
      case (_, field) => localFields += field
    }

    localFields
  }

  override def genLocalVariables: Seq[LocalVariable] = {
    if (nodePropsToRead.nonEmpty || relsPropsToRead.nonEmpty) {
      Seq(CURSOR_POOL_V, vPROPERTY_CURSOR)
    } else {
      Seq(CURSOR_POOL_V)
    }
  }

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
      declareAndAssign(typeRefOf[Long], fromNode, getNodeIdFromSlot(fromSlot, codeGen)),
      condition(notEqual(load(fromNode), constant(-1L))){
        block(
          assign(resultBoolean, constant(true)),
          setUpCursors(fromNode),
          setField(canContinue, profilingCursorNext[RelationshipTraversalCursor](loadField(relationshipsField), id))
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
        cacheProperties(relsPropsToRead, invokeSideEffect(loadField(relationshipsField), method[RelationshipTraversalCursor, Unit, PropertyCursor]("properties"), PROPERTY_CURSOR)),
        writeRow(getRelationship, getOtherNode),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(
          innermost.setUnlessPastLimit(canContinue, profilingCursorNext[RelationshipTraversalCursor](loadField(relationshipsField), id))),
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
      cacheProperties(nodePropsToRead, invokeSideEffect(cursor.target, method[NodeCursor, Unit, PropertyCursor]("properties"), PROPERTY_CURSOR)),
      allocateAndTraceCursor(traversalCursorField, executionEventField, ALLOCATE_TRAVERSAL_CURSOR),
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
    block(allocateAndTraceCursor(nodeCursorField, executionEventField, ALLOCATE_NODE_CURSOR),
      singleNode(load(fromNode), loadField(nodeCursorField)),
      ifElse(cursorNext[NodeCursor](loadField(nodeCursorField)))(
        block(
          cacheProperties(nodePropsToRead, invokeSideEffect(loadField(nodeCursorField), method[NodeCursor, Unit, PropertyCursor]("properties"), PROPERTY_CURSOR)),
          allocateAndTraceCursor(traversalCursorField, executionEventField,
            ALLOCATE_TRAVERSAL_CURSOR),
          setField(relationshipsField, invokeStatic(expandMethod,
            loadField(traversalCursorField),
            loadField(nodeCursorField),
            loadField(typeField))))
      )( //else
        setField(relationshipsField,
          getStatic[RelationshipTraversalCursor, RelationshipTraversalCursor]("EMPTY"))
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

  private def cacheProperties(props: Option[SlottedPropertyKeys], setupPropertyCursor: IntermediateRepresentation) = {
    props.map(p => {
      val resolvedOps = p.resolved.map {
        case (token, offset) =>
          codeGen.setCachedPropertyAt(offset,
            ternary(
              invoke(PROPERTY_CURSOR, method[PropertyCursor, Boolean, Int]("seekProperty"), constant(token)),
              invoke(PROPERTY_CURSOR, method[PropertyCursor, Value]("propertyValue")),
              noValue
            ))
      }
      val unResolvedOps = p.unresolved.map {
        case (name, offset) =>
          val f = missingProperties(name)
          block(
            condition(equal(loadField(f), NO_TOKEN)) {
              setField(f, OperatorCodeGenHelperTemplates.propertyKeyId(name))
            },
            codeGen.setCachedPropertyAt(offset,
              ternary(
                invoke(PROPERTY_CURSOR, method[PropertyCursor, Boolean, Int]("seekProperty"), loadField(f)),
                invoke(PROPERTY_CURSOR, method[PropertyCursor, Value]("propertyValue")),
                noValue
              )
            )
          )
      }
      block(setupPropertyCursor +: (resolvedOps ++ unResolvedOps):_*)
    }
    ).getOrElse(noop())
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
