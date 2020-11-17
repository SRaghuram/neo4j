/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.add
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLength
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNaN
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.Method
import org.neo4j.cypher.internal.frontend.helpers.SeqCombiner.combine
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.CompositeValueIndexCursor
import org.neo4j.cypher.internal.runtime.KernelAPISupport.RANGE_SEEKABLE_VALUE_GROUPS
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.ValuedNodeIndexCursor
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledHelpers
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeek
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekMode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeIndexSeeker
import org.neo4j.cypher.internal.runtime.pipelined.NodeIndexCursorRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.CursorPools
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyQueriesNodeIndexSeekTaskTemplate.createCursorsMethod
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyQueriesNodeIndexSeekTaskTemplate.isImpossible
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_NODE_INDEX_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NodeValueIndexCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.asStorableValue
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.indexOrder
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.indexReadSession
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.nodeIndexSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexQuery
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate
import org.neo4j.internal.kernel.api.IndexQueryConstraints
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

class NodeIndexSeekOperator(val workIdentity: WorkIdentity,
                            offset: Int,
                            properties: Array[SlottedIndexedProperty],
                            queryIndexId: Int,
                            indexOrder: IndexOrder,
                            argumentSize: SlotConfiguration.Size,
                            valueExpr: QueryExpression[Expression],
                            indexMode: IndexSeekMode = IndexSeek)
  extends StreamingOperator {


  private val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
  private val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot)
  private val propertyIds: Array[Int] = properties.map(_.propertyKeyId)


  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    singletonIndexedSeq(
      new NodeIndexSeekTask(
        inputMorsel.nextCopy,
        workIdentity,
        offset,
        indexPropertyIndices,
        indexPropertySlotOffsets,
        queryIndexId,
        indexOrder,
        argumentSize,
        propertyIds,
        valueExpr,
        indexMode)
    )
  }
}

class NodeIndexSeekTask(inputMorsel: Morsel,
                        val workIdentity: WorkIdentity,
                        offset: Int,
                        indexPropertyIndices: Array[Int],
                        indexPropertySlotOffsets: Array[Int],
                        queryIndexId: Int,
                        indexOrder: IndexOrder,
                        argumentSize: SlotConfiguration.Size,
                        val propertyIds: Array[Int],
                        val valueExpr: QueryExpression[Expression],
                        val indexMode: IndexSeekMode = IndexSeek) extends InputLoopTask(inputMorsel) with NodeIndexSeeker {

  protected var nodeCursor: NodeValueIndexCursor = _
  private var cursorsToClose: Array[NodeValueIndexCursor] = _
  private var exactSeekValues: Array[Value] = _

  protected def onNode(node: Long): Unit = {
    //hook for extending classes
  }

  protected def onExitInnerLoop(): Unit = {
    //hook for extending classes
  }

  // INPUT LOOP TASK
  override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
    val queryState = state.queryStateForExpressionEvaluation(resources)
    initExecutionContext.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
    val indexQueries = computeIndexQueries(queryState, initExecutionContext)
    val read = state.query.transactionalContext.transaction.dataRead
    if (indexQueries.size == 1) {
      nodeCursor = resources.cursorPools.nodeValueIndexCursorPool.allocateAndTrace()
      cursorsToClose = Array(nodeCursor)
      seek(state.queryIndexes(queryIndexId), nodeCursor, read, indexQueries.head)
    } else {
      // If we should use ValuedNodeIndexCursors here at some point, remember to not free them into the same pool
      cursorsToClose = indexQueries.filterNot(isImpossible).map(query => {
        val cursor = resources.cursorPools.nodeValueIndexCursorPool.allocateAndTrace()
        read.nodeIndexSeek(state.queryIndexes(queryIndexId), cursor, IndexQueryConstraints.constrained(indexOrder, needsValues || indexOrder != IndexOrder.NONE), query: _*)
        cursor
      }).toArray
      nodeCursor = orderedCursor(indexOrder, cursorsToClose)
    }

    true
  }

  override final protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
    while (outputRow.onValidRow && nodeCursor != null && nodeCursor.next()) {
      val node = nodeCursor.nodeReference()
      onNode(node)
      outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
      outputRow.setLongAt(offset, node)
      var i = 0
      while (i < indexPropertyIndices.length) {
        val indexPropertyIndex = indexPropertyIndices(i)
        val value =
          if (exactSeekValues != null) {
            exactSeekValues(indexPropertyIndex)
          } else {
            nodeCursor.propertyValue(indexPropertyIndex)
          }
        outputRow.setCachedPropertyAt(indexPropertySlotOffsets(i), value)
        i += 1
      }
      outputRow.next()
    }
    onExitInnerLoop()
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
    if (nodeCursor != null) {
      nodeCursor.setTracer(event)
    }
  }

  override protected def closeInnerLoop(resources: QueryResources): Unit = {
    if (cursorsToClose != null) {
      cursorsToClose.foreach(resources.cursorPools.nodeValueIndexCursorPool.free)
      cursorsToClose = null
    }
    nodeCursor = null
  }

  // HELPERS
  protected def orderedCursor(indexOrder: IndexOrder, cursors: Array[NodeValueIndexCursor]): NodeValueIndexCursor = indexOrder match {
    case IndexOrder.NONE => CompositeValueIndexCursor.unordered(cursors)
    case IndexOrder.ASCENDING => CompositeValueIndexCursor.ascending(cursors)
    case IndexOrder.DESCENDING => CompositeValueIndexCursor.descending(cursors)
  }
  private def needsValues: Boolean = indexPropertyIndices.nonEmpty

  private def seek[RESULT <: AnyRef](index: IndexReadSession,
                                     nodeCursor: NodeValueIndexCursor,
                                     read: Read,
                                     predicates: Seq[IndexQuery]): Unit = {



    if (isImpossible(predicates)) {
      return // leave cursor un-initialized/empty
    }

    // We don't need property values from the index for an exact seek
    exactSeekValues =
      if (needsValues && predicates.forall(_.isInstanceOf[ExactPredicate])) {
        predicates.map(_.asInstanceOf[ExactPredicate].value()).toArray
      } else {
        null
      }
    val needsValuesFromIndexSeek = exactSeekValues == null && needsValues
    read.nodeIndexSeek(index, nodeCursor, IndexQueryConstraints.constrained(indexOrder, needsValuesFromIndexSeek), predicates: _*)
  }
}

class NodeWithValues(val nodeId: Long, val values: Array[Value])

abstract class SingleQueryNodeIndexSeekTaskTemplate(override val inner: OperatorTaskTemplate,
                                                    id: Id,
                                                    innermost: DelegateOperatorTaskTemplate,
                                                    nodeVarName: String,
                                                    offset: Int,
                                                    property: SlottedIndexedProperty,
                                                    order: IndexOrder,
                                                    needsValues: Boolean,
                                                    queryIndexId: Int,
                                                    argumentSize: SlotConfiguration.Size,
                                                    codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  protected val nodeIndexCursorField: InstanceField = field[NodeValueIndexCursor](codeGen.namer.nextVariableName())

  codeGen.registerCursor(nodeVarName, NodeIndexCursorRepresentation(loadField(nodeIndexCursorField)))

  /**
   * Return the value of the property if we need to cache it.
   *
   * This value is either provided by the index in the case of a range seek,
   * or we already have the value in the case of an exact seek.
   */
  protected def getPropertyValue: IntermediateRepresentation

  /**
   * Extension point called at the start of inner-loop initializaton
   */
  protected def beginInnerLoop: IntermediateRepresentation

  /**
   * Checks whether the predicate is possible.
   *
   * Examples of impossible predicates is doing an exact seek for NO_VALUE
   * or a range seek for some types where range seeks are not supported. In
   * these cases we don't set up cursors etc but we'll just return no results
   * @return `true` if the predicate is possible otherwise `false`
   */
  protected def isPredicatePossible: IntermediateRepresentation

  /**
   * Returns the predicate used in `Read::nodeIndexSeek`
   *
   * The predicate is for example an exact seek or a range seek
   * @return The predicate used in the seek query
   */
  protected def predicate: IntermediateRepresentation

  override def genMoreFields: Seq[Field] = Seq(nodeIndexCursorField)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val hasInnerLoopVar = codeGen.namer.nextVariableName()
    /**
     * {{{
     *   [extension point]
     *   val hasInnerLoop = [isPredicatePossible]
     *   if (hasInnerLoop) {
     *     nodeIndexCursor = resources.cursorPools.nodeValueIndexCursorPool.allocate()
     *     context.transactionalContext.dataRead.nodeIndexSeek(indexReadSession(queryIndexId),
     *                                                         nodeIndexCursor,
     *                                                         [predicate])
     *     this.canContinue = nodeLabelCursor.next()
     *    }
     *    hasInnerLoop
     * }}}
     */
    block(
      beginInnerLoop,
      declareAndAssign(typeRefOf[Boolean], hasInnerLoopVar, isPredicatePossible),
      setField(canContinue, constant(false)),
      condition(load(hasInnerLoopVar))(
        block(
          allocateAndTraceCursor(nodeIndexCursorField, executionEventField, ALLOCATE_NODE_INDEX_CURSOR, doProfile),
          nodeIndexSeek(indexReadSession(queryIndexId), loadField(nodeIndexCursorField), predicate, order, needsValues),
          setField(canContinue, profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id, doProfile))
        )),
      load(hasInnerLoopVar)
    )
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     ...
     *     setLongAt(offset, nodeIndexCursor.nodeReference())
     *     setCachedPropertyAt(offset, [getPropertyValue])
     *     << inner.genOperate >>
     *     this.canContinue = this.nodeIndexCursor.next()
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        codeGen.setLongAt(offset, invoke(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Long]("nodeReference"))),
        property.maybeCachedNodePropertySlot.map(codeGen.setCachedPropertyAt(_, getPropertyValue)).getOrElse(noop()),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(innermost.setUnlessPastLimit(canContinue, profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id, doProfile))),
        endInnerLoop
      )
    )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   resources.cursorPools.nodeValueIndexCursorPool.free(nodeIndexCursor)
     *   nodeIndexCursor = null
     * }}}
     */
    block(
      freeCursor[NodeValueIndexCursor](loadField(nodeIndexCursorField), NodeValueIndexCursorPool),
      setField(nodeIndexCursorField, constant(null))
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(nodeIndexCursorField)))(
        invokeSideEffect(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Unit, KernelReadTracer]("setTracer"), event)
      ),
      inner.genSetExecutionEvent(event)
    )
}

/**
 * Code generation template for index seeks of the form `MATCH (n:L) WHERE n.prop = 42`
 */
class SingleExactSeekQueryNodeIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                    id: Id,
                                                    innermost: DelegateOperatorTaskTemplate,
                                                    nodeVarName: String,
                                                    offset: Int,
                                                    property: SlottedIndexedProperty,
                                                    generateSeekValue: () => IntermediateExpression,
                                                    queryIndexId: Int,
                                                    argumentSize: SlotConfiguration.Size)
                                                   (codeGen: OperatorExpressionCompiler) extends
                                                                                         SingleQueryNodeIndexSeekTaskTemplate(inner, id, innermost, nodeVarName, offset, property, IndexOrder.NONE, false, queryIndexId,
                                                                                           argumentSize, codeGen) {

  private var seekValue: IntermediateExpression = _
  private val seekValueVariable = variable[Value](codeGen.namer.nextVariableName(), constant(null))

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V, seekValueVariable)

  override def genExpressions: Seq[IntermediateExpression] = Seq(seekValue)

  override protected def getPropertyValue: IntermediateRepresentation = load(seekValueVariable)

  override protected def beginInnerLoop: IntermediateRepresentation = {
    if (seekValue == null) {
      val value = generateSeekValue()
      seekValue = value.copy(ir = asStorableValue(nullCheckIfRequired(value)))
    }
    assign(seekValueVariable, seekValue.ir)
  }

  override protected def isPredicatePossible: IntermediateRepresentation =
    and(notEqual(load(seekValueVariable), noValue), not(isNaN(load(seekValueVariable))))

  override protected def predicate: IntermediateRepresentation =
    invokeStatic(method[IndexQuery, ExactPredicate, Int, Object]("exact"), constant(property.propertyKeyId), load(seekValueVariable))
}

/**
 * Code generation template for range seeks, e.g. `n.prop < 42`, `n.prop >= 42`, and `42 < n.prop <= 43`
 */
class SingleRangeSeekQueryNodeIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                    id: Id,
                                                    innermost: DelegateOperatorTaskTemplate,
                                                    nodeVarName: String,
                                                    offset: Int,
                                                    property: SlottedIndexedProperty,
                                                    seekExpression: SeekExpression,
                                                    queryIndexId: Int,
                                                    order: IndexOrder,
                                                    argumentSize: SlotConfiguration.Size)
                                                   (codeGen: OperatorExpressionCompiler) extends
                                                                                         SingleQueryNodeIndexSeekTaskTemplate(inner, id, innermost, nodeVarName, offset, property, order, property.getValueFromIndex, queryIndexId,
                                                                                           argumentSize, codeGen) {
  private var seekValues: Seq[IntermediateExpression] = _
  private val predicateVar = variable[IndexQuery](codeGen.namer.nextVariableName(), constant(null))

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V, predicateVar)
  override def genExpressions: Seq[IntermediateExpression] = seekValues
  override protected def getPropertyValue: IntermediateRepresentation =
    invoke(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Value, Int]("propertyValue"), constant(0))
  override protected def beginInnerLoop: IntermediateRepresentation = {
    if (seekValues == null) {
      seekValues = seekExpression.generateSeekValues.map(_ ()).map(v => v.copy(ir = nullCheckIfRequired(v)))
    }
    assign(predicateVar, seekExpression.generatePredicate(seekValues.map(_.ir)))
  }
  override protected def isPredicatePossible: IntermediateRepresentation =
    invokeStatic(method[CompiledHelpers, Boolean, IndexQuery]("possibleRangePredicate"), predicate)
  override protected def predicate: IntermediateRepresentation = load(predicateVar)
}

abstract class BaseManyQueriesNodeIndexSeekTaskTemplate(override val inner: OperatorTaskTemplate,
                                           id: Id,
                                           innermost: DelegateOperatorTaskTemplate,
                                           offset: Int,
                                           properties: Seq[SlottedIndexedProperty],
                                           order: IndexOrder,
                                           argumentSize: SlotConfiguration.Size,
                                           codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {
  private val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2).toArray
  private val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot).toArray
  protected val needsValues: Boolean = indexPropertyIndices.nonEmpty
  private val nodeIndexCursorField = field[NodeValueIndexCursor](codeGen.namer.nextVariableName("indexCursor"))
  private val nodeCursorsToCloseField = field[Array[NodeValueIndexCursor]](codeGen.namer.nextVariableName("cursorsToClose"))

  def createCursorArray: IntermediateRepresentation
  def getCursorToFree(cursor: IntermediateRepresentation): IntermediateRepresentation
  override def genMoreFields: Seq[Field] = Seq(nodeIndexCursorField, nodeCursorsToCloseField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  private def getCursor = {
    val methodName =
      order match {
        case IndexOrder.NONE => "unordered"
        case IndexOrder.ASCENDING => "ascending"
        case IndexOrder.DESCENDING => "descending"
      }
    method[CompositeValueIndexCursor, NodeValueIndexCursor, Array[NodeValueIndexCursor]](methodName)
  }

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   this.cursorsToClose = createCursor(...)
     *   this.nodeIndexCursor = ascending(cursorsToClose)
     *   this.canContinue = next(indexReadSession(queryIndexId), nodeIndexCursor, queryIterator, read)
     *   true
     * }}}
     */
    block(
      setField(nodeCursorsToCloseField, createCursorArray),
      setField(nodeIndexCursorField, invokeStatic(getCursor, loadField(nodeCursorsToCloseField))),
      setField(canContinue, profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id, doProfile)),
      constant(true))
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     ...
     *     setLongAt(offset, nodeIndexCursor.nodeReference())
     *     setCachedPropertyAt(offset, queryIterator.current)//only if applicable
     *     << inner.genOperate >>
     *     this.canContinue = next(indexReadSession(queryIndexId), nodeIndexCursor, queryIterator, read)
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        codeGen.setLongAt(offset, invoke(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Long]("nodeReference"))),
        cacheProperties,
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(
          innermost.setUnlessPastLimit(canContinue,
            profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id, doProfile))
        ),
        endInnerLoop
      )
    )
  }

  private def getPropertyValueRepresentation(offset: Int): IntermediateRepresentation =
    invoke(loadField(nodeIndexCursorField),
      method[NodeValueIndexCursor, Value, Int]("propertyValue"), constant(offset))

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    val i = codeGen.namer.nextVariableName("i")
    /**
     * {{{
     *   if (cursorsToClose != null) {
     *      cursorsToClose.map(getCursorToFree).foreach(resources.cursorPools.nodeValueIndexCursorPool.free)
     *      cursorsToClose = null
     *   }
     *   nodeIndexCursor = null
     * }}}
     */
    block(
      condition(isNotNull(loadField(nodeCursorsToCloseField))){
        block(
          declareAndAssign(typeRefOf[Int], i, constant(0)),
          loop(IntermediateRepresentation.lessThan(load(i), arrayLength(loadField(nodeCursorsToCloseField)))) {
            block(
              freeCursor[NodeValueIndexCursor](getCursorToFree(arrayLoad(loadField(nodeCursorsToCloseField), load(i))), NodeValueIndexCursorPool),
              assign(i, add(load(i), constant(1)))
            )
          },
          setField(nodeCursorsToCloseField, constant(null))
        )
      },
      setField(nodeIndexCursorField, constant(null))
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(nodeIndexCursorField)))(
        invokeSideEffect(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Unit, KernelReadTracer]("setTracer"), event)
      ),
      inner.genSetExecutionEvent(event)
    )

  private def cacheProperties: IntermediateRepresentation = {
    val ops = for {i <- indexPropertyIndices.indices
                   indexPropertyIndex = indexPropertyIndices(i)
                   slot = indexPropertySlotOffsets(i)
                   } yield {

      codeGen.setCachedPropertyAt(slot, getPropertyValueRepresentation(indexPropertyIndex))
    }
    block(ops:_*)
  }
}

/**
 * Code generation template for index seeks of the form `MATCH (n:L) WHERE n.prop = 1 OR n.prop = 2 OR n.prop =...`
 *
 * Will use delegate to `unordered`, `ascending`, or `descending` in `CompositeValueIndexCursor` depending on the order
 * to get a cursor.
 */
class ManyQueriesNodeIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                           id: Id,
                                           innermost: DelegateOperatorTaskTemplate,
                                           offset: Int,
                                           property: SlottedIndexedProperty,
                                           seekExpression: SeekExpression,
                                           queryIndexId: Int,
                                           order: IndexOrder,
                                           argumentSize: SlotConfiguration.Size)
                                          (codeGen: OperatorExpressionCompiler)
  extends BaseManyQueriesNodeIndexSeekTaskTemplate(inner, id, innermost, offset, Seq(property), order, argumentSize, codeGen) {
  private var seekValues: Seq[IntermediateExpression] = _

  override def getCursorToFree(cursor: IntermediateRepresentation): IntermediateRepresentation = cursor

  override def createCursorArray: IntermediateRepresentation = {
    if (seekValues == null) {
      seekValues = seekExpression.generateSeekValues.map(_ ()).map(v => v.copy(ir = nullCheckIfRequired(v)))
    }
    invokeStatic(createCursorsMethod,
      seekExpression.generatePredicate(seekValues.map(_.ir)),
      indexReadSession(queryIndexId),
      indexOrder(order),
      constant(needsValues),
      loadField(DATA_READ),
      CURSOR_POOL)
  }
  override def genExpressions: Seq[IntermediateExpression] = seekValues
}

class CompositeNodeIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                         id: Id,
                                         innermost: DelegateOperatorTaskTemplate,
                                         offset: Int,
                                         properties: Seq[SlottedIndexedProperty],
                                         seekExpressions: Seq[SeekExpression],
                                         queryIndexId: Int,
                                         order: IndexOrder,
                                         argumentSize: SlotConfiguration.Size)
                                        (codeGen: OperatorExpressionCompiler)
  extends BaseManyQueriesNodeIndexSeekTaskTemplate(inner, id, innermost, offset, properties, order, argumentSize, codeGen) {
  private var seekValues: Seq[Seq[IntermediateExpression]] = _

  override def getCursorToFree(cursor: IntermediateRepresentation): IntermediateRepresentation = invokeStatic(ManyQueriesNodeIndexSeekTaskTemplate.compositeCursorToFreeMethod, cursor)

  override def createCursorArray: IntermediateRepresentation = {
    if (seekValues == null) {
      seekValues = seekExpressions.map(_.generateSeekValues).map(compile).map(nullCheck)
    }
    val predicates = seekValues.zip(seekExpressions.map(_.generatePredicate)).map{
      case (vs, p) => p(vs.map(_.ir))
    }
    invokeStatic(ManyQueriesNodeIndexSeekTaskTemplate.compositeCreateCursorsMethod,
      arrayOf[Array[IndexQuery]](predicates:_*),
      indexReadSession(queryIndexId),
      indexOrder(order),
      constant(needsValues),
      loadField(DATA_READ),
      CURSOR_POOL)
  }
  override def genExpressions: Seq[IntermediateExpression] = seekValues.flatten
  private def compile(generators: Seq[() => IntermediateExpression]): Seq[IntermediateExpression] = generators.map(_())
  private def nullCheck(in: Seq[IntermediateExpression]): Seq[IntermediateExpression] = in.map(v => v.copy(ir = nullCheckIfRequired(v)))
}

object ManyQueriesNodeIndexSeekTaskTemplate {
  def createCursors(predicates: Array[IndexQuery],
                    index: IndexReadSession,
                    order: IndexOrder,
                    needsValues: Boolean,
                    read: Read,
                    cursorPools: CursorPools): Array[NodeValueIndexCursor] = {
    val cursors = new Array[NodeValueIndexCursor](predicates.length)
    val pool = cursorPools.nodeValueIndexCursorPool
    var i = 0
    while (i < cursors.length) {
      val cursor = pool.allocate()
      read.nodeIndexSeek(index, cursor, IndexQueryConstraints.constrained(order, needsValues || order != IndexOrder.NONE), predicates(i))
      cursors(i) = cursor
      i += 1
    }
    cursors
  }

  def compositeCursorToFree(cursor: NodeValueIndexCursor): NodeValueIndexCursor = {
    cursor match {
      case cursor: ValuedNodeIndexCursor => cursor.inner
      case x => x
    }
  }

  def compositeCreateCursors(predicates: Array[Array[IndexQuery]],
                             index: IndexReadSession,
                             order: IndexOrder,
                             needsValues: Boolean,
                             read: Read,
                             cursorPools: CursorPools): Array[NodeValueIndexCursor] = {
    val combinedPredicates = combine(predicates)
    val cursors = new Array[NodeValueIndexCursor](combinedPredicates.length)
    val pool = cursorPools.nodeValueIndexCursorPool
    var i = 0
    while (i < cursors.length) {
      val cursor = pool.allocate()
      val queries = combinedPredicates(i)
      val reallyNeedsValues = needsValues || order != IndexOrder.NONE
      val actualValues =
      // We don't need property values from the index for an exact seek
        if (reallyNeedsValues && queries.forall(_.isInstanceOf[ExactPredicate])) {
            queries.map(_.asInstanceOf[ExactPredicate].value())
          } else {
          null
        }
      val needsValuesFromIndexSeek = actualValues == null && reallyNeedsValues
      read.nodeIndexSeek(index, cursor, IndexQueryConstraints.constrained(order, needsValuesFromIndexSeek), queries:_*)
      if (reallyNeedsValues && actualValues != null) {
        cursors(i) = new ValuedNodeIndexCursor(cursor, actualValues)
      } else {
        cursors(i) = cursor
      }
      i += 1
    }
    cursors
  }

  def isImpossible(predicates: Seq[IndexQuery]): Boolean = predicates.exists(isImpossible)

  def isImpossible(predicate: IndexQuery): Boolean = predicate match {
        case p: IndexQuery.ExactPredicate => (p.value() eq Values.NO_VALUE) || (p.value().isInstanceOf[FloatingPointValue] && p.value().asInstanceOf[FloatingPointValue].isNaN)
        case p: IndexQuery.RangePredicate[_] => !RANGE_SEEKABLE_VALUE_GROUPS.contains(p.valueGroup())
        case _ => false
  }

  def getPropertyValue(iterator: CompositePredicateIterator, nodeCursor: NodeValueIndexCursor, offset: Int): Value =
    propertyValue(iterator.previous(offset), nodeCursor, offset)

  private def propertyValue(predicate: IndexQuery, nodeCursor: NodeValueIndexCursor, offset: Int) = predicate match {
    case exactPredicate: ExactPredicate => exactPredicate.value()
    case _ => nodeCursor.propertyValue(offset)
  }

  def compositeQueryIterator(predicates: Array[Array[IndexQuery]]): CompositePredicateIterator = {
    //Combine predicates
    //[ [a, b], [c], [d, e, f]] => [
    //                                [a, c, d],
    //                                [a, c, e],
    //                                [a, c, f],
    //                                [b, c, d],
    //                                [b, c, e],
    //                                [b, c, f],
    //                             ]
    new CompositePredicateIterator(combine(predicates))
  }

  val createCursorsMethod: Method =
    method[ManyQueriesNodeIndexSeekTaskTemplate, Array[NodeValueIndexCursor], Array[IndexQuery], IndexReadSession, IndexOrder, Boolean, Read, CursorPools]("createCursors")
  val compositeCreateCursorsMethod: Method =
    method[ManyQueriesNodeIndexSeekTaskTemplate, Array[NodeValueIndexCursor], Array[Array[IndexQuery]], IndexReadSession, IndexOrder, Boolean, Read, CursorPools]("compositeCreateCursors")
  val compositeGetPropertyMethod: Method =
    method[ManyQueriesNodeIndexSeekTaskTemplate,
      Value,
      CompositePredicateIterator,
      NodeValueIndexCursor,
      Int]("getPropertyValue")
  val compositeQueryIteratorMethod: Method = method[ManyQueriesNodeIndexSeekTaskTemplate, CompositePredicateIterator, Array[Array[IndexQuery]]]("compositeQueryIterator")
  val compositeCursorToFreeMethod: Method = method[ManyQueriesNodeIndexSeekTaskTemplate, NodeValueIndexCursor, NodeValueIndexCursor]("compositeCursorToFree")
}

case class SeekExpression(generateSeekValues: Seq[() => IntermediateExpression],
                          generatePredicate: Seq[IntermediateRepresentation] => IntermediateRepresentation,
                          single: Boolean = false)


class ManyPredicateIterator(predicates: Array[IndexQuery]) {
  private var pos = 0

  def hasNext: Boolean = pos < predicates.length

  def next(): IndexQuery = {
    val current = predicates(pos)
    pos += 1
    current
  }

  def previous: IndexQuery = predicates(pos - 1)
}

class CompositePredicateIterator(predicates: Array[Array[IndexQuery]]) {
  private var pos = 0

  def hasNext: Boolean = pos < predicates.length

  def next(): Array[IndexQuery] = {
    val current = predicates(pos)
    pos += 1
    current
  }

  def previous: Array[IndexQuery] = predicates(pos - 1)
}
