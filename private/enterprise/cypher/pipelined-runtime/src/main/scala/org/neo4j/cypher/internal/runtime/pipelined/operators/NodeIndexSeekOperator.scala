/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
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
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledHelpers
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeek
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekMode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeIndexSeeker
import org.neo4j.cypher.internal.runtime.pipelined.NodeIndexCursorRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyQueriesNodeIndexSeekTaskTemplate.compositeGetPropertyMethod
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyQueriesNodeIndexSeekTaskTemplate.compositeNextMethod
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyQueriesNodeIndexSeekTaskTemplate.compositeQueryIteratorMethod
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyQueriesNodeIndexSeekTaskTemplate.getPropertyMethod
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyQueriesNodeIndexSeekTaskTemplate.nextMethod
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyQueriesNodeIndexSeekTaskTemplate.queryIteratorMethod
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_NODE_INDEX_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NodeValueIndexCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.asStorableValue
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.indexOrder
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.indexReadSession
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.nodeIndexSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
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
                            override val valueExpr: QueryExpression[Expression],
                            override val indexMode: IndexSeekMode = IndexSeek)
  extends StreamingOperator with NodeIndexSeeker {

  private val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
  private val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot)
  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    singletonIndexedSeq(new OTask(inputMorsel.nextCopy))
  }

  override val propertyIds: Array[Int] = properties.map(_.propertyKeyId)

  class OTask(inputMorsel: Morsel) extends InputLoopTask(inputMorsel) {

    override def workIdentity: WorkIdentity = NodeIndexSeekOperator.this.workIdentity

    private var nodeCursor: NodeValueIndexCursor = _
    private var cursorsToClose: Array[NodeValueIndexCursor] = _
    private var exactSeekValues: Array[Value] = _

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
        cursorsToClose = indexQueries.filterNot(impossiblePredicates).map(query => {
          val cursor = resources.cursorPools.nodeValueIndexCursorPool.allocateAndTrace()
          read.nodeIndexSeek(state.queryIndexes(queryIndexId), cursor, IndexQueryConstraints.constrained(indexOrder, needsValues || indexOrder != IndexOrder.NONE), query: _*)
          cursor
        }).toArray
        nodeCursor = orderedCursor(indexOrder, cursorsToClose)
      }

      true
    }

    private def orderedCursor(indexOrder: IndexOrder, cursors: Array[NodeValueIndexCursor]): NodeValueIndexCursor = indexOrder match {
      case IndexOrder.NONE => CompositeValueIndexCursor.unordered(cursors)
      case IndexOrder.ASCENDING => CompositeValueIndexCursor.ascending(cursors)
      case IndexOrder.DESCENDING => CompositeValueIndexCursor.descending(cursors)
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
      val read = state.queryContext.transactionalContext.transaction.dataRead()
      while (outputRow.onValidRow && nodeCursor != null && nodeCursor.next()) {
        outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setLongAt(offset, nodeCursor.nodeReference())
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

    private def seek[RESULT <: AnyRef](index: IndexReadSession,
                                       nodeCursor: NodeValueIndexCursor,
                                       read: Read,
                                       predicates: Seq[IndexQuery]): Unit = {



      if (impossiblePredicates(predicates)) {
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

  def impossiblePredicates(predicates: Seq[IndexQuery]): Boolean  = {
    predicates.exists {
      case p: IndexQuery.ExactPredicate => (p.value() eq Values.NO_VALUE) || (p.value().isInstanceOf[FloatingPointValue] && p.value().asInstanceOf[FloatingPointValue].isNaN)
      case _: IndexQuery.ExistsPredicate if predicates.length > 1 => false
      case p: IndexQuery =>
        !RANGE_SEEKABLE_VALUE_GROUPS.contains(p.valueGroup())
    }
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
          allocateAndTraceCursor(nodeIndexCursorField, executionEventField, ALLOCATE_NODE_INDEX_CURSOR),
          nodeIndexSeek(indexReadSession(queryIndexId), loadField(nodeIndexCursorField), predicate, order, needsValues),
          setField(canContinue, profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id))
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
        doIfInnerCantContinue(innermost.setUnlessPastLimit(canContinue, profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id))),
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

/**
 * Code generation template for index seeks of the form `MATCH (n:L) WHERE n.prop = 1 OR n.prop = 2 OR n.prop =...`
 * Generates code for first doing an exact search of the first predicate and when the result is exhausted
 * it moves on to the next predicate until all predicates have been visited.
 */
class ManyQueriesNodeIndexSeekTaskTemplate(override val inner: OperatorTaskTemplate,
                                           id: Id,
                                           innermost: DelegateOperatorTaskTemplate,
                                           nodeVarName: String,
                                           offset: Int,
                                           property: SlottedIndexedProperty,
                                           seekExpression: SeekExpression,
                                           queryIndexId: Int,
                                           order: IndexOrder,
                                           argumentSize: SlotConfiguration.Size)
                                          (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {


  private var seekValues: Seq[IntermediateExpression] = _

  private val needsValues = property.getValueFromIndex
  private val nodeIndexCursorField = field[NodeValueIndexCursor](codeGen.namer.nextVariableName())
  private val queryIteratorField = field[ManyPredicateIterator](codeGen.namer.nextVariableName())

  override def genMoreFields: Seq[Field] = Seq(nodeIndexCursorField, queryIteratorField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genExpressions: Seq[IntermediateExpression] = seekValues

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    if (seekValues == null) {
      seekValues = seekExpression.generateSeekValues.map(_ ()).map(v => v.copy(ir = nullCheckIfRequired(v)))
    }
    /**
     * {{{
     *   this.queryIterator = queryIterator(property, ([query predicate])
     *   this.nodeIndexCursor = resources.cursorPools.nodeValueIndexCursorPool.allocate()
     *   this.canContinue = next(indexReadSession(queryIndexId), nodeIndexCursor, queryIterator, read)
     *   true
     * }}}
     */
    block(
      setField(queryIteratorField,
      invokeStatic(queryIteratorMethod, seekExpression.generatePredicate(seekValues.map(_.ir)))),
      setField(nodeIndexCursorField, ALLOCATE_NODE_INDEX_CURSOR),
      setField(canContinue,
        invokeStatic(nextMethod,
          indexReadSession(queryIndexId),
          loadField(nodeIndexCursorField),
          loadField(queryIteratorField),
          indexOrder(order),
                            constant(needsValues),
                            loadField(DATA_READ))),
      profileRow(id, loadField(canContinue)),
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
        property.maybeCachedNodePropertySlot.map(codeGen.setCachedPropertyAt(_,
          getPropertyValueRepresentation)).getOrElse(noop()),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(
          block(
            innermost.setUnlessPastLimit(canContinue,
              invokeStatic(nextMethod,
                indexReadSession(queryIndexId),
                loadField(nodeIndexCursorField),
                loadField(queryIteratorField),
                indexOrder(order), constant(needsValues), loadField(DATA_READ))),
            profileRow(id, loadField(canContinue))),
        ),
        endInnerLoop
      )
    )
  }

  private def getPropertyValueRepresentation: IntermediateRepresentation =
    invokeStatic(getPropertyMethod,
                 loadField(queryIteratorField),
                 loadField(nodeIndexCursorField),
                 constant(0))

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

class CompositeNodeIndexSeekTaskTemplate(override val inner: OperatorTaskTemplate,
                                         id: Id,
                                         innermost: DelegateOperatorTaskTemplate,
                                         nodeVarName: String,
                                         offset: Int,
                                         properties: Seq[SlottedIndexedProperty],
                                         seekExpressions: Seq[SeekExpression],
                                         queryIndexId: Int,
                                         order: IndexOrder,
                                         argumentSize: SlotConfiguration.Size)
                                        (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  private var seekValues: Seq[Seq[IntermediateExpression]] = _

  private val nodeIndexCursorField = field[NodeValueIndexCursor](codeGen.namer.nextVariableName())
  private val queryIteratorField = field[CompositePredicateIterator](codeGen.namer.nextVariableName())
  private val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2).toArray
  private val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot).toArray

  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  override def genMoreFields: Seq[Field] = Seq(nodeIndexCursorField, queryIteratorField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genExpressions: Seq[IntermediateExpression] = seekValues.flatten

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    if (seekValues == null) {
      seekValues = seekExpressions.map(_.generateSeekValues).map(compile).map(nullCheck)
    }
    /**
      * {{{
      *   this.queryIterator = compositeQueryIterator(new IndexQuery[][]{[query predicate]}
      *   this.nodeIndexCursor = resources.cursorPools.nodeValueIndexCursorPool.allocate()
      *   this.canContinue = next(indexReadSession(queryIndexId), nodeIndexCursor, queryIterator, read)
      *   true
      * }}}
      */
    val predicates = seekValues.zip(seekExpressions.map(_.generatePredicate)).map{
      case (vs, p) => p(vs.map(_.ir))
    }

    block(
      setField(queryIteratorField,
               invokeStatic(compositeQueryIteratorMethod, arrayOf[Array[IndexQuery]](predicates:_*))),
      setField(nodeIndexCursorField, ALLOCATE_NODE_INDEX_CURSOR),
      setField(canContinue,
               invokeStatic(compositeNextMethod,
                            indexReadSession(queryIndexId),
                            loadField(nodeIndexCursorField),
                            loadField(queryIteratorField),
                            indexOrder(order),
                            constant(needsValues),
                            loadField(DATA_READ))),
      profileRow(id, loadField(canContinue)),
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
          block(
            innermost.setUnlessPastLimit(canContinue,
              invokeStatic(compositeNextMethod,
                indexReadSession(queryIndexId),
                loadField(nodeIndexCursorField),
                loadField(queryIteratorField),
                indexOrder(order),
                constant(needsValues),
                loadField(DATA_READ))),
            profileRow(id, loadField(canContinue))),
        ),
        endInnerLoop
        )
      )
  }

  private def cacheProperties: IntermediateRepresentation = {
    val ops = for {i <- indexPropertyIndices.indices
      indexPropertyIndex = indexPropertyIndices(i)
      slot = indexPropertySlotOffsets(i)
    } yield {

      codeGen.setCachedPropertyAt(slot, getPropertyValueRepresentation(indexPropertyIndex))
    }
    block(ops:_*)
  }
  private def compile(generators: Seq[() => IntermediateExpression]): Seq[IntermediateExpression] = generators.map(_())

  private def nullCheck(in: Seq[IntermediateExpression]): Seq[IntermediateExpression] = in.map(v => v.copy(ir = nullCheckIfRequired(v)))

  private def getPropertyValueRepresentation(offset: Int): IntermediateRepresentation =
    invokeStatic(compositeGetPropertyMethod,
                 loadField(queryIteratorField),
                 loadField(nodeIndexCursorField),
                 constant(offset))

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

object ManyQueriesNodeIndexSeekTaskTemplate {
  def next(index: IndexReadSession,
           cursor: NodeValueIndexCursor,
           queries: ManyPredicateIterator,
           order: IndexOrder,
           needsValues: Boolean,
           read: Read): Boolean = {
    while (true) {
      if (cursor.next()) {
        return true
      }
      else if (queries.hasNext) {
        var continue = true
        while (continue) {
          val indexQuery = queries.next()
          if (!isImpossible(indexQuery)) {
            val reallyNeedsValues = needsValues && !indexQuery.isInstanceOf[ExactPredicate]
            read.nodeIndexSeek(index, cursor, IndexQueryConstraints.constrained(order, reallyNeedsValues), indexQuery)
            continue = false
          } else {
            continue = queries.hasNext
          }
        }
      } else {
        return false
      }
    }
    throw new IllegalStateException("Unreachable code")
  }

  def next(index: IndexReadSession,
           cursor: NodeValueIndexCursor,
           queries: CompositePredicateIterator,
           order: IndexOrder,
           needsValues: Boolean,
           read: Read): Boolean = {
    while (true) {
      if (cursor.next()) {
        return true
      }
      else if (queries.hasNext) {
        var continue = true
        while (continue) {
          val indexQueries = queries.next()
          if (!isImpossible(indexQueries)) {
            val reallyNeedsValues = needsValues && !indexQueries.forall(_.isInstanceOf[ExactPredicate])
            read.nodeIndexSeek(index, cursor, IndexQueryConstraints.constrained(order, reallyNeedsValues), indexQueries:_*)
            continue = false
          } else {
            continue = queries.hasNext
          }
        }
      } else {
        return false
      }
    }
    throw new IllegalStateException("Unreachable code")
  }

  def isImpossible(predicates: Array[IndexQuery]): Boolean = predicates.exists(isImpossible)

  def isImpossible(predicate: IndexQuery): Boolean = predicate match {
        case p: IndexQuery.ExactPredicate => (p.value() eq Values.NO_VALUE) || (p.value().isInstanceOf[FloatingPointValue] && p.value().asInstanceOf[FloatingPointValue].isNaN)
        case p: IndexQuery.RangePredicate[_] => !RANGE_SEEKABLE_VALUE_GROUPS.contains(p.valueGroup())
        case _ => false
  }

  def getPropertyValue(iterator: ManyPredicateIterator, nodeCursor: NodeValueIndexCursor, offset: Int): Value =
    propertyValue(iterator.previous, nodeCursor, offset)

  def getPropertyValue(iterator: CompositePredicateIterator, nodeCursor: NodeValueIndexCursor, offset: Int): Value =
    propertyValue(iterator.previous(offset), nodeCursor, offset)

  private def propertyValue(predicate: IndexQuery, nodeCursor: NodeValueIndexCursor, offset: Int) = predicate match {
    case exactPredicate: ExactPredicate => exactPredicate.value()
    case _ => nodeCursor.propertyValue(offset)
  }

  def queryIterator(predicates: Array[IndexQuery]) =
    new ManyPredicateIterator(predicates)


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

  val nextMethod: Method =
    method[ManyQueriesNodeIndexSeekTaskTemplate,
      Boolean,
      IndexReadSession,
      NodeValueIndexCursor,
      ManyPredicateIterator,
      IndexOrder,
      Boolean,
      Read]("next")

  val compositeNextMethod: Method =
    method[ManyQueriesNodeIndexSeekTaskTemplate,
      Boolean,
      IndexReadSession,
      NodeValueIndexCursor,
      CompositePredicateIterator,
      IndexOrder,
      Boolean,
      Read]("next")

  val getPropertyMethod: Method =
    method[ManyQueriesNodeIndexSeekTaskTemplate,
      Value,
      ManyPredicateIterator,
      NodeValueIndexCursor,
      Int]("getPropertyValue")

  val compositeGetPropertyMethod: Method =
    method[ManyQueriesNodeIndexSeekTaskTemplate,
      Value,
      CompositePredicateIterator,
      NodeValueIndexCursor,
      Int]("getPropertyValue")

  val queryIteratorMethod: Method =
    method[ManyQueriesNodeIndexSeekTaskTemplate, ManyPredicateIterator, Array[IndexQuery]]("queryIterator")

  val compositeQueryIteratorMethod: Method = method[ManyQueriesNodeIndexSeekTaskTemplate, CompositePredicateIterator, Array[Array[IndexQuery]]]("compositeQueryIterator")
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
