/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
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
import org.neo4j.codegen.api.IntermediateRepresentation.increment
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNaN
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.lessThan
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.nonGenericTypeRefOf
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
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.ValuedRelationshipIndexCursor
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledHelpers
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EntityIndexSeeker
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeek
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekMode
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.CursorPools
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyQueriesNodeIndexSeekTaskTemplate.isImpossible
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_REL_INDEX_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_REL_SCAN_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.RelScanCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.RelValueIndexCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.asStorableValue
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.indexOrder
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.indexReadSession
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.relationshipIndexSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.singleRelationship
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
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.util.Preconditions
import org.neo4j.values.storable.Value

import scala.collection.mutable.ArrayBuffer

class DirectedRelationshipIndexSeekOperator(val workIdentity: WorkIdentity,
                                            relOffset: Int,
                                            startOffset: Int,
                                            endOffset: Int,
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
      new DirectedRelationshipIndexSeekTask(
        inputMorsel.nextCopy,
        workIdentity,
        relOffset,
        startOffset,
        endOffset,
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

class DirectedRelationshipIndexSeekTask(inputMorsel: Morsel,
                                        val workIdentity: WorkIdentity,
                                        relOffset: Int,
                                        startOffset: Int,
                                        endOffset: Int,
                                        indexPropertyIndices: Array[Int],
                                        indexPropertySlotOffsets: Array[Int],
                                        queryIndexId: Int,
                                        indexOrder: IndexOrder,
                                        argumentSize: SlotConfiguration.Size,
                                        val propertyIds: Array[Int],
                                        val valueExpr: QueryExpression[Expression],
                                        val indexMode: IndexSeekMode = IndexSeek) extends InputLoopTask(inputMorsel) with EntityIndexSeeker {

  protected var relCursor: RelationshipValueIndexCursor = _
  protected var scanCursor: RelationshipScanCursor = _
  private var cursorsToClose: Array[RelationshipValueIndexCursor] = _
  private var exactSeekValues: Array[Value] = _

  // INPUT LOOP TASK
  override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
    val queryState = state.queryStateForExpressionEvaluation(resources)
    initExecutionContext.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
    val indexQueries = computeIndexQueries(queryState, initExecutionContext)
    val read = state.query.transactionalContext.transaction.dataRead
    val (cursor, closers) = computeCursor(indexQueries, read, state, resources, queryIndexId, indexOrder, needsValues || indexOrder != IndexOrder.NONE)
    relCursor = cursor
    cursorsToClose = closers
    scanCursor = resources.cursorPools.relationshipScanCursorPool.allocateAndTrace()
    true
  }

  override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
    val read = state.query.transactionalContext.transaction.dataRead
    while (outputRow.onValidRow && relCursor != null && relCursor.next()) {
      val relationship = relCursor.relationshipReference()
      outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
      outputRow.setLongAt(relOffset, relationship)
      read.singleRelationship(relationship, scanCursor)
      scanCursor.next()
      outputRow.setLongAt(startOffset, scanCursor.sourceNodeReference())
      outputRow.setLongAt(endOffset, scanCursor.targetNodeReference())
      cacheProperties(outputRow)
      outputRow.next()
    }
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
    if (relCursor != null) {
      relCursor.setTracer(event)
    }
  }

  override protected def closeInnerLoop(resources: QueryResources): Unit = {
    if (cursorsToClose != null) {
      cursorsToClose.foreach(resources.cursorPools.relationshipValueIndexCursorPool.free)
      cursorsToClose = null
    }
    if (scanCursor != null) {
      resources.cursorPools.relationshipScanCursorPool.free(scanCursor)
      scanCursor = null
    }
    relCursor = null
  }

  // HELPERS
  protected def cacheProperties(outputRow: MorselFullCursor): Unit = {
    var i = 0
    while (i < indexPropertyIndices.length) {
      val indexPropertyIndex = indexPropertyIndices(i)
      val value =
        if (exactSeekValues != null) {
          exactSeekValues(indexPropertyIndex)
        } else {
          relCursor.propertyValue(indexPropertyIndex)
        }
      outputRow.setCachedPropertyAt(indexPropertySlotOffsets(i), value)
      i += 1
    }
  }
  protected def computeCursor(indexQueries: Seq[Seq[IndexQuery]],
                              read: Read,
                              state: PipelinedQueryState,
                              resources: QueryResources,
                              queryIndex: Int,
                              kernelIndexOrder: IndexOrder,
                              needsValues: Boolean): (RelationshipValueIndexCursor, Array[RelationshipValueIndexCursor]) = {
    if (indexQueries.size == 1) {
      val cursor = nonCompositeSeek(state.queryIndexes(queryIndex), resources, read, indexQueries.head)
      (cursor, Array(cursor))
    } else {
      val cursors = ArrayBuffer.empty[RelationshipValueIndexCursor]
      indexQueries.filterNot(isImpossible).foreach(query => {
        val cursor = seek(state.queryIndexes(queryIndex), resources, read, query, needsValues)
        cursors += cursor
      })
      val cursorArray = cursors.toArray
      (orderedCursor(kernelIndexOrder, cursorArray), cursorArray)
    }
  }

  private def orderedCursor(indexOrder: IndexOrder, cursors: Array[RelationshipValueIndexCursor]): RelationshipValueIndexCursor = indexOrder match {
    case IndexOrder.NONE => CompositeValueIndexCursor.unordered(cursors)
    case IndexOrder.ASCENDING => CompositeValueIndexCursor.ascending(cursors)
    case IndexOrder.DESCENDING => CompositeValueIndexCursor.descending(cursors)
  }

  private def needsValues: Boolean = indexPropertyIndices.nonEmpty

  //Only used for non-composite indexes, contains optimizations so that we
  //can avoid getting the value from the index when doing exact seeks.
  private def nonCompositeSeek(index: IndexReadSession,
                               resources: QueryResources,
                               read: Read,
                               predicates: Seq[IndexQuery]): RelationshipValueIndexCursor = {

    if (isImpossible(predicates)) {
      return RelationshipValueIndexCursor.EMPTY// leave cursor un-initialized/empty
    }

    // We don't need property values from the index for an exact seek
    exactSeekValues =
      if (needsValues && predicates.forall(_.isInstanceOf[ExactPredicate])) {
        predicates.map(_.asInstanceOf[ExactPredicate].value()).toArray
      } else {
        null
      }
    val needsValuesFromIndexSeek = exactSeekValues == null && needsValues

    seek(index, resources, read, predicates, needsValuesFromIndexSeek)
  }

  private def seek(index: IndexReadSession,
                   resources: QueryResources,
                   read: Read,
                   predicates: Seq[IndexQuery],
                   needsValuesFromIndexSeek: Boolean): RelationshipValueIndexCursor = {
    val pool = resources.cursorPools.relationshipValueIndexCursorPool
    val cursor = pool.allocateAndTrace()
    read.relationshipIndexSeek(index, cursor, IndexQueryConstraints.constrained(indexOrder, needsValuesFromIndexSeek), predicates: _*)
    cursor
  }
}

abstract class BaseRelationshipIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                     id: Id,
                                                     val innermost: DelegateOperatorTaskTemplate,
                                                     val relOffset: Int,
                                                     val startOffset: Int,
                                                     val endOffset: Int,
                                                     val argumentSize: SlotConfiguration.Size,
                                                     codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {
  protected val relIndexCursorField: InstanceField = field[RelationshipValueIndexCursor](codeGen.namer.nextVariableName("valueCursor"))
  protected val relScanCursorField: InstanceField = field[RelationshipScanCursor](codeGen.namer.nextVariableName("scanCursor"))

  protected def cacheProperties: IntermediateRepresentation

  override protected def genInnerLoop: IntermediateRepresentation = {
    val localRelVar = codeGen.namer.nextVariableName("rel")
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     ...
     *     setLongAt(relOffset, relIndexCursor.relationshipReference())
     *     [set scan cursor to point at relationshipReference]
     *     setLongAt(startOffset, relScanCursor.sourceNodeReference())
     *     setLongAt(endOffset, relScanCursor.targetNodeReference())
     *     setCachedPropertyAt(relOffset, [getPropertyValue])
     *     << inner.genOperate >>
     *     this.canContinue = this.relIndexCursor.next()
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        declareAndAssign(localRelVar, invoke(loadField(relIndexCursorField), method[RelationshipValueIndexCursor, Long]("relationshipReference"))),
        codeGen.setLongAt(relOffset, load[Long](localRelVar)),
        singleRelationship(load[Long](localRelVar), loadField(relScanCursorField)),
        invokeStatic(method[Preconditions, Unit, Boolean, String]("checkState"),
          profilingCursorNext[RelationshipScanCursor](loadField(relScanCursorField), id, doProfile, codeGen.namer), constant("Missing relationship")),
        codeGen.setLongAt(startOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
        codeGen.setLongAt(endOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("targetNodeReference"))),
        cacheProperties,
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(innermost.setUnlessPastLimit(canContinue, profilingCursorNext[RelationshipValueIndexCursor](loadField(relIndexCursorField), id, doProfile, codeGen.namer))),
        endInnerLoop
      )
    )
  }

  override def genMoreFields: Seq[Field] = Seq(relIndexCursorField, relScanCursorField)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(relIndexCursorField)))(block(
        invokeSideEffect(loadField(relIndexCursorField), method[RelationshipValueIndexCursor, Unit, KernelReadTracer]("setTracer"), event),
        invokeSideEffect(loadField(relScanCursorField), method[RelationshipScanCursor, Unit, KernelReadTracer]("setTracer"), event))
      ),
      inner.genSetExecutionEvent(event)
    )
}

abstract class SingleQueryRelationshipIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                            id: Id,
                                                            innermost: DelegateOperatorTaskTemplate,
                                                            relOffset: Int,
                                                            startOffset: Int,
                                                            endOffset: Int,
                                                            property: SlottedIndexedProperty,
                                                            order: IndexOrder,
                                                            needsValues: Boolean,
                                                            queryIndexId: Int,
                                                            argumentSize: SlotConfiguration.Size,
                                                            codeGen: OperatorExpressionCompiler)
  extends BaseRelationshipIndexSeekTaskTemplate(inner, id, innermost, relOffset, startOffset, endOffset, argumentSize, codeGen) {

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

  /**
   * Return the value of the property if we need to cache it.
   *
   * This value is either provided by the index in the case of a range seek,
   * or we already have the value in the case of an exact seek.
   */
  protected def getPropertyValue: IntermediateRepresentation

  override protected def cacheProperties: IntermediateRepresentation =
    property.maybeCachedNodePropertySlot.map(codeGen.setCachedPropertyAt(_, getPropertyValue)).getOrElse(noop())

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val hasInnerLoopVar = codeGen.namer.nextVariableName()
    /**
     * {{{
     *   [extension point]
     *   val hasInnerLoop = [isPredicatePossible]
     *   if (hasInnerLoop) {
     *     relIndexCursor = resources.cursorPools.relationshipValueIndexCursorPool.allocate()
     *     relScanCursor = resources.cursorPools.relationshipValueIndexCursorPool.allocate()
     *     context.transactionalContext.dataRead.relationshipIndexSeek(indexReadSession(queryIndexId),
     *                                                                 relIndexCursor,
     *                                                                 [predicate])
     *     this.canContinue = relIndexCursor.next()
     *    }
     *    hasInnerLoop
     * }}}
     */
    block(
      beginInnerLoop,
      declareAndAssign(typeRefOf[Boolean], hasInnerLoopVar, isPredicatePossible),
      setField(canContinue, constant(false)),
      condition(load[Boolean](hasInnerLoopVar))(
        block(
          allocateAndTraceCursor(relIndexCursorField, executionEventField, ALLOCATE_REL_INDEX_CURSOR, doProfile),
          allocateAndTraceCursor(relScanCursorField, executionEventField, ALLOCATE_REL_SCAN_CURSOR, doProfile),
          relationshipIndexSeek(indexReadSession(queryIndexId), loadField(relIndexCursorField), predicate, order, needsValues),
          setField(canContinue, profilingCursorNext[RelationshipValueIndexCursor](loadField(relIndexCursorField), id, doProfile, codeGen.namer))
        )),
      load[Boolean](hasInnerLoopVar)
    )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   resources.cursorPools.relationshipValueIndexCursorPool.free(relIndexCursor)
     *   resources.cursorPools.relationshipsScanCursorPool.free(relScanCursor)
     *   relIndexCursor = null
     *   relScanCursor = null
     * }}}
     */
    block(
      freeCursor[RelationshipValueIndexCursor](loadField(relIndexCursorField), RelValueIndexCursorPool),
      freeCursor[RelationshipScanCursor](loadField(relScanCursorField), RelScanCursorPool),
      setField(relIndexCursorField, constant(null)),
      setField(relScanCursorField, constant(null))
    )
  }
}

/**
 * Code generation template for index seeks of the form `MATCH ()-[r:R]->() WHERE r.prop = 42`
 */
class SingleExactDirectedSeekQueryRelationshipIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                                    id: Id,
                                                                    innermost: DelegateOperatorTaskTemplate,
                                                                    relOffset: Int,
                                                                    startOffset: Int,
                                                                    endOffset: Int,
                                                                    property: SlottedIndexedProperty,
                                                                    generateSeekValue: () => IntermediateExpression,
                                                                    queryIndexId: Int,
                                                                    argumentSize: SlotConfiguration.Size)
                                                                   (codeGen: OperatorExpressionCompiler)
  extends SingleQueryRelationshipIndexSeekTaskTemplate(inner,
                                                       id,
                                                       innermost,
                                                       relOffset,
                                                       startOffset,
                                                       endOffset,
                                                       property,
                                                       IndexOrder.NONE,
                                                       false,
                                                       queryIndexId,
                                                       argumentSize,
                                                       codeGen) {

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
 * Code generation template for range seeks, e.g. `r.prop < 42`, `r.prop >= 42`, and `42 < r.prop <= 43`
 */
class SingleDirectedRangeSeekQueryRelationshipIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                                    id: Id,
                                                                    innermost: DelegateOperatorTaskTemplate,
                                                                    relOffset: Int,
                                                                    startOffset: Int,
                                                                    endOffset: Int,
                                                                    property: SlottedIndexedProperty,
                                                                    seekExpression: SeekExpression,
                                                                    queryIndexId: Int,
                                                                    order: IndexOrder,
                                                                    argumentSize: SlotConfiguration.Size)(codeGen: OperatorExpressionCompiler)
  extends SingleQueryRelationshipIndexSeekTaskTemplate(inner,
                                                       id,
                                                       innermost,
                                                       relOffset,
                                                       startOffset,
                                                       endOffset,
                                                       property,
                                                       order,
                                                       property.getValueFromIndex,
                                                       queryIndexId,
                                                       argumentSize,
                                                       codeGen) {
  private var seekValues: Seq[IntermediateExpression] = _
  private val predicateVar = variable[IndexQuery](codeGen.namer.nextVariableName(), constant(null))

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V, predicateVar)
  override def genExpressions: Seq[IntermediateExpression] = seekValues
  override protected def getPropertyValue: IntermediateRepresentation =
    invoke(loadField(relIndexCursorField), method[RelationshipValueIndexCursor, Value, Int]("propertyValue"), constant(0))
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

abstract class BaseManyQueriesRelationshipIndexSeekTaskTemplate(override val inner: OperatorTaskTemplate,
                                                                id: Id,
                                                                innermost: DelegateOperatorTaskTemplate,
                                                                relOffset: Int,
                                                                startOffset: Int,
                                                                endOffset: Int,
                                                                properties: Seq[SlottedIndexedProperty],
                                                                order: IndexOrder,
                                                                argumentSize: SlotConfiguration.Size,
                                                                codeGen: OperatorExpressionCompiler)
  extends BaseRelationshipIndexSeekTaskTemplate(inner, id, innermost, relOffset, startOffset, endOffset, argumentSize, codeGen) {
  private val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2).toArray
  private val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot).toArray
  protected val needsValues: Boolean = indexPropertyIndices.nonEmpty
  private val relCursorsToCloseField = field[Array[RelationshipValueIndexCursor]](codeGen.namer.nextVariableName("cursorsToClose"))

  def createCursorArray: IntermediateRepresentation
  def getCursorToFree(cursor: IntermediateRepresentation): IntermediateRepresentation
  override def genMoreFields: Seq[Field] = Seq(relIndexCursorField, relScanCursorField, relCursorsToCloseField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  private def getCursor = {
    val methodName =
      order match {
        case IndexOrder.NONE => "unordered"
        case IndexOrder.ASCENDING => "ascending"
        case IndexOrder.DESCENDING => "descending"
      }

    //Since CompositeValueIndexCursor we need to do some trickery so that we access the static method as
    //'CompositeValueIndexCursor.methodName` instead of `CompositeValueIndexCursor<Object>.methodName
    Method(nonGenericTypeRefOf[CompositeValueIndexCursor[_]],
      typeRefOf[RelationshipValueIndexCursor], methodName,
      typeRefOf[Array[RelationshipValueIndexCursor]])
  }

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   this.cursorsToClose = createCursor(...)
     *   this.relIndexCursor = ascending(cursorsToClose)
     *   this.relScanCursor = resources.cursorPools.relationshipValueIndexCursorPool.allocate()
     *   this.canContinue =  relIndexCursor.next()
     *   true
     * }}}
     */
    block(
      setField(relCursorsToCloseField, createCursorArray),
      setField(relIndexCursorField, invokeStatic(getCursor, loadField(relCursorsToCloseField))),
      allocateAndTraceCursor(relScanCursorField, executionEventField, ALLOCATE_REL_SCAN_CURSOR, doProfile),
      setField(canContinue, profilingCursorNext[RelationshipValueIndexCursor](loadField(relIndexCursorField), id, doProfile, codeGen.namer)),
      constant(true))
  }


  private def getPropertyValueRepresentation(offset: Int): IntermediateRepresentation =
    invoke(loadField(relIndexCursorField),
      method[RelationshipValueIndexCursor, Value, Int]("propertyValue"), constant(offset))

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    val i = codeGen.namer.nextVariableName("i")
    /**
     * {{{
     *   if (cursorsToClose != null) {
     *      cursorsToClose.map(getCursorToFree).foreach(resources.cursorPools.relationshipValueIndexCursorPool.free)
     *      cursorsToClose = null
     *   }
     *   resources.cursorPools.relationshipsScanCursorPool.free(relScanCursor)
     *   relIndexCursor = null
     *   relScanCursor = null
     * }}}
     */
    block(
      condition(isNotNull(loadField(relCursorsToCloseField))){
        block(
          declareAndAssign(typeRefOf[Int], i, constant(0)),
          loop(lessThan(i, arrayLength(loadField(relCursorsToCloseField)))) {
            block(
              freeCursor[RelationshipValueIndexCursor](getCursorToFree(arrayLoad(loadField(relCursorsToCloseField), load[Int](i))), RelValueIndexCursorPool),
              increment(i)
            )
          },
          freeCursor[RelationshipScanCursor](loadField(relScanCursorField), RelScanCursorPool),
          setField(relCursorsToCloseField, constant(null)),
          setField(relScanCursorField, constant(null))
        )
      },
      setField(relIndexCursorField, constant(null))
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(relIndexCursorField)))(block(
        invokeSideEffect(loadField(relIndexCursorField), method[RelationshipValueIndexCursor, Unit, KernelReadTracer]("setTracer"), event),
        invokeSideEffect(loadField(relScanCursorField), method[RelationshipScanCursor, Unit, KernelReadTracer]("setTracer"), event))
      ),
      inner.genSetExecutionEvent(event)
    )

  override protected def cacheProperties: IntermediateRepresentation = {
    val ops = for {i <- indexPropertyIndices.indices
                   indexPropertyIndex = indexPropertyIndices(i)
                   slot = indexPropertySlotOffsets(i)
                   } yield {

      codeGen.setCachedPropertyAt(slot, getPropertyValueRepresentation(indexPropertyIndex))
    }
    block(ops:_*)
  }
}

object BaseManyQueriesRelationshipIndexSeekTaskTemplate {
  def createCursors(predicates: Array[IndexQuery],
                    index: IndexReadSession,
                    order: IndexOrder,
                    needsValues: Boolean,
                    read: Read,
                    cursorPools: CursorPools): Array[RelationshipValueIndexCursor] = {
    val cursors = new Array[RelationshipValueIndexCursor](predicates.length)
    val pool = cursorPools.relationshipValueIndexCursorPool
    var i = 0
    while (i < cursors.length) {
      val cursor = pool.allocate()
      read.relationshipIndexSeek(index, cursor, IndexQueryConstraints.constrained(order, needsValues || order != IndexOrder.NONE), predicates(i))
      cursors(i) = cursor
      i += 1
    }
    cursors
  }

  def compositeCursorToFree(cursor: RelationshipValueIndexCursor): RelationshipValueIndexCursor = {
    cursor match {
      case cursor: ValuedRelationshipIndexCursor => cursor.inner
      case x => x
    }
  }

  def compositeCreateCursors(predicates: Array[Array[IndexQuery]],
                             index: IndexReadSession,
                             order: IndexOrder,
                             needsValues: Boolean,
                             read: Read,
                             cursorPools: CursorPools): Array[RelationshipValueIndexCursor] = {
    val combinedPredicates = combine(predicates)
    val cursors = new Array[RelationshipValueIndexCursor](combinedPredicates.length)
    val pool = cursorPools.relationshipValueIndexCursorPool
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
      read.relationshipIndexSeek(index, cursor, IndexQueryConstraints.constrained(order, needsValuesFromIndexSeek), queries:_*)
      if (reallyNeedsValues && actualValues != null) {
        cursors(i) = new ValuedRelationshipIndexCursor(cursor, actualValues)
      } else {
        cursors(i) = cursor
      }
      i += 1
    }
    cursors
  }
}

/**
 * Code generation template for index seeks of the form `MATCH ()-[r:R]->() WHERE r.prop = 1 OR r.prop = 2 OR r.prop =...`
 *
 * Will use delegate to `unordered`, `ascending`, or `descending` in `CompositeValueIndexCursor` depending on the order
 * to get a cursor.
 */
class ManyQueriesDirectedRelationshipIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                           id: Id,
                                                           innermost: DelegateOperatorTaskTemplate,
                                                           relOffset: Int,
                                                           startOffset: Int,
                                                           endOffset: Int,
                                                           property: SlottedIndexedProperty,
                                                           seekExpression: SeekExpression,
                                                           queryIndexId: Int,
                                                           order: IndexOrder,
                                                           argumentSize: SlotConfiguration.Size)
                                                          (codeGen: OperatorExpressionCompiler)
  extends BaseManyQueriesRelationshipIndexSeekTaskTemplate(inner, id, innermost, relOffset, startOffset, endOffset, Seq(property), order, argumentSize, codeGen) {
  private var seekValues: Seq[IntermediateExpression] = _

  override def getCursorToFree(cursor: IntermediateRepresentation): IntermediateRepresentation = cursor

  override def createCursorArray: IntermediateRepresentation = {
    if (seekValues == null) {
      seekValues = seekExpression.generateSeekValues.map(_ ()).map(v => v.copy(ir = nullCheckIfRequired(v)))
    }
    invokeStatic(
      method[BaseManyQueriesRelationshipIndexSeekTaskTemplate, Array[RelationshipValueIndexCursor], Array[IndexQuery], IndexReadSession, IndexOrder, Boolean, Read, CursorPools]("createCursors"),
      seekExpression.generatePredicate(seekValues.map(_.ir)),
      indexReadSession(queryIndexId),
      indexOrder(order),
      constant(needsValues),
      loadField(DATA_READ),
      CURSOR_POOL)
  }
  override def genExpressions: Seq[IntermediateExpression] = seekValues
}

class CompositeDirectedRelationshipIndexSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                         id: Id,
                                                         innermost: DelegateOperatorTaskTemplate,
                                                         relOffset: Int,
                                                         startOffset: Int,
                                                         endOffset: Int,
                                                         properties: Seq[SlottedIndexedProperty],
                                                         seekExpressions: Seq[SeekExpression],
                                                         queryIndexId: Int,
                                                         order: IndexOrder,
                                                         argumentSize: SlotConfiguration.Size)
                                                        (codeGen: OperatorExpressionCompiler)
  extends BaseManyQueriesRelationshipIndexSeekTaskTemplate(inner, id, innermost, relOffset, startOffset, endOffset, properties, order, argumentSize, codeGen) {
  private var seekValues: Seq[Seq[IntermediateExpression]] = _

  override def getCursorToFree(cursor: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(
      method[BaseManyQueriesRelationshipIndexSeekTaskTemplate, RelationshipValueIndexCursor, RelationshipValueIndexCursor]("compositeCursorToFree"),
      cursor)

  override def createCursorArray: IntermediateRepresentation = {
    if (seekValues == null) {
      seekValues = seekExpressions.map(_.generateSeekValues).map(compile).map(nullCheck)
    }
    val predicates = seekValues.zip(seekExpressions.map(_.generatePredicate)).map{
      case (vs, p) => p(vs.map(_.ir))
    }
    invokeStatic(
      method[BaseManyQueriesRelationshipIndexSeekTaskTemplate, Array[RelationshipValueIndexCursor], Array[Array[IndexQuery]], IndexReadSession, IndexOrder, Boolean, Read, CursorPools]("compositeCreateCursors"),
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
























