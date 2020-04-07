/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.Method
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.NodeIndexCursorRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeIndexStringSearchScanOperator.isValidOrThrowMethod
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_NODE_INDEX_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NodeValueIndexCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.indexReadSession
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.nodeIndexSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.internal.kernel.api.IndexQuery
import org.neo4j.internal.kernel.api.IndexQueryConstraints
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value


abstract class NodeIndexStringSearchScanOperator(val workIdentity: WorkIdentity,
                                                 nodeOffset: Int,
                                                 property: SlottedIndexedProperty,
                                                 queryIndexId: Int,
                                                 indexOrder: IndexOrder,
                                                 valueExpr: Expression,
                                                 argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, Array(property)) {

  override def nextTasks(state: PipelinedQueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    val indexSession = state.queryIndexes(queryIndexId)
    IndexedSeq(new OTask(inputMorsel.nextCopy, indexSession))
  }

  def computeIndexQuery(property: Int, value: TextValue): IndexQuery

  class OTask(inputMorsel: Morsel, index: IndexReadSession) extends InputLoopTask(inputMorsel) {

    override def workIdentity: WorkIdentity = NodeIndexStringSearchScanOperator.this.workIdentity

    private var cursor: NodeValueIndexCursor = _

    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {

      val read = state.queryContext.transactionalContext.dataRead
      val queryState = state.queryStateForExpressionEvaluation(resources)

      initExecutionContext.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
      val value = valueExpr(initExecutionContext, queryState)

      value match {
        case value: TextValue =>
          val indexQuery = computeIndexQuery(property.propertyKeyId, value)
          cursor = resources.cursorPools.nodeValueIndexCursorPool.allocateAndTrace()
          read.nodeIndexSeek(index, cursor, IndexQueryConstraints.constrained(indexOrder, property.maybeCachedNodePropertySlot.isDefined), indexQuery)
          true

        case IsNoValue() => false

        case x => throw new CypherTypeException(s"Expected a string value, but got $x")
      }
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
      iterate(inputCursor, outputRow, cursor, argumentSize)
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      if (cursor != null) {
        cursor.setTracer(event)
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.nodeValueIndexCursorPool.free(cursor)
      cursor = null
    }
  }
}

object NodeIndexStringSearchScanOperator {

  def isValidOrThrow(value: AnyValue): Boolean = value match {
    case IsNoValue() => false
    case _: TextValue => true
    case x => throw new CypherTypeException(s"Expected a string value, but got $x")
  }

  val isValidOrThrowMethod: Method = method[NodeIndexStringSearchScanOperator, Boolean, AnyValue]("isValidOrThrow")
}

class NodeIndexContainsScanOperator(workIdentity: WorkIdentity,
                                    nodeOffset: Int,
                                    property: SlottedIndexedProperty,
                                    queryIndexId: Int,
                                    indexOrder: IndexOrder,
                                    valueExpr: Expression,
                                    argumentSize: SlotConfiguration.Size)
  extends NodeIndexStringSearchScanOperator(workIdentity,
    nodeOffset,
    property,
    queryIndexId,
    indexOrder,
    valueExpr,
    argumentSize) {

  override def computeIndexQuery(property: Int,
                                 value: TextValue): IndexQuery = IndexQuery.stringContains(property, value)
}

class NodeIndexEndsWithScanOperator(workIdentity: WorkIdentity,
                                    nodeOffset: Int,
                                    property: SlottedIndexedProperty,
                                    queryIndexId: Int,
                                    indexOrder: IndexOrder,
                                    valueExpr: Expression,
                                    argumentSize: SlotConfiguration.Size)
  extends NodeIndexStringSearchScanOperator(workIdentity,
    nodeOffset,
    property,
    queryIndexId,
    indexOrder,
    valueExpr,
    argumentSize) {

  override def computeIndexQuery(property: Int,
                                 value: TextValue): IndexQuery = IndexQuery.stringSuffix(property, value)
}

class NodeIndexStringSearchScanTaskTemplate(inner: OperatorTaskTemplate,
                                            id: Id,
                                            innermost: DelegateOperatorTaskTemplate,
                                            nodeVarName: String,
                                            offset: Int,
                                            property: SlottedIndexedProperty,
                                            queryIndexId: Int,
                                            indexOrder: IndexOrder,
                                            generateExpression: () => IntermediateExpression,
                                            searchPredicate: (Int, IntermediateRepresentation) => IntermediateRepresentation,
                                            argumentSize: SlotConfiguration.Size)
                                           (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {


  private val nodeIndexCursorField = field[NodeValueIndexCursor](codeGen.namer.nextVariableName())
  private val needsValues = property.getValueFromIndex
  private var seekExpression: IntermediateExpression = _
  private val seekVariable = variable[Value](codeGen.namer.nextVariableName(), constant(null))

  codeGen.registerCursor(nodeVarName, NodeIndexCursorRepresentation(loadField(nodeIndexCursorField)))

  override def genMoreFields: Seq[Field] = Seq(nodeIndexCursorField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V, seekVariable)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(nodeIndexCursorField)))(
        invokeSideEffect(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Unit, KernelReadTracer]("setTracer"), event)
      ),
      inner.genSetExecutionEvent(event)
    )

  override def genExpressions: Seq[IntermediateExpression] = Seq(seekExpression)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    if (seekExpression == null) {
      seekExpression = generateExpression()
    }
    val hasInnerLoop = codeGen.namer.nextVariableName()
    /**
     * {{{
     *   this.nodeIndexCursor = resources.cursorPools.nodeValueIndexCursorPool.allocate()
     *   context.transactionalContext.dataRead.nodeIndexSeek(session, cursor, indexOrder, needsValues, searchPredicate)
     *   this.canContinue = nodeIndexCursor.next()
     *   true
     * }}}
     */
    block(
      assign(seekVariable, nullCheckIfRequired(seekExpression)),
      declareAndAssign(typeRefOf[Boolean], hasInnerLoop, constant(false)),
      setField(canContinue, constant(false)),
      condition(invokeStatic(isValidOrThrowMethod, load(seekVariable)))(
        block(
          allocateAndTraceCursor(nodeIndexCursorField, executionEventField, ALLOCATE_NODE_INDEX_CURSOR),
          nodeIndexSeek(
            indexReadSession(queryIndexId),
            loadField(nodeIndexCursorField),
            searchPredicate(property.propertyKeyId,
              cast[TextValue](load(seekVariable))), indexOrder, needsValues),
          setField(canContinue, profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id)),
          assign(hasInnerLoop, loadField(canContinue))
        )
      ),
      load(hasInnerLoop))
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     setLongAt(offset, nodeIndexCursor.nodeReference())
     *     setCachedPropertyAt(cacheOffset, nodeCursor.propertyValue(0))
     *     ...
     *     << inner.genOperate >>
     *     this.canContinue = this.nodeIndexCursor.next()
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        codeGen.setLongAt(offset, invoke(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Long]("nodeReference"))),
        property.maybeCachedNodePropertySlot.map(
          codeGen.setCachedPropertyAt(_,
            invoke(loadField(nodeIndexCursorField),
              method[NodeValueIndexCursor, Value, Int]("propertyValue"),
              constant(0) ))
        ).getOrElse(noop()),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(setField(canContinue, profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id)))
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
}

