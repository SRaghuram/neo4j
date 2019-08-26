/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable, Method}
import org.neo4j.cypher.internal.physicalplanning.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.NodeIndexStringSearchScanOperator.failOrFalseMethod
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, IsNoValue, NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{TextValue, Value}

abstract class NodeIndexStringSearchScanOperator(val workIdentity: WorkIdentity,
                                        nodeOffset: Int,
                                        property: SlottedIndexedProperty,
                                        queryIndexId: Int,
                                        indexOrder: IndexOrder,
                                        valueExpr: Expression,
                                        argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, Array(property)) {

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    val indexSession = state.queryIndexes(queryIndexId)
    IndexedSeq(new OTask(inputMorsel.nextCopy, indexSession))
  }

  def computeIndexQuery(property: Int, value: TextValue): IndexQuery

  class OTask(val inputMorsel: MorselExecutionContext, index: IndexReadSession) extends InputLoopTask {

    override def workIdentity: WorkIdentity = NodeIndexStringSearchScanOperator.this.workIdentity

    private var cursor: NodeValueIndexCursor = _

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {

      val read = context.transactionalContext.dataRead
      val queryState = new OldQueryState(context,
                                           resources = null,
                                           params = state.params,
                                           resources.expressionCursors,
                                           Array.empty[IndexReadSession],
                                           resources.expressionVariables(state.nExpressionSlots),
                                           state.subscriber,
                                           NoMemoryTracker)

      initExecutionContext.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
      val value = valueExpr(initExecutionContext, queryState)

      value match {
        case value: TextValue =>
          val indexQuery = computeIndexQuery(property.propertyKeyId, value)
          cursor = resources.cursorPools.nodeValueIndexCursorPool.allocate()
          read.nodeIndexSeek(index, cursor, indexOrder, property.maybeCachedNodePropertySlot.isDefined, indexQuery)
          true

        case x => NodeIndexStringSearchScanOperator.failOrFalse(x)
      }
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      iterate(inputMorsel, outputRow, cursor, argumentSize)
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
  def failOrFalse(value: AnyValue): Boolean = value match {
    case IsNoValue() => false
    case x => throw new CypherTypeException(s"Expected a string value, but got $x")
  }

  val failOrFalseMethod: Method = method[NodeIndexStringSearchScanOperator, Boolean, AnyValue]("failOrFalse")
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
                                            offset: Int,
                                            property: SlottedIndexedProperty,
                                            queryIndexId: Int,
                                            indexOrder: IndexOrder,
                                            generateExpression: () => IntermediateExpression,
                                            searchPredicate: (Int, IntermediateRepresentation) => IntermediateRepresentation,
                                            argumentSize: SlotConfiguration.Size)
                                           (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._

  private val nodeIndexCursorField = field[NodeValueIndexCursor](codeGen.namer.nextVariableName())
  private val needsValues = property.getValueFromIndex
  private var seekExpression: IntermediateExpression = _
  private val seekVariable = variable[Value](codeGen.namer.nextVariableName(), constant(null))

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
    seekExpression = generateExpression()
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
      condition(or(instanceOf[TextValue](load(seekVariable)), invokeStatic(failOrFalseMethod, load(seekVariable))))(
        block(
          allocateAndTraceCursor(nodeIndexCursorField, executionEventField, ALLOCATE_NODE_INDEX_CURSOR),
          nodeIndexSeek(indexReadSession(queryIndexId), loadField(nodeIndexCursorField), searchPredicate(property.propertyKeyId, cast[TextValue](load(seekVariable))), indexOrder, needsValues),
          setField(canContinue, cursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField))),
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
        if (innermost.shouldWriteToContext && (argumentSize.nLongs > 0 || argumentSize.nReferences > 0)) {
          invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit, ExecutionContext, Int, Int]("copyFrom"),
                           loadField(INPUT_MORSEL), constant(argumentSize.nLongs), constant(argumentSize.nReferences))
        } else {
          noop()
        },
        codeGen.setLongAt(offset, invoke(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Long]("nodeReference"))),
        property.maybeCachedNodePropertySlot.map(
          codeGen.setCachedPropertyAt(_,
                                      invoke(loadField(nodeIndexCursorField),
                                             method[NodeValueIndexCursor, Value, Int]("propertyValue"),
                                             constant(0) ))
          ).getOrElse(noop()),
        profileRow(id),
        inner.genOperateWithExpressions,
        setField(canContinue, cursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField)))
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

