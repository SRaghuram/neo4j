/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api._
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution._
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.operations.{CursorUtils, CypherCoercions, CypherFunctions}
import org.neo4j.internal.kernel.api.IndexQuery.{ExactPredicate, RangePredicate, StringContainsPredicate, StringSuffixPredicate}
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.token.api.TokenConstants
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{TextValue, Value}
import org.neo4j.values.virtual.ListValue

object OperatorCodeGenHelperTemplates {
  sealed trait CursorPoolsType {
    def name: String
  }
  case object NodeCursorPool extends CursorPoolsType {
    override def name: String = "nodeCursorPool"
  }
  case object NodeLabelIndexCursorPool extends CursorPoolsType {
    override def name: String = "nodeLabelIndexCursorPool"
  }
  case object NodeValueIndexCursorPool extends CursorPoolsType {
    override def name: String = "nodeValueIndexCursorPool"
  }
  case object GroupCursorPool extends CursorPoolsType {
    override def name: String = "relationshipGroupCursorPool"
  }

  case object TraversalCursorPool extends CursorPoolsType {
    override def name: String = "relationshipTraversalCursorPool"
  }

  case object RelScanCursorPool extends CursorPoolsType {
    override def name: String = "relationshipScanCursorPool"
  }

  val UNINITIALIZED_LONG_SLOT_VALUE: IntermediateRepresentation = constant(-2L)
  val UNINITIALIZED_REF_SLOT_VALUE: IntermediateRepresentation = constant(null)

  // Constructor parameters
  val DATA_READ_CONSTRUCTOR_PARAMETER: Parameter = param[Read]("dataRead")
  val INPUT_MORSEL_CONSTRUCTOR_PARAMETER: Parameter = param[MorselExecutionContext]("inputMorsel")
  val ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER: Parameter = param[ArgumentStateMaps]("argumentStateMaps")

  // Other method parameters
  val QUERY_RESOURCE_PARAMETER: Parameter = param[QueryResources]("resources")
  val OPERATOR_CLOSER_PARAMETER: Parameter = param[OperatorCloser]("operatorCloser")

  // Fields
  val WORK_IDENTITY_STATIC_FIELD_NAME  = "_workIdentity"
  val DATA_READ: InstanceField = field[Read]("dataRead", load(DATA_READ_CONSTRUCTOR_PARAMETER.name))
  val INPUT_MORSEL: InstanceField = field[MorselExecutionContext]("inputMorsel", load(INPUT_MORSEL_CONSTRUCTOR_PARAMETER.name))

  // IntermediateRepresentation code
  val QUERY_PROFILER: IntermediateRepresentation = load("queryProfiler")
  val QUERY_STATE: IntermediateRepresentation = load("state")
  val QUERY_RESOURCES: IntermediateRepresentation = load("resources")

  val CURSOR_POOL_V: LocalVariable =
    variable[CursorPools]("cursorPools",
                          invoke(QUERY_RESOURCES,
                                 method[QueryResources, CursorPools]("cursorPools")))
  val CURSOR_POOL: IntermediateRepresentation =
    load(CURSOR_POOL_V)

  val OUTPUT_ROW: IntermediateRepresentation =
    load("context")

  val OUTPUT_ROW_MOVE_TO_NEXT: IntermediateRepresentation =
    invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit]("moveToNextRow"))

  val DB_ACCESS: IntermediateRepresentation =
    load("dbAccess")

  val PARAMS: IntermediateRepresentation =
    load("params")

  val EXPRESSION_CURSORS: IntermediateRepresentation =
    load("cursors")

  val EXPRESSION_VARIABLES: IntermediateRepresentation =
    load("expressionVariables")

  val EXECUTION_STATE: IntermediateRepresentation =
    load("executionState")

  val PIPELINE_ID: IntermediateRepresentation =
    load("pipelineId")

  val SUBSCRIBER: LocalVariable = variable[QuerySubscriber]("subscriber",
                                                            invoke(QUERY_STATE, method[QueryState, QuerySubscriber]("subscriber")))
  val SUBSCRIPTION: LocalVariable = variable[FlowControl]("subscription",
                                                          invoke(QUERY_STATE, method[QueryState, FlowControl]("flowControl")))
  val DEMAND: LocalVariable = variable[Long]("demand",
                                             invoke(load(SUBSCRIPTION), method[FlowControl, Long]("getDemand")))

  val SERVED: LocalVariable = variable[Long]("served", constant(0L))

  val HAS_DEMAND: IntermediateRepresentation = lessThan(load(SERVED), load(DEMAND))

  val PRE_POPULATE_RESULTS_V: LocalVariable =
    variable[Boolean]("prePopulateResults",
                      invoke(QUERY_STATE,
                             method[QueryState, Boolean]("prepopulateResults")))

  val PRE_POPULATE_RESULTS: IntermediateRepresentation =
    load(PRE_POPULATE_RESULTS_V)

  val ALLOCATE_NODE_CURSOR: IntermediateRepresentation = allocateCursor(NodeCursorPool)
  val ALLOCATE_NODE_LABEL_CURSOR: IntermediateRepresentation = allocateCursor(NodeLabelIndexCursorPool)
  val ALLOCATE_NODE_INDEX_CURSOR: IntermediateRepresentation = allocateCursor(NodeValueIndexCursorPool)
  val ALLOCATE_GROUP_CURSOR: IntermediateRepresentation = allocateCursor(GroupCursorPool)
  val ALLOCATE_TRAVERSAL_CURSOR: IntermediateRepresentation = allocateCursor(TraversalCursorPool)
  val ALLOCATE_REL_SCAN_CURSOR: IntermediateRepresentation = allocateCursor(RelScanCursorPool)

  val OUTER_LOOP_LABEL_NAME: String = "outerLoop"

  val INPUT_ROW_IS_VALID: IntermediateRepresentation = invoke(loadField(INPUT_MORSEL), method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_IS_VALID: IntermediateRepresentation = invoke(OUTPUT_ROW, method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_FINISHED_WRITING: IntermediateRepresentation = invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit]("finishedWriting"))
  val INPUT_ROW_MOVE_TO_NEXT: IntermediateRepresentation = invokeSideEffect(loadField(INPUT_MORSEL), method[MorselExecutionContext, Unit]("moveToNextRow"))
  val UPDATE_DEMAND: IntermediateRepresentation =
    invokeSideEffect(load(SUBSCRIPTION), method[FlowControl, Unit, Long]("addServed"), load(SERVED))

  // This is used as bound on the work unit for pipelines that does not write to output morsels, e.g. ends with pre-aggregation
  val OUTPUT_COUNTER: LocalVariable = variable[Int]("outputCounter", invoke(QUERY_STATE, method[QueryState, Int]("morselSize")))
  val UPDATE_OUTPUT_COUNTER: IntermediateRepresentation = assign(OUTPUT_COUNTER, subtract(load(OUTPUT_COUNTER), constant(1)))
  val HAS_REMAINING_OUTPUT: IntermediateRepresentation = greaterThan(load(OUTPUT_COUNTER), constant(0))

  val NO_TOKEN: GetStatic = getStatic[TokenConstants, Int]("NO_TOKEN")

  val SET_TRACER: Method = method[Cursor, Unit, KernelReadTracer]("setTracer")
  val NO_KERNEL_TRACER: IntermediateRepresentation = constant(null)
  val NO_OPERATOR_PROFILE_EVENT: IntermediateRepresentation = constant(null)
  private val TRACE_ON_NODE: Method = method[KernelReadTracer, Unit, Long]("onNode")
  private val TRACE_DB_HIT: Method = method[OperatorProfileEvent, Unit]("dbHit")
  private val TRACE_DB_HITS: Method = method[OperatorProfileEvent, Unit, Int]("dbHits")
  val CALL_CAN_CONTINUE: IntermediateRepresentation = invoke(self(), method[ContinuableOperatorTask, Boolean]("canContinue"))


  def allocateCursor(cursorPools: CursorPoolsType): IntermediateRepresentation =
    invoke(
      invoke(CURSOR_POOL, method[CursorPools, CursorPool[_]](cursorPools.name)),
    method[CursorPool[_], Cursor]("allocate"))

  def allNodeScan(cursor: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(loadField(DATA_READ), method[Read, Unit, NodeCursor]("allNodesScan"), cursor)

  def nodeLabelScan(label: IntermediateRepresentation, cursor: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(loadField(DATA_READ), method[Read, Unit, Int, NodeLabelIndexCursor]("nodeLabelScan"), label,
                     cursor)

  def nodeHasLabel(node: IntermediateRepresentation, labelToken: IntermediateRepresentation): IntermediateRepresentation = {
    invokeStatic(
      method[CursorUtils, Boolean, Read, NodeCursor, Long, Int]("nodeHasLabel"),
      loadField(OperatorCodeGenHelperTemplates.DATA_READ),
      ExpressionCompiler.NODE_CURSOR,
      node,
      labelToken)
  }
  def nodeIndexScan(indexReadSession: IntermediateRepresentation,
                    cursor: IntermediateRepresentation,
                    order: IndexOrder,
                    needsValues: Boolean): IntermediateRepresentation =
    invokeSideEffect(loadField(DATA_READ),
                     method[Read, Unit, IndexReadSession, NodeValueIndexCursor, IndexOrder, Boolean]("nodeIndexScan"),
                     indexReadSession, cursor, indexOrder(order), constant(needsValues))

  def nodeIndexSeek(indexReadSession: IntermediateRepresentation,
                    cursor: IntermediateRepresentation,
                    query: IntermediateRepresentation,
                    order: IndexOrder,
                    needsValues: Boolean): IntermediateRepresentation = {
    invokeSideEffect(loadField(DATA_READ),
                     method[Read, Unit, IndexReadSession, NodeValueIndexCursor, IndexOrder, Boolean, Array[IndexQuery]](
                       "nodeIndexSeek"),
                     indexReadSession,
                     cursor,
                     indexOrder(order),
                     constant(needsValues),
                     arrayOf[IndexQuery](query))
  }

  def indexOrder(indexOrder: IndexOrder): IntermediateRepresentation = indexOrder match {
    case IndexOrder.ASCENDING => getStatic[IndexOrder, IndexOrder]("ASCENDING")
    case IndexOrder.DESCENDING => getStatic[IndexOrder, IndexOrder]("DESCENDING")
    case IndexOrder.NONE => getStatic[IndexOrder, IndexOrder]("NONE")
  }

  def exactSeek(prop: Int, expression: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(method[IndexQuery, ExactPredicate, Int, Object]("exact"), constant(prop), expression)

  def lessThanSeek(prop: Int, inclusive: Boolean, expression: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(method[IndexQuery, RangePredicate[_], Int, Value, Boolean, Value, Boolean]("range"), constant(prop), constant(null), constant(false), expression, constant(inclusive))

  def greaterThanSeek(prop: Int, inclusive: Boolean, expression: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(method[IndexQuery, RangePredicate[_], Int, Value, Boolean, Value, Boolean]("range"), constant(prop), expression, constant(inclusive), constant(null), constant(false))

  def rangeBetweenSeek(prop: Int, fromInclusive: Boolean, fromExpression: IntermediateRepresentation, toInclusive: Boolean, toExpression: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(method[IndexQuery, RangePredicate[_], Int, Value, Boolean, Value, Boolean]("range"), constant(prop), fromExpression, constant(fromInclusive), toExpression, constant(toInclusive))

  def stringContainsScan(prop: Int, expression: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(method[IndexQuery, StringContainsPredicate, Int, TextValue]("stringContains"), constant(prop), expression)

  def stringEndsWithScan(prop: Int, expression: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(method[IndexQuery, StringSuffixPredicate, Int, TextValue]("stringSuffix"), constant(prop), expression)

  def singleNode(node: IntermediateRepresentation, cursor: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(loadField(DATA_READ), method[Read, Unit, Long, NodeCursor]("singleNode"), node, cursor)

  def singleRelationship(relationship: IntermediateRepresentation, cursor: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(loadField(DATA_READ), method[Read, Unit, Long, RelationshipScanCursor]("singleRelationship"), relationship, cursor)

  def allocateAndTraceCursor(cursorField: InstanceField, executionEventField: InstanceField, allocate: IntermediateRepresentation): IntermediateRepresentation =
    condition(isNull(loadField(cursorField)))(
      block(
      setField(cursorField, allocate),
      invokeSideEffect(loadField(cursorField), SET_TRACER, loadField(executionEventField))
    ))

  def freeCursor[CURSOR](cursor: IntermediateRepresentation, cursorPools: CursorPoolsType)(implicit out: Manifest[CURSOR]): IntermediateRepresentation =
    invokeSideEffect(
      invoke(CURSOR_POOL, method[CursorPools, CursorPool[_]](cursorPools.name)),
      method[CursorPool[_], Unit, Cursor]("free"), cursor)

  def cursorNext[CURSOR](cursor: IntermediateRepresentation)(implicit out: Manifest[CURSOR]): IntermediateRepresentation =
    invoke(cursor, method[CURSOR, Boolean]("next"))

  def nodeLabelId(labelName: String): IntermediateRepresentation = invoke(DB_ACCESS, method[DbAccess, Int, String]("nodeLabel"), constant(labelName))
  def relationshipTypeId(typeName: String): IntermediateRepresentation = invoke(DB_ACCESS, method[DbAccess, Int, String]("relationshipType"), constant(typeName))

  // Profiling

  private def event(id: Id) = loadField(field[OperatorProfileEvent]("operatorExecutionEvent_" + id.x))
  def profilingCursorNext[CURSOR](cursor: IntermediateRepresentation, id: Id)(implicit out: Manifest[CURSOR]): IntermediateRepresentation = {
    /**
      * {{{
      *   val tmp = cursor.next()
      *   event.row(tmp)
      *   tmp
      * }}}
      */
    val hasNext = "tmp_" + id.x
    block(
      declareAndAssign(typeRefOf[Boolean], hasNext, invoke(cursor, method[CURSOR, Boolean]("next"))),
      condition(isNotNull(event(id))) {
        invokeSideEffect(event(id),
                         method[OperatorProfileEvent, Unit, Boolean]("row"), load(hasNext))
      },
      load(hasNext)
      )
  }
  def profileRow(id: Id): IntermediateRepresentation = {
    condition(isNotNull(event(id)))(invokeSideEffect(event(id), method[OperatorProfileEvent, Unit]("row")))
  }

  def profileRow(id: Id, hasRow: IntermediateRepresentation): IntermediateRepresentation = {
    condition(isNotNull(event(id)))(invokeSideEffect(event(id), method[OperatorProfileEvent, Unit, Boolean]("row"), hasRow))
  }

  def profileRows(id: Id, nRows: Int): IntermediateRepresentation = {
    condition(isNotNull(event(id)))(invokeSideEffect(event(id), method[OperatorProfileEvent, Unit, Int]("rows"),
      constant(nRows)))
  }

  def profileRows(id: Id, nRows: IntermediateRepresentation): IntermediateRepresentation = {
    condition(isNotNull(event(id)))(invokeSideEffect(event(id), method[OperatorProfileEvent, Unit, Int]("rows"),
      nRows))
  }
  def closeEvent(id: Id): IntermediateRepresentation =
    condition(isNotNull(event(id)))(invokeSideEffect(event(id), method[OperatorProfileEvent, Unit]("close")))

  def dbHit(event: IntermediateRepresentation): IntermediateRepresentation = condition(isNotNull(event))(invoke(event, TRACE_DB_HIT))
  def dbHits(event: IntermediateRepresentation, nHits: IntermediateRepresentation): IntermediateRepresentation = condition(isNotNull(event))(invoke(event, TRACE_DB_HITS, nHits))
  def onNode(event: IntermediateRepresentation, node: IntermediateRepresentation): IntermediateRepresentation = condition(isNotNull(event))(invoke(event, TRACE_ON_NODE, node))
  def indexReadSession(offset: Int): IntermediateRepresentation =
    arrayLoad(invoke(QUERY_STATE, method[QueryState, Array[IndexReadSession]]("queryIndexes")), offset)

  def asStorableValue(in: IntermediateRepresentation): IntermediateRepresentation = invokeStatic(method[CypherCoercions, Value, AnyValue]("asStorableValue"), in)
  def asListValue(in: IntermediateRepresentation): IntermediateRepresentation = invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), in)
}
