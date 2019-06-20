/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api._
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.morsel.execution._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.operations.{CypherCoercions, CypherFunctions}
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate
import org.neo4j.internal.kernel.api._
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.token.api.TokenConstants
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
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

  // Constructor parameters
  val DATA_READ_CONSTRUCTOR_PARAMETER: Parameter = param[Read]("dataRead")
  val INPUT_MORSEL_CONSTRUCTOR_PARAMETER: Parameter = param[MorselExecutionContext]("inputMorsel")
  val QUERY_RESOURCE_PARAMETER: Parameter = param[QueryResources]("resources")

  // Fields
  val DATA_READ: InstanceField = field[Read]("dataRead", load(DATA_READ_CONSTRUCTOR_PARAMETER.name))
  val INPUT_MORSEL: InstanceField = field[MorselExecutionContext]("inputMorsel", load(INPUT_MORSEL_CONSTRUCTOR_PARAMETER.name))
  val INNER_LOOP: InstanceField = field[Boolean]("innerLoop", constant(false))

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
    invokeSideEffect(load("context"), method[MorselExecutionContext, Unit]("moveToNextRow"))

  val DB_ACCESS: IntermediateRepresentation =
    load("dbAccess")

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

  val INPUT_ROW_IS_VALID: IntermediateRepresentation = invoke(loadField(INPUT_MORSEL), method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_IS_VALID: IntermediateRepresentation = invoke(load("context"), method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_FINISHED_WRITING: IntermediateRepresentation = invokeSideEffect(load("context"), method[MorselExecutionContext, Unit]("finishedWriting"))
  val INPUT_ROW_MOVE_TO_NEXT: IntermediateRepresentation = invokeSideEffect(loadField(INPUT_MORSEL), method[MorselExecutionContext, Unit]("moveToNextRow"))
  val UPDATE_DEMAND: IntermediateRepresentation =
    invokeSideEffect(load(SUBSCRIPTION), method[FlowControl, Unit, Long]("addServed"), load(SERVED))

  val NO_TOKEN: GetStatic = getStatic[TokenConstants, Int]("NO_TOKEN")

  val SET_TRACER: Method = method[Cursor, Unit, KernelReadTracer]("setTracer")
  val NO_KERNEL_TRACER: GetStatic = getStatic[KernelReadTracer, KernelReadTracer]("NONE")
  val NO_OPERATOR_PROFILE_EVENT: GetStatic = getStatic[OperatorProfileEvent, OperatorProfileEvent]("NONE")

  def allocateCursor(cursorPools: CursorPoolsType): IntermediateRepresentation =
    invoke(
      invoke(CURSOR_POOL, method[CursorPools, CursorPool[_]](cursorPools.name)),
    method[CursorPool[_], Cursor]("allocate"))

  def allNodeScan(cursor: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(loadField(DATA_READ), method[Read, Unit, NodeCursor]("allNodesScan"), cursor)

  def nodeLabelScan(label: IntermediateRepresentation, cursor: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(loadField(DATA_READ), method[Read, Unit, Int, NodeLabelIndexCursor]("nodeLabelScan"), label,
                     cursor)

  def nodeIndexSeek(indexReadSession: IntermediateRepresentation,
                    cursor: IntermediateRepresentation,
                    query: IntermediateRepresentation,
                    indexOrder: IndexOrder = IndexOrder.NONE): IntermediateRepresentation = {
    val order = indexOrder match {
      case IndexOrder.ASCENDING => getStatic[IndexOrder, IndexOrder]("ASCENDING")
      case IndexOrder.DESCENDING => getStatic[IndexOrder, IndexOrder]("DESCENDING")
      case IndexOrder.NONE => getStatic[IndexOrder, IndexOrder]("NONE")
    }
    invokeSideEffect(loadField(DATA_READ),
                     method[Read, Unit, IndexReadSession, NodeValueIndexCursor, IndexOrder, Boolean, Array[IndexQuery]](
                       "nodeIndexSeek"),
                     indexReadSession,
                     cursor,
                     order,
                     constant(false),
                     arrayOf[IndexQuery](query))
  }

  def exactSeek(prop: Int, expression: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(method[IndexQuery, ExactPredicate, Int, Object]("exact"), constant(prop), expression)

  def singleNode(node: IntermediateRepresentation, cursor: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(loadField(DATA_READ), method[Read, Unit, Long, NodeCursor]("singleNode"), node, cursor)

  def allocateAndTraceCursor(cursorField: InstanceField, executionEventField: InstanceField, allocate: IntermediateRepresentation): IntermediateRepresentation =
    block(
      setField(cursorField, allocate),
      invokeSideEffect(loadField(cursorField), SET_TRACER, loadField(executionEventField))
    )

  def freeCursor[CURSOR](cursor: IntermediateRepresentation, cursorPools: CursorPoolsType)(implicit out: Manifest[CURSOR]): IntermediateRepresentation =
    invokeSideEffect(
      invoke(CURSOR_POOL, method[CursorPools, CursorPool[_]](cursorPools.name)),
      method[CursorPool[_], Unit, Cursor]("free"), cursor)

  def cursorNext[CURSOR](cursor: IntermediateRepresentation)(implicit out: Manifest[CURSOR]): IntermediateRepresentation =
    invoke(cursor, method[CURSOR, Boolean]("next"))

  def nodeLabelId(labelName: String): IntermediateRepresentation = invoke(DB_ACCESS, method[DbAccess, Int, String]("nodeLabel"), constant(labelName))

  // Profiling
  def profileRow(id: Id): IntermediateRepresentation = {
    invokeSideEffect(loadField(field[OperatorProfileEvent]("operatorExecutionEvent_" + id.x)), method[OperatorProfileEvent, Unit]("row"))
  }

  def indexReadSession(offset: Int): IntermediateRepresentation =
    arrayLoad(invoke(QUERY_STATE, method[QueryState, Array[IndexReadSession]]("queryIndexes")), offset)

  def asStorableValue(in: IntermediateRepresentation): IntermediateRepresentation = invokeStatic(method[CypherCoercions, Value, AnyValue]("asStorableValue"), in)
  def asListValue(in: IntermediateRepresentation): IntermediateRepresentation = invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), in)
}
