/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{InstanceField, IntermediateRepresentation, Parameter}
import org.neo4j.cypher.internal.runtime.morsel.{CursorPool, CursorPools, MorselExecutionContext}
import org.neo4j.internal.kernel.api._

object OperatorCodeGenHelperTemplates {
  sealed trait CursorPoolsType {
    def name: String
  }
  case object NodeCursorPool extends CursorPoolsType {
    override def name: String = "nodeCursorPool"
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

  // Fields
  val DATA_READ: InstanceField = field[Read]("dataRead", load(DATA_READ_CONSTRUCTOR_PARAMETER.name))
  val INPUT_MORSEL: InstanceField = field[MorselExecutionContext]("inputMorsel", load(INPUT_MORSEL_CONSTRUCTOR_PARAMETER.name))
  val INNER_LOOP: InstanceField = field[Boolean]("innerLoop", constant(false))

  // IntermediateRepresentation code
  val CURSOR_POOL: IntermediateRepresentation =
    load("cursorPools")

  val OUTPUT_ROW: IntermediateRepresentation =
    load("context")

  val OUTPUT_ROW_MOVE_TO_NEXT: IntermediateRepresentation =
    invokeSideEffect(load("context"), method[MorselExecutionContext, Unit]("moveToNextRow"))

  val DB_ACCESS: IntermediateRepresentation =
    load("dbAccess")

  val PRE_POPULATE_RESULTS: IntermediateRepresentation =
    load("prePopulateResults")

  val ALLOCATE_NODE_CURSOR: IntermediateRepresentation = allocateCursor(NodeCursorPool)
  val ALLOCATE_GROUP_CURSOR: IntermediateRepresentation = allocateCursor(GroupCursorPool)
  val ALLOCATE_TRAVERSAL_CURSOR: IntermediateRepresentation = allocateCursor(TraversalCursorPool)

  val INPUT_ROW_IS_VALID: IntermediateRepresentation = invoke(loadField(INPUT_MORSEL), method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_IS_VALID: IntermediateRepresentation = invoke(load("context"), method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_FINISHED_WRITING: IntermediateRepresentation = invokeSideEffect(load("context"), method[MorselExecutionContext, Unit]("finishedWriting"))
  val INPUT_ROW_MOVE_TO_NEXT: IntermediateRepresentation = invokeSideEffect(loadField(INPUT_MORSEL), method[MorselExecutionContext, Unit]("moveToNextRow"))

  def allocateCursor(cursorPools: CursorPoolsType): IntermediateRepresentation =
    invoke(
      invoke(CURSOR_POOL, method[CursorPools, CursorPool[_]](cursorPools.name)),
    method[CursorPool[_], Cursor]("allocate"))

  def allNodeScan(cursor: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(loadField(DATA_READ), method[Read, Unit, NodeCursor]("allNodesScan"), cursor)

  def singleNode(node: IntermediateRepresentation, cursor: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(loadField(DATA_READ), method[Read, Unit, Long, NodeCursor]("singleNode"), node, cursor)

  def freeCursor[CURSOR](cursor: IntermediateRepresentation, cursorPools: CursorPoolsType)(implicit out: Manifest[CURSOR]): IntermediateRepresentation =
    invokeSideEffect(
      invoke(CURSOR_POOL, method[CursorPools, CursorPool[_]](cursorPools.name)),
      method[CursorPool[_], Unit, Cursor]("free"), cursor)

  def cursorNext[CURSOR](cursor: IntermediateRepresentation)(implicit out: Manifest[CURSOR]): IntermediateRepresentation =
    invoke(cursor, method[CURSOR, Boolean]("next"))

}
