/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateRepresentation._
import org.neo4j.cypher.internal.runtime.compiled.expressions.{InstanceField, IntermediateRepresentation, Parameter}
import org.neo4j.cypher.internal.runtime.morsel.{CursorPool, MorselExecutionContext}
import org.neo4j.internal.kernel.api.{Cursor, NodeCursor, Read}

object OperatorCodeGenHelperTemplates {
  // Constructor parameters
  val DATA_READ_CONSTRUCTOR_PARAMETER: Parameter = param[Read]("dataRead")
  val INPUT_MORSEL_CONSTRUCTOR_PARAMETER: Parameter = param[MorselExecutionContext]("inputMorsel")

  // Fields
  val DATA_READ: InstanceField = field[Read]("dataRead", load(DATA_READ_CONSTRUCTOR_PARAMETER.name))
  val CURSOR: InstanceField = field[NodeCursor]("cursor")
  val INPUT_MORSEL: InstanceField = field[MorselExecutionContext]("inputMorsel", load(INPUT_MORSEL_CONSTRUCTOR_PARAMETER.name))
  val INNER_LOOP: InstanceField = field[Boolean]("innerLoop", constant(false))

  // IntermediateRepresentation code
  val OUTPUT_ROW: IntermediateRepresentation =
    load("context")

  val OUTPUT_ROW_MOVE_TO_NEXT: IntermediateRepresentation =
    invokeSideEffect(load("context"), method[MorselExecutionContext, Unit]("moveToNextRow"))

  val DB_ACCESS: IntermediateRepresentation =
    load("dbAccess")

  val ALLOCATE_NODE_CURSOR: IntermediateRepresentation =
    invoke(load("nodeCursorPool"), method[CursorPool[_], Cursor]("allocate"))

  val FREE_NODE_CURSOR: IntermediateRepresentation =
    invokeSideEffect(load("nodeCursorPool"), method[CursorPool[NodeCursor], Unit, Cursor]("free"), loadField(CURSOR))

  val ALL_NODE_SCAN: IntermediateRepresentation = invokeSideEffect(loadField(DATA_READ), method[Read, Unit, NodeCursor]("allNodesScan"), loadField(CURSOR))

  val INPUT_ROW_IS_VALID: IntermediateRepresentation = invoke(loadField(INPUT_MORSEL), method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_IS_VALID: IntermediateRepresentation = invoke(load("context"), method[MorselExecutionContext, Boolean]("isValidRow"))
  val OUTPUT_ROW_FINISHED_WRITING: IntermediateRepresentation = invokeSideEffect(load("context"), method[MorselExecutionContext, Unit]("finishedWriting"))
  val INPUT_ROW_MOVE_TO_NEXT: IntermediateRepresentation = invokeSideEffect(loadField(INPUT_MORSEL), method[MorselExecutionContext, Unit]("moveToNextRow"))
}
