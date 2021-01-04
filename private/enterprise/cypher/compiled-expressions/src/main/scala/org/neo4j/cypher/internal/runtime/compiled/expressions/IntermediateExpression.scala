/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.LocalVariable

/**
 * Describes a compiled expression
 *
 * @param ir The actual instructions to perform
 * @param fields The fields necessary for performing instructions
 * @param variables The top-level variables necessary for performing instructions
 * @param nullChecks Instructions for null-checking, if any of the instructions are null the whole expression will be null
 * @param requireNullCheck If `true` it is required to perform null-checking before evaluating the instructions, if `false`
 *                         the expression might still be null but it will not fail when the instructions are performed.
 */
case class IntermediateExpression(ir: IntermediateRepresentation,
                                  fields: Seq[Field],
                                  variables: Seq[LocalVariable],
                                  nullChecks: Set[IntermediateRepresentation],
                                  requireNullCheck: Boolean = true) {
  def withVariable(variable: LocalVariable*): IntermediateExpression = copy(variables = variables ++ variable)
}

object IntermediateExpression {
  val EMPTY: IntermediateExpression = IntermediateExpression(IntermediateRepresentation.noop(), Seq.empty, Seq.empty, Set.empty)
}
