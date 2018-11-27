/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast

import org.neo4j.cypher.internal.v3_5.ast.semantics.{SemanticCheck, SemanticCheckResult, SemanticCheckableExpression}
import org.neo4j.cypher.internal.v3_5.expressions.{LogicalVariable, Expression => ASTExpression}
import org.neo4j.cypher.internal.v3_5.util.{InputPosition, InternalException}

abstract class RuntimeVariable(override val name: String) extends LogicalVariable with SemanticCheckableExpression {
  override def semanticCheck(ctx: ASTExpression.SemanticContext): SemanticCheck = SemanticCheckResult.success

  override def position: InputPosition = InputPosition.NONE

  override def copyId = fail()

  override def renameId(newName: String) = fail()

  override def bumpId = fail()

  private def fail(): Nothing = throw new InternalException("Tried using a RuntimeVariable as Variable")
}
