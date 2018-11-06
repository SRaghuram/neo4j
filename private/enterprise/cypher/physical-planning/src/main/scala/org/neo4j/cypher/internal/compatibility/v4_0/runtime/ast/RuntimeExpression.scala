/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.compatibility.v4_0.runtime.ast

import org.opencypher.v9_0.ast.semantics.{SemanticCheck, SemanticCheckResult, SemanticCheckableExpression}
import org.opencypher.v9_0.expressions.{Expression => ASTExpression}
import org.opencypher.v9_0.util.InputPosition

trait RuntimeExpression extends ASTExpression with SemanticCheckableExpression {
  override def semanticCheck(ctx: ASTExpression.SemanticContext): SemanticCheck = SemanticCheckResult.success

  override def position: InputPosition = InputPosition.NONE
}
