/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheckResult}

// Scope expressions bundle together variables of a new scope
// together with any child expressions that get evaluated in a context where
// these variables are bound
//
// This is a hard contract: There must be no child expressions of a scope expressions
// that are not
// - either introduced variables
// - or child expressions in a scope where those variables are bound
//
trait ScopeExpression extends Expression {
  def introducedVariables: Set[Variable]
}

case class FilterScope(variable: Variable, innerPredicate: Option[Expression])(val position: InputPosition) extends ScopeExpression {
  override def semanticCheck(ctx: SemanticContext) = SemanticCheckResult.success
  val introducedVariables = Set(variable)
}

case class ExtractScope(variable: Variable, innerPredicate: Option[Expression], extractExpression: Option[Expression])(val position: InputPosition) extends ScopeExpression {
  override def semanticCheck(ctx: SemanticContext) = SemanticCheckResult.success
  val introducedVariables = Set(variable)
}

case class ReduceScope(accumulator: Variable, variable: Variable, expression: Expression)(val position: InputPosition) extends ScopeExpression {
  override def semanticCheck(ctx: SemanticContext) = SemanticCheckResult.success
  val introducedVariables = Set(accumulator, variable)
}
