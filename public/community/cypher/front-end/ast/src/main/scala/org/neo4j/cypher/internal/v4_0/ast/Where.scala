/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v4_0.ast

import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Property}
import org.neo4j.cypher.internal.util.symbols._
import org.neo4j.cypher.internal.util.{ASTNode, InputPosition}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticCheckable, SemanticExpressionCheck, SemanticPatternCheck}

case class Where(expression: Expression)(val position: InputPosition)
  extends ASTNode with SemanticCheckable {

  def dependencies = expression.dependencies

  def semanticCheck =
    SemanticExpressionCheck.simple(expression) chain
    SemanticPatternCheck.checkValidPropertyKeyNames(expression.findByAllClass[Property].map(prop => prop.propertyKey), expression.position) chain
    SemanticExpressionCheck.expectType(CTBoolean.covariant, expression)
}
