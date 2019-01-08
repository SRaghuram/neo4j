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

import org.neo4j.cypher.internal.frontend.v3_3.SemanticCheck
import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CypherType

case class CoerceTo(expr: Expression, typ: CypherType) extends Expression {
  def position = expr.position

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    expr.semanticCheck(ctx) chain expr.expectType(typ.covariant)
}
