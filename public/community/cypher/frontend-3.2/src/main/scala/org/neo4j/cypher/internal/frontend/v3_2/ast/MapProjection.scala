/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v3_2.ast

import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression.{SemanticContext, _}
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, _}

case class MapProjection(name: Variable, items: Seq[MapProjectionElement], outerScope: Scope = Scope.empty)
                        (val position: InputPosition)
  extends Expression with SimpleTyping {
  protected def possibleTypes = CTMap

  override def semanticCheck(ctx: SemanticContext) =
    items.semanticCheck(ctx) chain
    super.semanticCheck(ctx) ifOkChain // We need to remember the scope to later rewrite this ASTNode
    recordCurrentScope

  def withOuterScope(outerScope: Scope) =
    copy(outerScope = outerScope)(position)
}

sealed trait MapProjectionElement extends SemanticCheckableWithContext with ASTNode

case class LiteralEntry(key: PropertyKeyName, exp: Expression)(val position: InputPosition) extends MapProjectionElement {
  override def semanticCheck(ctx: SemanticContext) = exp.semanticCheck(ctx)
}

case class VariableSelector(id: Variable)(val position: InputPosition) extends MapProjectionElement {
  override def semanticCheck(ctx: SemanticContext) = id.semanticCheck(ctx)
}

case class PropertySelector(id: Variable)(val position: InputPosition) extends MapProjectionElement {
  override def semanticCheck(ctx: SemanticContext) = SemanticCheckResult.success
}

case class AllPropertiesSelector()(val position: InputPosition) extends MapProjectionElement {
  override def semanticCheck(ctx: SemanticContext) = SemanticCheckResult.success
}
