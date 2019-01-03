/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5

import org.neo4j.cypher.internal.compatibility.v3_5.helpers.as4_0
import org.neo4j.cypher.internal.v3_5.{ast => astV3_5}
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.{ast => astv4_0}

case class StatementWrapper(statement: astV3_5.Statement) extends astv4_0.Statement {
  override def semanticCheck: SemanticCheck = ???

  override lazy val returnColumns: List[String] = statement.returnColumns

  override lazy val position: InputPosition = as4_0(statement.position)
}
