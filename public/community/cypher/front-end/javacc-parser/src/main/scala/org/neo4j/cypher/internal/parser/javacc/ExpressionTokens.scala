/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.parser.javacc

import org.neo4j.cypher.internal.parser.javacc.CypherConstants.DECIMAL_DOUBLE
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.IDENTIFIER
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.LBRACKET
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.LCURLY
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.LPAREN
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.MINUS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.PLUS
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.STRING_LITERAL1
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.STRING_LITERAL2
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNSIGNED_DECIMAL_INTEGER
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNSIGNED_HEX_INTEGER
import org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNSIGNED_OCTAL_INTEGER

object ExpressionTokens {

  val tokens = Set(
    DECIMAL_DOUBLE,
    IDENTIFIER,
    LBRACKET,
    LCURLY,
    LPAREN,
    MINUS,
    PLUS,
    STRING_LITERAL1,
    STRING_LITERAL2,
    UNSIGNED_DECIMAL_INTEGER,
    UNSIGNED_HEX_INTEGER,
    UNSIGNED_OCTAL_INTEGER,
  )
}
