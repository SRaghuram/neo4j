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
package org.neo4j.cypher.internal.frontend.v3_2.parser

import org.parboiled.scala._

class BaseRulesTest extends ParserTest[Any, Any] with Base {

  test("testWhitespaceHandling") {
    implicit val parserToTest: Rule1[Boolean] = "a" ~ WS ~ "b" ~ push(true)

    parsing("a b") shouldGive true
    parsing("a　b") shouldGive true
  }

  def convert(result: Any): Any = result
}
