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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_5.{PatternRelationship, QueryGraph, SimplePatternLength}
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection

class SolvablesTest extends CypherFunSuite {

  val node1Name = "a"
  val node2Name = "b"

  val relName = "rel"
  val rel = PatternRelationship(relName, (node1Name, node2Name), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  test("should compute solvables from empty query graph") {
    val qg = QueryGraph.empty

    Solvables(qg) should equal(Set.empty)
  }

  test("should compute solvables from query graph with pattern relationships") {
    val qg = QueryGraph.empty.addPatternNodes(node1Name, node2Name).addPatternRelationship(rel)

    Solvables(qg) should equal(Set(SolvableRelationship(rel)))
  }
}
