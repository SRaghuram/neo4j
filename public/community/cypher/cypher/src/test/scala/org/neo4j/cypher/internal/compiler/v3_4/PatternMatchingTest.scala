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
package org.neo4j.cypher.internal.compiler.v3_4

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.runtime.interpreted.ValueComparisonHelper.beEquivalentTo
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.runtime.interpreted.commands.RelatedTo
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.HasLabel
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.UnresolvedLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.matching.PatternMatchingBuilder
import org.neo4j.cypher.internal.runtime.interpreted.symbols.SymbolTable
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, PatternGraphBuilder, QueryStateTestSupport}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection

class PatternMatchingTest extends ExecutionEngineFunSuite with PatternGraphBuilder with QueryStateTestSupport {
  val symbols = SymbolTable(Map("a" -> CTNode))
  val patternRelationship: RelatedTo = RelatedTo("a", "b", "r", Seq.empty, SemanticDirection.OUTGOING)
  val rightNode = patternRelationship.right
  val label = UnresolvedLabel("Person")

  test("should_handle_a_single_relationship_with_no_matches") {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq.empty, Set("a", "r", "b"))
    val aNode = createNode()

    // When
    val result = withQueryState { queryState =>
      matcher.getMatches(ExecutionContext.empty.set("a", aNode), queryState).toList
    }

    // Then
    result shouldBe empty
  }

  test("should_handle_a_single_relationship_with_1_match") {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq.empty, Set("a", "r", "b"))
    val aNode = createNode()
    val bNode = createNode()
    val relationship = relate(aNode, bNode)

    // When
    val result = withQueryState { queryState =>
      matcher.getMatches(ExecutionContext.empty.set("a", aNode), queryState).toList
    }

    // Then
    result should beEquivalentTo(List(Map("a" -> aNode, "b" -> bNode, "r" -> relationship)))
  }

  test("should_handle_a_mandatory_labeled_node_with_no_matches") {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq(HasLabel(Variable("b"), label)), Set("a", "r", "b"))
    val aNode = createNode()
    val bNode = createNode()
    relate(aNode, bNode)

    // When
    val result = withQueryState { queryState =>
      matcher.getMatches(ExecutionContext.empty.set("a", aNode), queryState).toList
    }

    // Then
    result shouldBe empty
  }

  test("should_handle_a_mandatory_labeled_node_with_matches") {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq(HasLabel(Variable("b"), label)), Set("a", "r", "b"))
    val aNode = createNode()
    val bNode = createLabeledNode("Person")
    val relationship = relate(aNode, bNode)

    // When
    val result = withQueryState { queryState =>
      matcher.getMatches(ExecutionContext.empty.set("a", aNode), queryState).toList
    }

    // Then
    result should beEquivalentTo(List(Map("a" -> aNode, "b" -> bNode, "r" -> relationship)))
  }
}
