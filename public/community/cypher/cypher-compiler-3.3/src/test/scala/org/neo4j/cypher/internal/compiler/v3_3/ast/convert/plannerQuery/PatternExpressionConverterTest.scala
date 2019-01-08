/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_3.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, Variable}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{SemanticDirection, ast}
import org.neo4j.cypher.internal.ir.v3_3._
import org.neo4j.cypher.internal.ir.v3_3.helpers.ExpressionConverters._

class PatternExpressionConverterTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val aNode: ast.NodePattern = ast.NodePattern(Some(ast.Variable("a")(pos)), Seq.empty, None)_
  val bNode: ast.NodePattern = ast.NodePattern(Some(ast.Variable("b")(pos)), Seq.empty, None)_
  val unnamedVariable: ast.Variable = ast.Variable("  UNNAMED1")_
  val anonymousNode: ast.NodePattern = ast.NodePattern(Some(unnamedVariable), Seq.empty, None)_
  val rRel: ast.RelationshipPattern = ast.RelationshipPattern(Some(ast.Variable("r")(pos)), Seq.empty, None, None, SemanticDirection.OUTGOING)_
  val TYP: ast.RelTypeName = ast.RelTypeName("TYP")_

  val rRelWithType: ast.RelationshipPattern = rRel.copy(types = Seq(TYP)) _
  val planRel = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val planRelWithType = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq(TYP), SimplePatternLength)

  private def projections(names: String*): Map[String, Expression] = names.map {
    case x => x -> Variable(x)(pos)
  }.toMap

  test("(a)-[r]->(b)") {
    // Given
    val patternExpression = createPatternExpression(aNode, rRel, bNode)

    // When
    val qg = patternExpression.asQueryGraph

    // Then
    qg.selections should equal(Selections())
    qg.patternRelationships should equal(Set(planRel))
    qg.argumentIds should equal(Set("a", "r", "b"))
    qg.patternNodes should equal(Set("a", "b"))
  }

  test("(a)-[r:TYP]->(b)") {
    // Given
    val patternExpression = createPatternExpression(aNode, rRelWithType, bNode)

    // When
    val qg = patternExpression.asQueryGraph

    // Then
    qg.selections should equal(Selections())
    qg.patternRelationships should equal(Set(planRelWithType))
    qg.argumentIds should equal(Set("a", "r", "b"))
    qg.patternNodes should equal(Set("a", "b"))
  }

  test("(a)-[r]->(  UNNAMED1)") {
    // Given
    val patternExpression = createPatternExpression(aNode, rRel, anonymousNode)

    // When
    val qg = patternExpression.asQueryGraph

    // Then
    qg.selections should equal(Selections())
    qg.patternRelationships should equal(Set(planRel.copy(nodes = ("a", "  UNNAMED1"))))
    qg.argumentIds should equal(Set("a", "r"))
    qg.patternNodes should equal(Set("a", "  UNNAMED1"))
  }

  test("(a)-[r]->(b:Label)") {
    val labelName: ast.LabelName = ast.LabelName("Label")_
    // Given
    val patternExpression = createPatternExpression(aNode, rRel, bNode.copy(labels = Seq(labelName))(pos))

    // When
    val qg = patternExpression.asQueryGraph

    // Then
    val predicate: ast.HasLabels = ast.HasLabels(ast.Variable("b")(pos), Seq(labelName))_
    qg.selections should equal(Selections(Set(Predicate(Set("b"), predicate))))
    qg.patternRelationships should equal(Set(planRel))
    qg.argumentIds should equal(Set("a", "r", "b"))
    qg.patternNodes should equal(Set("a", "b"))
  }

  def createPatternExpression(n1: ast.NodePattern, r: ast.RelationshipPattern, n2: ast.NodePattern): ast.PatternExpression =
    ast.PatternExpression(ast.RelationshipsPattern(ast.RelationshipChain(n1, r, n2) _) _)
}
