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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.DeleteExpression
import org.neo4j.cypher.internal.ir.MergeNodePattern
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UpdateGraphTest extends CypherFunSuite {
  private val pos = DummyPosition(0)

  test("should not be empty after adding label to set") {
    val original = QueryGraph()
    val setLabel = SetLabelPattern("name", Seq.empty)

    original.addMutatingPatterns(setLabel).containsUpdates should be(true)
  }

  test("overlap when reading all labels and creating a specific label") {
    //MATCH (a) CREATE (:L)
    val qg = QueryGraph(patternNodes = Set("a"))
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qg) shouldBe true
  }

  test("overlap when reading and creating the same label") {
    //MATCH (a:L) CREATE (b:L)
    val selections = Selections.from(HasLabels(Variable("a")(pos), Seq(LabelName("L")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qg) shouldBe true
  }

  test("no overlap when reading and creating different labels") {
    //MATCH (a:L1:L2) CREATE (b:L3)
    val selections = Selections.from(HasLabels(Variable("a")(pos), Seq(LabelName("L1")(pos), LabelName("L2")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L3")))

    ug.overlaps(qg) shouldBe false
  }

  test("no overlap when properties don't overlap even though labels do") {
    //MATCH (a {foo: 42}) CREATE (a:L)
    val selections = Selections.from(In(Variable("a")(pos),
      Property(Variable("a")(pos), PropertyKeyName("foo")(pos))(pos))(pos))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qg) shouldBe false
  }

  test("no overlap when properties don't overlap even though labels explicitly do") {
    //MATCH (a:L {foo: 42}) CREATE (a:L)
    val selections = Selections.from(Seq(
      In(Variable("a")(pos),Property(Variable("a")(pos), PropertyKeyName("foo")(pos))(pos))(pos),
      HasLabels(Variable("a")(pos), Seq(LabelName("L")(pos)))(pos)))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qg) shouldBe false
  }

  test("overlap when reading all rel types and creating a specific type") {
    //MATCH (a)-[r]->(b)  CREATE (a)-[r2:T]->(b)
    val qg = QueryGraph(patternRelationships =
      Set(PatternRelationship("r", ("a", "b"),
        SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)))
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T", "b")))

    ug.overlaps(qg) shouldBe true
  }

  test("no overlap when reading and writing different rel types") {
    //MATCH (a)-[r:T1]->(b)  CREATE (a)-[r2:T]->(b)
    val qg = QueryGraph(patternRelationships =
      Set(PatternRelationship("r", ("a", "b"),
        SemanticDirection.OUTGOING, Seq(RelTypeName("T1")(pos)), SimplePatternLength)))
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T2", "b")))

    ug.overlaps(qg) shouldBe false
  }

  test("overlap when reading and writing same rel types") {
    //MATCH (a)-[r:T1]->(b)  CREATE (a)-[r2:T1]->(b)
    val qg = QueryGraph(patternRelationships =
      Set(PatternRelationship("r", ("a", "b"),
        SemanticDirection.OUTGOING, Seq(RelTypeName("T1")(pos)), SimplePatternLength)))
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T1", "b")))

    ug.overlaps(qg) shouldBe true
  }

  test("no overlap when reading and writing same rel types but matching on rel property") {
    //MATCH (a)-[r:T1 {foo: 42}]->(b)  CREATE (a)-[r2:T1]->(b)
    val selections = Selections.from(In(Variable("a")(pos),
      Property(Variable("r")(pos), PropertyKeyName("foo")(pos))(pos))(pos))
    val qg = QueryGraph(patternRelationships =
      Set(PatternRelationship("r", ("a", "b"),
        SemanticDirection.OUTGOING, Seq(RelTypeName("T1")(pos)), SimplePatternLength)),
      selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T1", "b")))

    ug.overlaps(qg) shouldBe false
  }

  test("overlap when reading and writing same property and rel type") {
    //MATCH (a)-[r:T1 {foo: 42}]->(b)  CREATE (a)-[r2:T1]->(b)
    val selections = Selections.from(In(Variable("a")(pos),
      Property(Variable("r")(pos), PropertyKeyName("foo")(pos))(pos))(pos))
    val qg = QueryGraph(patternRelationships =
      Set(PatternRelationship("r", ("a", "b"),
        SemanticDirection.OUTGOING, Seq(RelTypeName("T1")(pos)), SimplePatternLength)),
      selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        CreatePattern(
          Nil,
          List(
            CreateRelationship("r2", "a", RelTypeName("T1")(pos), "b", SemanticDirection.OUTGOING,
              Some(
                MapExpression(Seq(
                  (PropertyKeyName("foo")(pos), SignedDecimalIntegerLiteral("42")(pos))
                ))(pos)
              )
            )
          )
        )
      ))

    ug.overlaps(qg) shouldBe true
  }

  test("overlap when reading, deleting and merging") {
    //MATCH (a:L1:L2) DELETE a CREATE (b:L3)
    val selections = Selections.from(HasLabels(Variable("a")(pos), Seq(LabelName("L1")(pos), LabelName("L2")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(
      DeleteExpression(Variable("a")(pos), forced = false),
      MergeNodePattern(
        CreateNode("b", Seq(LabelName("L3")(pos), LabelName("L3")(pos)), None),
        QueryGraph.empty, Seq.empty, Seq.empty)
    ))

    ug.overlaps(qg) shouldBe true
  }

  test("overlap when reading and deleting with collections") {
    //... WITH collect(a) as col DELETE col[0]
    val qg = QueryGraph(argumentIds = Set("col"))
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(
      DeleteExpression(Variable("col")(pos), forced = false)
    ))

    ug.overlaps(qg) shouldBe true
  }

  private def createNode(name: String, labels: String*) =
    CreatePattern(List(CreateNode(name, labels.map(l => LabelName(l)(pos)), None)), Nil)

  private def createRelationship(name: String, start: String, relType: String, end: String) =
    CreatePattern(
      Nil,
      List(CreateRelationship(name, start, RelTypeName(relType)(pos), end, SemanticDirection.OUTGOING, None))
    )
}
