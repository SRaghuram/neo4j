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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.idp

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{LogicalPlanningContext, Metrics, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{SemanticDirection, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_3._
import org.neo4j.cypher.internal.v3_3.logical.plans.{Expand, ExpandAll, ExpandInto, LogicalPlan}

class ExpandSolverStepTest extends CypherFunSuite  with AstConstructionTestSupport {
  self =>
  private val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(0))

  implicit def converter(s: Symbol): String = s.toString()


  case class TestPlan(availableSymbols: Set[String] = Set.empty) extends LogicalPlan {

    override def lhs: Option[LogicalPlan] = None

    override def rhs: Option[LogicalPlan] = None

    override def solved: PlannerQuery with CardinalityEstimation = self.solved

    override def strictness: StrictnessMode = ???
  }

  private val plan1 = TestPlan()


  private val pattern1 = PatternRelationship('r1, ('a, 'b), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val pattern2 = PatternRelationship('r2, ('b, 'c), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  private val table = new IDPTable[LogicalPlan]()
  private val qg = mock[QueryGraph]

  private implicit val context = LogicalPlanningContext(mock[PlanContext], LogicalPlanProducer(mock[CardinalityModel]),
    mock[Metrics], mock[SemanticTable], mock[QueryGraphSolver], notificationLogger = mock[InternalNotificationLogger])

  test("does not expand based on empty table") {
    implicit val registry = IdRegistry[PatternRelationship]

    expandSolverStep(qg)(registry, register(pattern1, pattern2), table) should be(empty)
  }

  test("expands if an unsolved pattern relationship overlaps once with a single solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]

    val plan = TestPlan(Set[String]('a, 'r1, 'b))
    table.put(register(pattern1), plan)

    expandSolverStep(qg)(registry, register(pattern1, pattern2), table).toSet should equal(Set(
      Expand(plan, 'b, SemanticDirection.OUTGOING, Seq.empty, 'c, 'r2, ExpandAll)(solved)
    ))
  }

  test("expands if an unsolved pattern relationships overlaps twice with a single solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]

    val plan = TestPlan(Set[String]('a, 'r1, 'b))

    table.put(register(pattern1), plan)

    val patternX = PatternRelationship('r2, ('a, 'b), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    expandSolverStep(qg)(registry, register(pattern1, patternX), table).toSet should equal(Set(
      Expand(plan, 'a, SemanticDirection.OUTGOING, Seq.empty, 'b, 'r2, ExpandInto)(solved),
      Expand(plan, 'b, SemanticDirection.INCOMING, Seq.empty, 'a, 'r2, ExpandInto)(solved)
    ))
  }

  test("does not expand if an unsolved pattern relationship does not overlap with a solved plan") {
    implicit val registry = IdRegistry[PatternRelationship]

    when(plan1.availableSymbols).thenReturn(Set[String]('a, 'r1, 'b))
    table.put(register(pattern1), plan1)

    val patternX = PatternRelationship('r2, ('x, 'y), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    expandSolverStep(qg)(registry, register(pattern1, patternX), table).toSet should be(empty)
  }

  test("expands if an unsolved pattern relationship overlaps with multiple solved plans") {
    implicit val registry = IdRegistry[PatternRelationship]
    val plan = TestPlan(Set[String]('a, 'r1, 'b, 'c, 'r2, 'd))

    table.put(register(pattern1, pattern2), plan)

    val pattern3 = PatternRelationship('r3, ('b, 'c), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    expandSolverStep(qg)(registry, register(pattern1, pattern2, pattern3), table).toSet should equal(Set(
      Expand(plan, 'b, SemanticDirection.OUTGOING, Seq.empty, 'c, 'r3, ExpandInto)(solved),
      Expand(plan, 'c, SemanticDirection.INCOMING, Seq.empty, 'b, 'r3, ExpandInto)(solved)
    ))
  }

  def register[X](patRels: X*)(implicit registry: IdRegistry[X]) = registry.registerAll(patRels)
}
