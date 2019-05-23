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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Property, PropertyKeyName, Variable}
import org.neo4j.cypher.internal.v4_0.util.DummyPosition
import org.neo4j.cypher.internal.v4_0.util.attribution.{IdGen, SequentialIdGen}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class ProjectingPlanTest extends CypherFunSuite {
  private val pos = DummyPosition(0)
  private implicit val idGen: IdGen = new SequentialIdGen()

  trait Projector {
    def apply(input: LogicalPlan, projections: Map[String, Expression]): ProjectingPlan
    def name: String
  }

  private val project: Projector = new Projector {
    override def apply(input: LogicalPlan,
                       projections: Map[String, Expression]): ProjectingPlan = Projection(input, projections)
    override val name: String = "Projection"
  }
  private val distinct: Projector = new Projector {
    override def apply(input: LogicalPlan,
                       projections: Map[String, Expression]): ProjectingPlan = Distinct(input, projections)
    override val name: String = "Distinct"
  }

  for(projector <- Seq(project, distinct)) {
    test(s"should have empty availableCachedProperties in ${projector.name} if property projected") {
      val xDotFoo = Property(Variable("x")(pos), PropertyKeyName("foo")(pos))(pos)
      val projections = Map("xfoo" -> xDotFoo)

      val input = FakePlan(Map(xDotFoo -> cached("x.foo")))
      val output = projector(input, projections)

      output.availableCachedProperties should be(empty)
    }

    test(s"should rename availableCachedProperties in ${projector.name} if node projected") {
      val x = Variable("x")(pos)
      val xDotFoo = Property(x, PropertyKeyName("foo")(pos))(pos)
      val yDotFoo = Property(Variable("y")(pos), PropertyKeyName("foo")(pos))(pos)
      val projections = Map("y" -> Variable("x")(pos))

      val input = FakePlan(Map(xDotFoo -> cached("x.foo")))
      val output = projector(input, projections)

      output.availableCachedProperties should equal(Map(yDotFoo -> cached("x.foo")))
    }

    test(s"should rename availableCachedProperties in ${projector.name} if node projected, when column name does not match") {
      val x = Variable("x")(pos)
      val xDotFoo = Property(x, PropertyKeyName("foo")(pos))(pos)
      val yDotFoo = Property(Variable("y")(pos), PropertyKeyName("foo")(pos))(pos)
      val projections = Map("y" -> x)

      val input = FakePlan(Map(xDotFoo -> cached("z.foo")))
      val output = projector(input, projections)

      output.availableCachedProperties should equal(Map(yDotFoo -> cached("z.foo")))
    }

    test(s"should have empty availableCachedProperties in ${projector.name} if cached node property projected") {
      val xDotFoo = Property(Variable("x")(pos), PropertyKeyName("foo")(pos))(pos)
      val projections = Map("y" -> cached("x.foo"))

      val input = FakePlan(Map(xDotFoo -> cached("x.foo")))
      val output = projector(input, projections)

      output.availableCachedProperties should be(empty)
    }

    test(s"should provide multiple available properties from indexes if things are projection more than once  in ${projector.name}") {
      val x = Variable("x")(pos)
      val xDotFoo = Property(x, PropertyKeyName("foo")(pos))(pos)
      val yDotFoo = Property(Variable("y")(pos), PropertyKeyName("foo")(pos))(pos)
      val zDotFoo = Property(Variable("z")(pos), PropertyKeyName("foo")(pos))(pos)
      val projections = Map("y" -> x, "z" -> x)

      val input = FakePlan(Map(xDotFoo -> cached("x.foo")))
      val output = projector(input, projections)

      output.availableCachedProperties should equal(Map(yDotFoo -> cached("x.foo"), zDotFoo -> cached("x.foo")))
    }

    test(s"should not hide availableCachedProperties in ${projector.name} if projecting different property than what has been cached") {
      val xDotFoo = Property(Variable("x")(pos), PropertyKeyName("foo")(pos))(pos)
      val xDotBar = Property(Variable("x")(pos), PropertyKeyName("bar")(pos))(pos)
      val projections = Map("xbar" -> xDotBar)

      val input = FakePlan(Map(xDotFoo -> cached("x.foo")))
      val output = projector(input, projections)

      output.availableCachedProperties should equal(Map(xDotFoo -> cached("x.foo")))
    }

    test(s"should only pass along availableCachedProperties in ${projector.name} that are not projected") {
      val xDotFoo = Property(Variable("x")(pos), PropertyKeyName("foo")(pos))(pos)
      val xDotBar = Property(Variable("x")(pos), PropertyKeyName("bar")(pos))(pos)
      val xDotBaz = Property(Variable("x")(pos), PropertyKeyName("baz")(pos))(pos)
      val projections = Map("xbar" -> xDotBar)

      val input = FakePlan(Map(xDotFoo -> cached("x.foo"), xDotBar -> cached("x.bar"), xDotBaz -> cached("x.baz")))
      val output = projector(input, projections)

      output.availableCachedProperties should equal(Map(xDotFoo -> cached("x.foo"), xDotBaz -> cached("x.baz")))
    }
  }

  case class FakePlan(override val availableCachedProperties: Map[Property, CachedProperty] = Map.empty)(implicit idGen: IdGen)
    extends LogicalPlan(idGen) with LazyLogicalPlan {
    override def rhs: Option[LogicalPlan] = None
    override def lhs: Option[LogicalPlan] = None

    override def availableSymbols: Set[String] = Set.empty
  }

  private def cached(varAndProp: String): CachedProperty = {
    val array = varAndProp.split("\\.", 2)
    val (v, prop) = (array(0), array(1))
    CachedProperty(v, PropertyKeyName(prop)(pos), CACHED_NODE)(pos)
  }
}
