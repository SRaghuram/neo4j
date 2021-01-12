/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable.ArrayBuffer

class TreeBuilderTest extends CypherFunSuite {

  implicit val idGen: SequentialIdGen = new SequentialIdGen

  test("build leaf") {
    // given
    val treeBuilder = new TestTreeBuilder
    val logicalPlan = leaf("a")

    // when
    treeBuilder.build(logicalPlan)

    // then
    treeBuilder.callbacks shouldBe Seq(
      OnLeaf("a", Some("initialArgument"))
    )
  }

  test("build link") {
    // given
    val treeBuilder = new TestTreeBuilder
    val logicalPlan = link("a", leaf("b"))

    // when
    treeBuilder.build(logicalPlan)

    // then
    treeBuilder.callbacks shouldBe Seq(
      OnLeaf("b", Some("initialArgument")),
      OnOneChildPlan("a", "B", Some("initialArgument"))
    )
  }

  test("build branch") {
    // given
    val treeBuilder = new TestTreeBuilder
    val logicalPlan = branch("a", leaf("b"), leaf("c"))

    // when
    treeBuilder.build(logicalPlan)

    // then
    treeBuilder.callbacks shouldBe Seq(
      OnLeaf("b", Some("initialArgument")),
      OnTwoChildPlanComingFromLeft("a", "B", Some("initialArgument")),
      OnLeaf("c", Some("argA")),
      OnTwoChildPlanComingFromRight("a", "B", "C", Some("argA")),
    )
  }

  test("build complex") {
    // given
    val treeBuilder = new TestTreeBuilder
    val logicalPlan =
      branch("a",
        branch("b",
          link("d", leaf("h")),
          leaf("e")),
        branch("c",
          leaf("f"),
          leaf("g")))


    // when
    treeBuilder.build(logicalPlan)

    // then
    treeBuilder.callbacks shouldBe Seq(
      OnLeaf("h", Some("initialArgument")),
      OnOneChildPlan("d", "H", Some("initialArgument")),
      OnTwoChildPlanComingFromLeft("b", "D", Some("initialArgument")),
      OnLeaf("e", Some("argB")),
      OnTwoChildPlanComingFromRight("b", "D", "E", Some("argB")),
      OnTwoChildPlanComingFromLeft("a", "B", Some("initialArgument")),
      OnLeaf("f", Some("argA")),
      OnTwoChildPlanComingFromLeft("c", "F", Some("argA")),
      OnLeaf("g", Some("argC")),
      OnTwoChildPlanComingFromRight("c", "F", "G", Some("argC")),
      OnTwoChildPlanComingFromRight("a", "B", "C", Some("argA"))
    )
  }

  test("fail gracefully on invalid plans") {
    // given
    val treeBuilder = new TestTreeBuilder
    val logicalPlan =
      branch("a",
        branch("b",
          link("d", leaf("h")),
          leaf("e")),
        branch("INVALID",
          leaf("f"),
          leaf("g")))


    // when
    a [GracefulError] should be thrownBy treeBuilder.build(logicalPlan)
  }

  class TestTreeBuilder extends TreeBuilder[String, Option[String]] {

    val callbacks = new ArrayBuffer[CallBack]

    override protected def initialArgument(leftLeaf: LogicalPlan): Option[String] = Some("initialArgument")

    override protected def onLeaf(plan: LogicalPlan, argument: Option[String]): String = {
      val str = plan.asInstanceOf[StringPlan].str
      callbacks += OnLeaf(str, argument)
      str.toUpperCase
    }

    override protected def onOneChildPlan(plan: LogicalPlan, source: String, argument: Option[String]): String = {
      val str = plan.asInstanceOf[StringPlan].str
      callbacks += OnOneChildPlan(str, source, argument)
      str.toUpperCase
    }

    override protected def onTwoChildPlanComingFromLeft(plan: LogicalPlan,
                                                        lhs: String,
                                                        argument: Option[String]): Option[String] = {
      val str = plan.asInstanceOf[StringPlan].str
      callbacks += OnTwoChildPlanComingFromLeft(str, lhs, argument)
      Some("arg"+str.toUpperCase)
    }

    override protected def onTwoChildPlanComingFromRight(plan: LogicalPlan,
                                                         lhs: String,
                                                         rhs: String,
                                                         argument: Option[String]): String = {
      val str = plan.asInstanceOf[StringPlan].str
      callbacks += OnTwoChildPlanComingFromRight(str, lhs, rhs, argument)
      str.toUpperCase
    }

    override protected def validatePlan(plan: LogicalPlan): Unit = plan match {
      case StringPlan("INVALID", _, _ ) => throw new GracefulError
      case _ => //do nothing
    }
  }

  def leaf(str: String) = StringPlan(str, None, None)
  def link(str: String, child: StringPlan) = StringPlan(str, Some(child), None)
  def branch(str: String, lhs: StringPlan, rhs: StringPlan) = StringPlan(str, Some(lhs), Some(rhs))

  case class StringPlan(str: String,
                        override val lhs: Option[StringPlan],
                        override val rhs: Option[StringPlan]) extends LogicalPlan(idGen) {

    override def availableSymbols: Set[String] = ???
  }

  sealed trait CallBack
  case class OnLeaf(str: String, arg: Option[String]) extends CallBack
  case class OnOneChildPlan(str: String, source: String, arg: Option[String]) extends CallBack
  case class OnTwoChildPlanComingFromLeft(str: String, lhs: String, arg: Option[String]) extends CallBack
  case class OnTwoChildPlanComingFromRight(str: String, lhs: String, rhs: String, arg: Option[String]) extends CallBack

  class GracefulError extends RuntimeException
}
