/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

case class Planners(planners: Planner*)

object Planners {
  implicit def plannerToPlanners(planner: Planner): Planners = Planners(planner)

  val all = Planners(Cost, Rule)

  def definedBy(preParserArgs: Array[String]): Planners = {
    val planners = all.planners.filter(_.isDefinedBy(preParserArgs))
    if (planners.nonEmpty) Planners(planners: _*) else all
  }

  object Cost extends Planner(Set("COST", "IDP", "PROCEDURE"), "planner=cost")

  object Rule extends Planner(Set("RULE", "PROCEDURE"), "planner=rule")

}

case class Planner(acceptedPlannerNames: Set[String], preparserOption: String) {
  def isDefinedBy(preParserArgs: Array[String]): Boolean = preparserOption.split(" ").forall(preParserArgs.contains(_))
}
