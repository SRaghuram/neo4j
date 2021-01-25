/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

case class Planners(planners: Planner*)

object Planners {
  implicit def plannerToPlanners(planner: Planner): Planners = Planners(planner)

  val all = Planners(Cost)

  def definedBy(preParserArgs: Array[String]): Planners = {
    val planners = all.planners.filter(_.isDefinedBy(preParserArgs))
    if (planners.nonEmpty) Planners(planners: _*) else all
  }

  object Cost extends Planner(Set("COST", "IDP", "ADMINISTRATION"), "planner=cost")

}

case class Planner(acceptedPlannerNames: Set[String], preparserOption: String) {
  def isDefinedBy(preParserArgs: Array[String]): Boolean = preparserOption.split(" ").forall(preParserArgs.contains(_))
}
