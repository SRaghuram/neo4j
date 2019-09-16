/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import com.neo4j.cypher.EnterpriseGraphDatabaseTestSupport
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.planner.spi.{DPPlannerName, IDPPlannerName}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.scalatest.matchers.Matcher

class PreParsingAcceptanceTest extends ExecutionEngineFunSuite with EnterpriseGraphDatabaseTestSupport {

  test("should not use eagerness when option not provided ") {
    execute("MATCH () CREATE ()").executionPlanDescription() shouldNot includeSomewhere.aPlan("Eager")
  }

  test("should use eagerness when option is provided ") {
    execute("CYPHER updateStrategy=eager MATCH () CREATE ()").executionPlanDescription() should includeSomewhere.aPlan("Eager")
  }

  test("specifying no planner should provide IDP") {
    val query = "PROFILE RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(IDPPlannerName)
  }

  test("specifying cost planner should provide IDP") {
    val query = "PROFILE CYPHER planner=cost RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(IDPPlannerName)
  }

  test("specifying idp planner should provide IDP") {
    val query = "PROFILE CYPHER planner=idp RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(IDPPlannerName)
  }

  test("specifying dp planner should provide DP") {
    val query = "PROFILE CYPHER planner=dp RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(DPPlannerName)
  }

  test("specifying cost planner should provide IDP using old syntax") {
    val query = "PROFILE CYPHER planner=cost RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(IDPPlannerName)
  }

  test("specifying idp planner should provide IDP using old syntax") {
    val query = "PROFILE CYPHER planner=idp RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(IDPPlannerName)
  }

  test("specifying dp planner should provide DP using old syntax") {
    val query = "PROFILE CYPHER planner=dp RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(DPPlannerName)
  }

  test("runtime=morsel is default") {
    val query = "RETURN 1"
    execute(query).executionPlanDescription() should haveRuntime("MORSEL")
  }

  test("should fallback if morsel doesn't support query") {
    val query = "MATCH (n)-[*]->(m) RETURN n SKIP 1"
    execute(query).executionPlanDescription() should not equal "MORSEL"
  }

  for (runtime <- Seq("interpreted", "slotted", "morsel", "compiled", "parallel")) {

    test(s"runtime=$runtime is selectable") {
      val query = s"CYPHER runtime=$runtime RETURN 1"

      execute(query).executionPlanDescription() should haveRuntime(runtime.toUpperCase())
    }
  }

  private def havePlanner(expected: PlannerName): Matcher[InternalPlanDescription] =
    haveAsRoot.aPlan.containingArgument(expected.name)

  private def haveRuntime(expected: String): Matcher[InternalPlanDescription] =
    haveAsRoot.aPlan.containingArgument(expected)
}
