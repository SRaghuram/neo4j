/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import com.neo4j.cypher.EnterpriseGraphDatabaseTestSupport
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.planner.spi.DPPlannerName
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.exceptions.InvalidArgumentException
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

  test("runtime=pipelined is default") {
    val query = "RETURN 1"
    execute(query).executionPlanDescription() should haveRuntime("PIPELINED")
  }

  test("should fallback if pipelined doesn't support query") {
    val query = "MERGE (n:L)"
    execute(query).executionPlanDescription() shouldNot haveRuntime("PIPELINED")
  }

  test("should handle interpretedPipesFallback=all") {
    val query1 = "CYPHER runtime=pipelined interpretedPipesFallback=all MATCH (n:L) DELETE n RETURN 1"
    val query2 = "CYPHER runtime=pipelined MATCH (n:L) DELETE n RETURN 1"

    execute(query1).executionPlanDescription() should haveRuntime("PIPELINED")
    execute(query2).executionPlanDescription() shouldNot haveRuntime("PIPELINED")
  }

  test("should allow known debug options") {
    val query = "CYPHER debug=tostring RETURN 1"
    execute(query)
  }

  test("should fail on unknown debug options") {
    val query = "CYPHER debug=iamunknown RETURN 1"
    the[InvalidArgumentException].thrownBy(execute(query)).getMessage
                                 .should(include("iamunknown"))
  }

  for (runtime <- Seq("interpreted", "slotted", "pipelined", "parallel")) {
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
