/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.plandescription.Arguments.Details

class GraphPrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Traverse

  test("Grant traverse") {
    // When
    val plan = execute(s"EXPLAIN GRANT TRAVERSE ON GRAPH * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantTraverse", Details("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("GrantTraverse", Details("NODES *"), "editor",
            graphPrivilegePlan("GrantTraverse", Details("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("GrantTraverse", Details("NODES *"), "reader",
                assertDbmsAdminPlan("ASSIGN PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Grant traverse with parameter") {
    // When
    val plan = execute("EXPLAIN GRANT TRAVERSE ON GRAPH $db TO $role1, $role2", Map("db" -> DEFAULT_DATABASE_NAME, "role1" -> "reader", "role2" -> "editor")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantTraverse", Details("GRAPH $db"), Details("RELATIONSHIPS *"), "$role2",
          graphPrivilegePlan("GrantTraverse", Details("GRAPH $db"), Details("NODES *"), "$role2",
            graphPrivilegePlan("GrantTraverse", Details("GRAPH $db"), Details("RELATIONSHIPS *"), "$role1",
              graphPrivilegePlan("GrantTraverse", Details("GRAPH $db"), Details("NODES *"), "$role1",
                assertDbmsAdminPlan("ASSIGN PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny traverse") {
    // When
    val plan = execute(s"EXPLAIN DENY TRAVERSE ON GRAPH $SYSTEM_DATABASE_NAME NODE A TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyTraverse", SYSTEM_DATABASE_NAME, qualifierArg("NODE", "A"), "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke traverse") {
    // When
    val plan = execute(s"EXPLAIN REVOKE TRAVERSE ON GRAPH $DEFAULT_DATABASE_NAME RELATIONSHIPS A, B FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeTraverse(DENIED)", DEFAULT_DATABASE_NAME, qualifierArg("RELATIONSHIP", "B"), "reader",
          graphPrivilegePlan("RevokeTraverse(GRANTED)", DEFAULT_DATABASE_NAME, qualifierArg("RELATIONSHIP", "B"), "reader",
            graphPrivilegePlan("RevokeTraverse(DENIED)", DEFAULT_DATABASE_NAME, qualifierArg("RELATIONSHIP", "A"), "reader",
              graphPrivilegePlan("RevokeTraverse(GRANTED)", DEFAULT_DATABASE_NAME, qualifierArg("RELATIONSHIP", "A"), "reader",
                assertDbmsAdminPlan("REMOVE PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Revoke traverse on multiple dbs") {
    // When
    val plan = execute(s"EXPLAIN REVOKE TRAVERSE ON GRAPH $$db1, $$db2 RELATIONSHIPS A, B FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeTraverse(DENIED)", Details("GRAPH $db2"), qualifierArg("RELATIONSHIP", "B"), "reader",
          graphPrivilegePlan("RevokeTraverse(GRANTED)", Details("GRAPH $db2"), qualifierArg("RELATIONSHIP", "B"), "reader",
            graphPrivilegePlan("RevokeTraverse(DENIED)", Details("GRAPH $db2"), qualifierArg("RELATIONSHIP", "A"), "reader",
              graphPrivilegePlan("RevokeTraverse(GRANTED)", Details("GRAPH $db2"), qualifierArg("RELATIONSHIP", "A"), "reader",
                graphPrivilegePlan("RevokeTraverse(DENIED)", Details("GRAPH $db1"), qualifierArg("RELATIONSHIP", "B"), "reader",
                  graphPrivilegePlan("RevokeTraverse(GRANTED)", Details("GRAPH $db1"), qualifierArg("RELATIONSHIP", "B"), "reader",
                    graphPrivilegePlan("RevokeTraverse(DENIED)", Details("GRAPH $db1"), qualifierArg("RELATIONSHIP", "A"), "reader",
                      graphPrivilegePlan("RevokeTraverse(GRANTED)", Details("GRAPH $db1"), qualifierArg("RELATIONSHIP", "A"), "reader",
                        assertDbmsAdminPlan("REMOVE PRIVILEGE")
                      )
                    )
                  )
                )
              )
            )
          )
        )
      ).toString
    )
  }

  // Read

  test("Grant read") {
    // When
    val plan = execute(s"EXPLAIN GRANT READ {*} ON GRAPH foo, bar TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantRead", Details("GRAPH bar"), allResourceArg(), Details("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("GrantRead", Details("GRAPH bar"), allResourceArg(), Details("NODES *"), "editor",
            graphPrivilegePlan("GrantRead", Details("GRAPH bar"), allResourceArg(), Details("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("GrantRead", Details("GRAPH bar"), allResourceArg(), Details("NODES *"), "reader",
                graphPrivilegePlan("GrantRead", Details("GRAPH foo"), allResourceArg(), Details("RELATIONSHIPS *"), "editor",
                  graphPrivilegePlan("GrantRead", Details("GRAPH foo"), allResourceArg(), Details("NODES *"), "editor",
                    graphPrivilegePlan("GrantRead", Details("GRAPH foo"), allResourceArg(), Details("RELATIONSHIPS *"), "reader",
                      graphPrivilegePlan("GrantRead", Details("GRAPH foo"), allResourceArg(), Details("NODES *"), "reader",
                        assertDbmsAdminPlan("ASSIGN PRIVILEGE")
                      )
                    )
                  )
                )
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny read") {
    // When
    val plan = execute(s"EXPLAIN DENY READ {foo, prop} ON GRAPH * TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlanForAllGraphs("DenyRead", resourceArg("prop"), Details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlanForAllGraphs("DenyRead", resourceArg("foo"), Details("RELATIONSHIPS *"), "reader",
            graphPrivilegePlanForAllGraphs("DenyRead", resourceArg("prop"), Details("NODES *"), "reader",
              graphPrivilegePlanForAllGraphs("DenyRead", resourceArg("foo"), Details("NODES *"), "reader",
                assertDbmsAdminPlan("ASSIGN PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny read with parameter") {
    // When
    val plan = execute("EXPLAIN DENY READ {foo, prop} ON GRAPH $db TO $role", Map("db" -> DEFAULT_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyRead", Details("GRAPH $db"), resourceArg("prop"), Details("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("DenyRead", Details("GRAPH $db"), resourceArg("foo"), Details("RELATIONSHIPS *"), "$role",
            graphPrivilegePlan("DenyRead", Details("GRAPH $db"), resourceArg("prop"), Details("NODES *"), "$role",
              graphPrivilegePlan("DenyRead", Details("GRAPH $db"), resourceArg("foo"), Details("NODES *"), "$role",
                assertDbmsAdminPlan("ASSIGN PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Revoke read") {
    // When
    val plan = execute(s"EXPLAIN REVOKE READ {prop} ON GRAPH $DEFAULT_DATABASE_NAME ELEMENTS A FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeRead(DENIED)", DEFAULT_DATABASE_NAME, resourceArg("prop"), qualifierArg("RELATIONSHIP", "A"), "reader",
          graphPrivilegePlan("RevokeRead(GRANTED)", DEFAULT_DATABASE_NAME, resourceArg("prop"), qualifierArg("RELATIONSHIP", "A"), "reader",
            graphPrivilegePlan("RevokeRead(DENIED)", DEFAULT_DATABASE_NAME, resourceArg("prop"), qualifierArg("NODE", "A"), "reader",
              graphPrivilegePlan("RevokeRead(GRANTED)", DEFAULT_DATABASE_NAME, resourceArg("prop"), qualifierArg("NODE", "A"), "reader",
                assertDbmsAdminPlan("REMOVE PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  // Match

  test("Grant match") {
    // When
    val plan = execute(s"EXPLAIN GRANT MATCH {*} ON GRAPH $DEFAULT_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantMatch", DEFAULT_DATABASE_NAME, allResourceArg(), Details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("GrantMatch", DEFAULT_DATABASE_NAME, allResourceArg(), Details("NODES *"), "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny match all") {
    // When
    val plan = execute(s"EXPLAIN DENY MATCH {*} ON GRAPH $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyMatch", SYSTEM_DATABASE_NAME, allResourceArg(), Details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("DenyMatch", SYSTEM_DATABASE_NAME, allResourceArg(), Details("NODES *"), "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny match prop") {
    // When
    val plan = execute(s"EXPLAIN DENY MATCH {prop} ON GRAPH * TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlanForAllGraphs("DenyMatch", resourceArg("prop"), Details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlanForAllGraphs("DenyMatch", resourceArg("prop"), Details("NODES *"), "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny match prop with parameter") {
    // When
    val plan = execute("EXPLAIN DENY MATCH {prop} ON GRAPH $db TO $role", Map("db" -> DEFAULT_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyMatch", Details("GRAPH $db"), resourceArg("prop"), Details("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("DenyMatch", Details("GRAPH $db"), resourceArg("prop"), Details("NODES *"), "$role",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny match prop with parameter and multiple databases") {
    // When
    val plan = execute("EXPLAIN DENY MATCH {prop} ON GRAPH $db1, $db2 TO $role", Map("db" -> DEFAULT_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyMatch", Details("GRAPH $db2"), resourceArg("prop"), Details("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("DenyMatch", Details("GRAPH $db2"), resourceArg("prop"), Details("NODES *"), "$role",
            graphPrivilegePlan("DenyMatch", Details("GRAPH $db1"), resourceArg("prop"), Details("RELATIONSHIPS *"), "$role",
              graphPrivilegePlan("DenyMatch", Details("GRAPH $db1"), resourceArg("prop"), Details("NODES *"), "$role",
                assertDbmsAdminPlan("ASSIGN PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Revoke match") {
    // When
    val plan = execute(s"EXPLAIN REVOKE MATCH {prop} ON GRAPH $DEFAULT_DATABASE_NAME ELEMENTS A FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeMatch(DENIED)", DEFAULT_DATABASE_NAME, resourceArg("prop"), qualifierArg("RELATIONSHIP", "A"), "reader",
          graphPrivilegePlan("RevokeMatch(GRANTED)", DEFAULT_DATABASE_NAME, resourceArg("prop"), qualifierArg("RELATIONSHIP", "A"), "reader",
            graphPrivilegePlan("RevokeMatch(DENIED)", DEFAULT_DATABASE_NAME, resourceArg("prop"), qualifierArg("NODE", "A"), "reader",
              graphPrivilegePlan("RevokeMatch(GRANTED)", DEFAULT_DATABASE_NAME, resourceArg("prop"), qualifierArg("NODE", "A"), "reader",
                assertDbmsAdminPlan("REMOVE PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  // Write

  test("Grant write") {
    // When
    val plan = execute("EXPLAIN GRANT WRITE ON GRAPH * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantWrite", Details("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("GrantWrite", Details("NODES *"), "editor",
            graphPrivilegePlan("GrantWrite", Details("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("GrantWrite", Details("NODES *"), "reader",
                assertDbmsAdminPlan("ASSIGN PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny write") {
    // When
    val plan = execute(s"EXPLAIN DENY WRITE ON GRAPH $SYSTEM_DATABASE_NAME TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Details("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Details("NODES *"), "editor",
            graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Details("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Details("NODES *"), "reader",
                assertDbmsAdminPlan("ASSIGN PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Revoke grant write") {
    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT WRITE ON GRAPH $DEFAULT_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeWrite(GRANTED)", DEFAULT_DATABASE_NAME, Details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("RevokeWrite(GRANTED)", DEFAULT_DATABASE_NAME, Details("NODES *"), "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke deny write") {
    // When
    val plan = execute(s"EXPLAIN REVOKE DENY WRITE ON GRAPH $$foo, bar FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeWrite(DENIED)", Details("GRAPH bar"), Details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("RevokeWrite(DENIED)", Details("GRAPH bar"), Details("NODES *"), "reader",
            graphPrivilegePlan("RevokeWrite(DENIED)", Details("GRAPH $foo"), Details("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("RevokeWrite(DENIED)", Details("GRAPH $foo"), Details("NODES *"), "reader",
                assertDbmsAdminPlan("REMOVE PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Revoke write with parameter") {
    // When
    val plan = execute("EXPLAIN REVOKE WRITE ON GRAPH $db FROM $role", Map("db" -> DEFAULT_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeWrite(DENIED)", Details("GRAPH $db"), Details("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("RevokeWrite(GRANTED)", Details("GRAPH $db"), Details("RELATIONSHIPS *"), "$role",
            graphPrivilegePlan("RevokeWrite(DENIED)", Details("GRAPH $db"), Details("NODES *"), "$role",
              graphPrivilegePlan("RevokeWrite(GRANTED)", Details("GRAPH $db"), Details("NODES *"), "$role",
                assertDbmsAdminPlan("REMOVE PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  // Set label

  test("Grant set label") {
    // When
    val plan = execute("EXPLAIN GRANT SET LABEL foo ON GRAPH * TO role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantSetLabel", Details("ALL GRAPHS"), Details("LABEL foo"), Details("NODES *"), "role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny set label") {
    // When
    val plan = execute("EXPLAIN DENY SET LABEL foo, bar ON GRAPH * TO role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenySetLabel", Details("ALL GRAPHS"), Details(Seq("LABEL bar")), Details("NODES *"), "role",
          graphPrivilegePlan("DenySetLabel", Details("ALL GRAPHS"), Details(Seq("LABEL foo")), Details("NODES *"), "role",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke remove label") {
    // When
    val plan = execute("EXPLAIN REVOKE REMOVE LABEL * ON GRAPH * FROM role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeRemoveLabel(DENIED)", Details("ALL GRAPHS"), Details(Seq("ALL LABELS")), Details("NODES *"), "role",
          graphPrivilegePlan("RevokeRemoveLabel(GRANTED)", Details("ALL GRAPHS"), Details(Seq("ALL LABELS")), Details("NODES *"), "role",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant create element") {
    // When
    val plan = execute("EXPLAIN GRANT CREATE ON GRAPH * TO role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantCreateElement", Details("RELATIONSHIPS *"), "role",
          graphPrivilegePlan("GrantCreateElement", Details("NODES *"), "role",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create element") {
    // When
    val plan = execute("EXPLAIN DENY CREATE ON GRAPH * NODES foo, bar TO role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyCreateElement", Details("NODE bar"), "role",
          graphPrivilegePlan("DenyCreateElement", Details("NODE foo"), "role",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke delete element") {
    // When
    val plan = execute("EXPLAIN REVOKE DELETE ON GRAPH * RELATIONSHIP * FROM role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeDeleteElement(DENIED)", Details("RELATIONSHIPS *"), "role",
          graphPrivilegePlan("RevokeDeleteElement(GRANTED)", Details("RELATIONSHIPS *"), "role",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }
}
