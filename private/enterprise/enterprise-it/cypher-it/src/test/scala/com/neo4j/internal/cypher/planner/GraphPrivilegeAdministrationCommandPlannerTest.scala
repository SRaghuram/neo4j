/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.asPrettyString

class GraphPrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Traverse

  test("Grant traverse") {
    // When
    val plan = execute(s"EXPLAIN GRANT TRAVERSE ON GRAPH * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlanForAllGraphs("GrantTraverse", details("RELATIONSHIPS *"), "editor",
          graphPrivilegePlanForAllGraphs("GrantTraverse", details("NODES *"), "editor",
            graphPrivilegePlanForAllGraphs("GrantTraverse", details("RELATIONSHIPS *"), "reader",
              graphPrivilegePlanForAllGraphs("GrantTraverse", details("NODES *"), "reader",
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
        graphPrivilegePlan("GrantTraverse", details("GRAPH $db"), details("RELATIONSHIPS *"), "$role2",
          graphPrivilegePlan("GrantTraverse", details("GRAPH $db"), details("NODES *"), "$role2",
            graphPrivilegePlan("GrantTraverse", details("GRAPH $db"), details("RELATIONSHIPS *"), "$role1",
              graphPrivilegePlan("GrantTraverse", details("GRAPH $db"), details("NODES *"), "$role1",
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
        graphPrivilegePlan("RevokeTraverse(DENIED)", details("GRAPH $db2"), qualifierArg("RELATIONSHIP", "B"), "reader",
          graphPrivilegePlan("RevokeTraverse(GRANTED)", details("GRAPH $db2"), qualifierArg("RELATIONSHIP", "B"), "reader",
            graphPrivilegePlan("RevokeTraverse(DENIED)", details("GRAPH $db2"), qualifierArg("RELATIONSHIP", "A"), "reader",
              graphPrivilegePlan("RevokeTraverse(GRANTED)", details("GRAPH $db2"), qualifierArg("RELATIONSHIP", "A"), "reader",
                graphPrivilegePlan("RevokeTraverse(DENIED)", details("GRAPH $db1"), qualifierArg("RELATIONSHIP", "B"), "reader",
                  graphPrivilegePlan("RevokeTraverse(GRANTED)", details("GRAPH $db1"), qualifierArg("RELATIONSHIP", "B"), "reader",
                    graphPrivilegePlan("RevokeTraverse(DENIED)", details("GRAPH $db1"), qualifierArg("RELATIONSHIP", "A"), "reader",
                      graphPrivilegePlan("RevokeTraverse(GRANTED)", details("GRAPH $db1"), qualifierArg("RELATIONSHIP", "A"), "reader",
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
        graphPrivilegePlan("GrantRead", details("GRAPH bar"), allResourceArg(), details("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("GrantRead", details("GRAPH bar"), allResourceArg(), details("NODES *"), "editor",
            graphPrivilegePlan("GrantRead", details("GRAPH bar"), allResourceArg(), details("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("GrantRead", details("GRAPH bar"), allResourceArg(), details("NODES *"), "reader",
                graphPrivilegePlan("GrantRead", details("GRAPH foo"), allResourceArg(), details("RELATIONSHIPS *"), "editor",
                  graphPrivilegePlan("GrantRead", details("GRAPH foo"), allResourceArg(), details("NODES *"), "editor",
                    graphPrivilegePlan("GrantRead", details("GRAPH foo"), allResourceArg(), details("RELATIONSHIPS *"), "reader",
                      graphPrivilegePlan("GrantRead", details("GRAPH foo"), allResourceArg(), details("NODES *"), "reader",
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
        graphPrivilegePlanForAllGraphs("DenyRead", resourceArg("prop"), details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlanForAllGraphs("DenyRead", resourceArg("foo"), details("RELATIONSHIPS *"), "reader",
            graphPrivilegePlanForAllGraphs("DenyRead", resourceArg("prop"), details("NODES *"), "reader",
              graphPrivilegePlanForAllGraphs("DenyRead", resourceArg("foo"), details("NODES *"), "reader",
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
        graphPrivilegePlan("DenyRead", details("GRAPH $db"), resourceArg("prop"), details("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("DenyRead", details("GRAPH $db"), resourceArg("foo"), details("RELATIONSHIPS *"), "$role",
            graphPrivilegePlan("DenyRead", details("GRAPH $db"), resourceArg("prop"), details("NODES *"), "$role",
              graphPrivilegePlan("DenyRead", details("GRAPH $db"), resourceArg("foo"), details("NODES *"), "$role",
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
        graphPrivilegePlan("GrantMatch", DEFAULT_DATABASE_NAME, allResourceArg(), details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("GrantMatch", DEFAULT_DATABASE_NAME, allResourceArg(), details("NODES *"), "reader",
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
        graphPrivilegePlan("DenyMatch", SYSTEM_DATABASE_NAME, allResourceArg(), details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("DenyMatch", SYSTEM_DATABASE_NAME, allResourceArg(), details("NODES *"), "reader",
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
        graphPrivilegePlanForAllGraphs("DenyMatch", resourceArg("prop"), details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlanForAllGraphs("DenyMatch", resourceArg("prop"), details("NODES *"), "reader",
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
        graphPrivilegePlan("DenyMatch", details("GRAPH $db"), resourceArg("prop"), details("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("DenyMatch", details("GRAPH $db"), resourceArg("prop"), details("NODES *"), "$role",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny match prop with parameter and multiple graphs") {
    // When
    val plan = execute("EXPLAIN DENY MATCH {prop} ON GRAPH $db1, $db2 TO $role", Map("db" -> DEFAULT_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyMatch", details("GRAPH $db2"), resourceArg("prop"), details("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("DenyMatch", details("GRAPH $db2"), resourceArg("prop"), details("NODES *"), "$role",
            graphPrivilegePlan("DenyMatch", details("GRAPH $db1"), resourceArg("prop"), details("RELATIONSHIPS *"), "$role",
              graphPrivilegePlan("DenyMatch", details("GRAPH $db1"), resourceArg("prop"), details("NODES *"), "$role",
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
        graphPrivilegePlanForAllGraphs("GrantWrite", details("RELATIONSHIPS *"), "editor",
          graphPrivilegePlanForAllGraphs("GrantWrite", details("NODES *"), "editor",
            graphPrivilegePlanForAllGraphs("GrantWrite", details("RELATIONSHIPS *"), "reader",
              graphPrivilegePlanForAllGraphs("GrantWrite", details("NODES *"), "reader",
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
        graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, details("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, details("NODES *"), "editor",
            graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, details("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, details("NODES *"), "reader",
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
        graphPrivilegePlan("RevokeWrite(GRANTED)", DEFAULT_DATABASE_NAME, details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("RevokeWrite(GRANTED)", DEFAULT_DATABASE_NAME, details("NODES *"), "reader",
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
        graphPrivilegePlan("RevokeWrite(DENIED)", details("GRAPH bar"), details("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("RevokeWrite(DENIED)", details("GRAPH bar"), details("NODES *"), "reader",
            graphPrivilegePlan("RevokeWrite(DENIED)", details("GRAPH $foo"), details("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("RevokeWrite(DENIED)", details("GRAPH $foo"), details("NODES *"), "reader",
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
        graphPrivilegePlan("RevokeWrite(DENIED)", details("GRAPH $db"), details("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("RevokeWrite(GRANTED)", details("GRAPH $db"), details("RELATIONSHIPS *"), "$role",
            graphPrivilegePlan("RevokeWrite(DENIED)", details("GRAPH $db"), details("NODES *"), "$role",
              graphPrivilegePlan("RevokeWrite(GRANTED)", details("GRAPH $db"), details("NODES *"), "$role",
                assertDbmsAdminPlan("REMOVE PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Grant set label") {
    // When
    val plan = execute("EXPLAIN GRANT SET LABEL foo ON GRAPH * TO role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantSetLabel", details("ALL GRAPHS"), details("LABEL foo"), details("NODES *"), "role",
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
        graphPrivilegePlan("DenySetLabel", details("ALL GRAPHS"), details("LABEL bar"), details("NODES *"), "role",
          graphPrivilegePlan("DenySetLabel", details("ALL GRAPHS"), details("LABEL foo"), details("NODES *"), "role",
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
        graphPrivilegePlan("RevokeRemoveLabel(DENIED)", details("ALL GRAPHS"), details("ALL LABELS"), details("NODES *"), "role",
          graphPrivilegePlan("RevokeRemoveLabel(GRANTED)", details("ALL GRAPHS"), details("ALL LABELS"), details("NODES *"), "role",
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
        graphPrivilegePlanForAllGraphs("GrantCreateElement", details("RELATIONSHIPS *"), "role",
          graphPrivilegePlanForAllGraphs("GrantCreateElement", details("NODES *"), "role",
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
        graphPrivilegePlanForAllGraphs("DenyCreateElement", details("NODE bar"), "role",
          graphPrivilegePlanForAllGraphs("DenyCreateElement", details("NODE foo"), "role",
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
        graphPrivilegePlanForAllGraphs("RevokeDeleteElement(DENIED)", details("RELATIONSHIPS *"), "role",
          graphPrivilegePlanForAllGraphs("RevokeDeleteElement(GRANTED)", details("RELATIONSHIPS *"), "role",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant set property") {
    // When
    val plan = execute("EXPLAIN GRANT SET PROPERTY {*} ON GRAPH * TO role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlanForAllGraphs("GrantSetProperty", allResourceArg(), details("RELATIONSHIPS *"), "role",
          graphPrivilegePlanForAllGraphs("GrantSetProperty", allResourceArg(), details("NODES *"), "role",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny set property") {
    // When
    val plan = execute("EXPLAIN DENY SET PROPERTY {prop} ON GRAPH foo NODES bar, baz TO role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenySetProperty", "foo", resourceArg("prop"), details("NODE baz"), "role",
          graphPrivilegePlan("DenySetProperty", "foo", resourceArg("prop"), details("NODE bar"), "role",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke set property") {
    // When
    val plan = execute("EXPLAIN REVOKE GRANT SET PROPERTY {foo,bar} ON GRAPH * RELATIONSHIP baz FROM role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlanForAllGraphs("RevokeSetProperty(GRANTED)", resourceArg("bar"), details("RELATIONSHIP baz"), "role",
          graphPrivilegePlanForAllGraphs("RevokeSetProperty(GRANTED)", resourceArg("foo"), details("RELATIONSHIP baz"), "role",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant all graph privileges") {
    // When
    val plan = execute("EXPLAIN GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO role").executionPlanString()

    // Then
    plan should include(
      logPlan(
          graphPrivilegePlanForAllGraphs("GrantAllGraphPrivileges", Details(Seq.empty), "role",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
      ).toString
    )
  }

  test("Deny all graph privileges") {
    // When
    val plan = execute("EXPLAIN DENY ALL GRAPH PRIVILEGES ON GRAPH foo TO role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyAllGraphPrivileges", "foo", Details(Seq.empty), "role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke all graph privileges") {
    // When
    val plan = execute("EXPLAIN REVOKE ALL GRAPH PRIVILEGES ON GRAPHS foo, bar FROM role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeAllGraphPrivileges(DENIED)", "bar", Details(Seq.empty), "role",
          graphPrivilegePlan("RevokeAllGraphPrivileges(GRANTED)", "bar", Details(Seq.empty), "role",
            graphPrivilegePlan("RevokeAllGraphPrivileges(DENIED)", "foo", Details(Seq.empty), "role",
              graphPrivilegePlan("RevokeAllGraphPrivileges(GRANTED)", "foo", Details(Seq.empty), "role",
                assertDbmsAdminPlan("REMOVE PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Grant merge") {
    // When
    val plan = execute("EXPLAIN GRANT MERGE {*} ON GRAPH * TO role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlanForAllGraphs("GrantMerge", allResourceArg(), details("RELATIONSHIPS *"), "role",
          graphPrivilegePlanForAllGraphs("GrantMerge", allResourceArg(), details("NODES *"), "role",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke merge") {
    // When
    val plan = execute("EXPLAIN REVOKE GRANT MERGE {foo,bar} ON GRAPH * RELATIONSHIP baz FROM role").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlanForAllGraphs("RevokeMerge(GRANTED)", resourceArg("bar"), details("RELATIONSHIP baz"), "role",
          graphPrivilegePlanForAllGraphs("RevokeMerge(GRANTED)", resourceArg("foo"), details("RELATIONSHIP baz"), "role",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  private def details(info: String): Details = Details(asPrettyString.raw(info))
}
