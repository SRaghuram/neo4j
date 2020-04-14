/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.plandescription.Arguments.Database
import org.neo4j.cypher.internal.plandescription.Arguments.Qualifier

class GraphPrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Traverse

  test("Grant traverse") {
    // When
    val plan = execute(s"EXPLAIN GRANT TRAVERSE ON GRAPH * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantTraverse", Qualifier("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("GrantTraverse", Qualifier("NODES *"), "editor",
            graphPrivilegePlan("GrantTraverse", Qualifier("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("GrantTraverse", Qualifier("NODES *"), "reader",
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
        graphPrivilegePlan("GrantTraverse", Database("GRAPH $db"), Qualifier("RELATIONSHIPS *"), "$role2",
          graphPrivilegePlan("GrantTraverse", Database("GRAPH $db"), Qualifier("NODES *"), "$role2",
            graphPrivilegePlan("GrantTraverse", Database("GRAPH $db"), Qualifier("RELATIONSHIPS *"), "$role1",
              graphPrivilegePlan("GrantTraverse", Database("GRAPH $db"), Qualifier("NODES *"), "$role1",
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
        graphPrivilegePlan("RevokeTraverse(DENIED)", Database("GRAPH $db2"), qualifierArg("RELATIONSHIP", "B"), "reader",
          graphPrivilegePlan("RevokeTraverse(GRANTED)", Database("GRAPH $db2"), qualifierArg("RELATIONSHIP", "B"), "reader",
            graphPrivilegePlan("RevokeTraverse(DENIED)", Database("GRAPH $db2"), qualifierArg("RELATIONSHIP", "A"), "reader",
              graphPrivilegePlan("RevokeTraverse(GRANTED)", Database("GRAPH $db2"), qualifierArg("RELATIONSHIP", "A"), "reader",
                graphPrivilegePlan("RevokeTraverse(DENIED)", Database("GRAPH $db1"), qualifierArg("RELATIONSHIP", "B"), "reader",
                  graphPrivilegePlan("RevokeTraverse(GRANTED)", Database("GRAPH $db1"), qualifierArg("RELATIONSHIP", "B"), "reader",
                    graphPrivilegePlan("RevokeTraverse(DENIED)", Database("GRAPH $db1"), qualifierArg("RELATIONSHIP", "A"), "reader",
                      graphPrivilegePlan("RevokeTraverse(GRANTED)", Database("GRAPH $db1"), qualifierArg("RELATIONSHIP", "A"), "reader",
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
        graphPrivilegePlan("GrantRead", Database("GRAPH bar"), allResourceArg(), Qualifier("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("GrantRead", Database("GRAPH bar"), allResourceArg(), Qualifier("NODES *"), "editor",
            graphPrivilegePlan("GrantRead", Database("GRAPH bar"), allResourceArg(), Qualifier("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("GrantRead", Database("GRAPH bar"), allResourceArg(), Qualifier("NODES *"), "reader",
                graphPrivilegePlan("GrantRead", Database("GRAPH foo"), allResourceArg(), Qualifier("RELATIONSHIPS *"), "editor",
                  graphPrivilegePlan("GrantRead", Database("GRAPH foo"), allResourceArg(), Qualifier("NODES *"), "editor",
                    graphPrivilegePlan("GrantRead", Database("GRAPH foo"), allResourceArg(), Qualifier("RELATIONSHIPS *"), "reader",
                      graphPrivilegePlan("GrantRead", Database("GRAPH foo"), allResourceArg(), Qualifier("NODES *"), "reader",
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
        graphPrivilegePlan("DenyRead", resourceArg("prop"), Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("DenyRead", resourceArg("foo"), Qualifier("RELATIONSHIPS *"), "reader",
            graphPrivilegePlan("DenyRead", resourceArg("prop"), Qualifier("NODES *"), "reader",
              graphPrivilegePlan("DenyRead", resourceArg("foo"), Qualifier("NODES *"), "reader",
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
        graphPrivilegePlan("DenyRead", Database("GRAPH $db"), resourceArg("prop"), Qualifier("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("DenyRead", Database("GRAPH $db"), resourceArg("foo"), Qualifier("RELATIONSHIPS *"), "$role",
            graphPrivilegePlan("DenyRead", Database("GRAPH $db"), resourceArg("prop"), Qualifier("NODES *"), "$role",
              graphPrivilegePlan("DenyRead", Database("GRAPH $db"), resourceArg("foo"), Qualifier("NODES *"), "$role",
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
        graphPrivilegePlan("GrantMatch", DEFAULT_DATABASE_NAME, allResourceArg(), Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("GrantMatch", DEFAULT_DATABASE_NAME, allResourceArg(), Qualifier("NODES *"), "reader",
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
        graphPrivilegePlan("DenyMatch", SYSTEM_DATABASE_NAME, allResourceArg(), Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("DenyMatch", SYSTEM_DATABASE_NAME, allResourceArg(), Qualifier("NODES *"), "reader",
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
        graphPrivilegePlan("DenyMatch", resourceArg("prop"), Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("DenyMatch", resourceArg("prop"), Qualifier("NODES *"), "reader",
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
        graphPrivilegePlan("DenyMatch", Database("GRAPH $db"), resourceArg("prop"), Qualifier("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("DenyMatch", Database("GRAPH $db"), resourceArg("prop"), Qualifier("NODES *"), "$role",
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
        graphPrivilegePlan("DenyMatch", Database("GRAPH $db2"), resourceArg("prop"), Qualifier("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("DenyMatch", Database("GRAPH $db2"), resourceArg("prop"), Qualifier("NODES *"), "$role",
            graphPrivilegePlan("DenyMatch", Database("GRAPH $db1"), resourceArg("prop"), Qualifier("RELATIONSHIPS *"), "$role",
              graphPrivilegePlan("DenyMatch", Database("GRAPH $db1"), resourceArg("prop"), Qualifier("NODES *"), "$role",
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
        graphPrivilegePlan("GrantWrite", Qualifier("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("GrantWrite", Qualifier("NODES *"), "editor",
            graphPrivilegePlan("GrantWrite", Qualifier("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("GrantWrite", Qualifier("NODES *"), "reader",
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
        graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Qualifier("NODES *"), "editor",
            graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Qualifier("NODES *"), "reader",
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
        graphPrivilegePlan("RevokeWrite(GRANTED)", DEFAULT_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("RevokeWrite(GRANTED)", DEFAULT_DATABASE_NAME, Qualifier("NODES *"), "reader",
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
        graphPrivilegePlan("RevokeWrite(DENIED)", Database("GRAPH bar"), Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("RevokeWrite(DENIED)", Database("GRAPH bar"), Qualifier("NODES *"), "reader",
            graphPrivilegePlan("RevokeWrite(DENIED)", Database("GRAPH $foo"), Qualifier("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("RevokeWrite(DENIED)", Database("GRAPH $foo"), Qualifier("NODES *"), "reader",
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
        graphPrivilegePlan("RevokeWrite(DENIED)", Database("GRAPH $db"), Qualifier("RELATIONSHIPS *"), "$role",
          graphPrivilegePlan("RevokeWrite(GRANTED)", Database("GRAPH $db"), Qualifier("RELATIONSHIPS *"), "$role",
            graphPrivilegePlan("RevokeWrite(DENIED)", Database("GRAPH $db"), Qualifier("NODES *"), "$role",
              graphPrivilegePlan("RevokeWrite(GRANTED)", Database("GRAPH $db"), Qualifier("NODES *"), "$role",
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
        graphPrivilegePlan("GrantSetLabel", Database("ALL GRAPHS"), Qualifier("LABEL foo"), "role",
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
        graphPrivilegePlan("DenySetLabel", Database("ALL GRAPHS"), Qualifier("LABEL bar"), "role",
          graphPrivilegePlan("DenySetLabel", Database("ALL GRAPHS"), Qualifier("LABEL foo"), "role",
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
        graphPrivilegePlan("RevokeRemoveLabel(DENIED)", Database("ALL GRAPHS"), Qualifier("ALL LABELS"), "role",
          graphPrivilegePlan("RevokeRemoveLabel(GRANTED)", Database("ALL GRAPHS"), Qualifier("ALL LABELS"), "role",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }
}
