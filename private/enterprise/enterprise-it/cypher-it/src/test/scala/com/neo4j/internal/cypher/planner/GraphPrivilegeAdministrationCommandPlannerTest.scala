/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.plandescription.Arguments.Qualifier

class GraphPrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Traverse

  test("Grant traverse") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN GRANT TRAVERSE ON GRAPH * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantTraverse", Qualifier("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("GrantTraverse", Qualifier("NODES *"), "editor",
            graphPrivilegePlan("GrantTraverse", Qualifier("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("GrantTraverse", Qualifier("NODES *"), "reader",
                assertDbmsAdminPlan("GRANT PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny traverse") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY TRAVERSE ON GRAPH $SYSTEM_DATABASE_NAME NODE A TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyTraverse", SYSTEM_DATABASE_NAME, qualifierArg("NODE", "A"), "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke traverse") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE TRAVERSE ON GRAPH $DEFAULT_DATABASE_NAME RELATIONSHIPS A, B FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeTraverse(DENIED)", DEFAULT_DATABASE_NAME, qualifierArg("RELATIONSHIP", "B"), "reader",
          graphPrivilegePlan("RevokeTraverse(GRANTED)", DEFAULT_DATABASE_NAME, qualifierArg("RELATIONSHIP", "B"), "reader",
            graphPrivilegePlan("RevokeTraverse(DENIED)", DEFAULT_DATABASE_NAME, qualifierArg("RELATIONSHIP", "A"), "reader",
              graphPrivilegePlan("RevokeTraverse(GRANTED)", DEFAULT_DATABASE_NAME, qualifierArg("RELATIONSHIP", "A"), "reader",
                assertDbmsAdminPlan("REVOKE PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  // Read

  test("Grant read") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN GRANT READ {*} ON GRAPH $DEFAULT_DATABASE_NAME TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantRead", DEFAULT_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("GrantRead", DEFAULT_DATABASE_NAME, Qualifier("NODES *"), "editor",
            graphPrivilegePlan("GrantRead", DEFAULT_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("GrantRead", DEFAULT_DATABASE_NAME, Qualifier("NODES *"), "reader",
                assertDbmsAdminPlan("GRANT PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny read") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY READ {foo, prop} ON GRAPH * TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyRead", Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("DenyRead", Qualifier("RELATIONSHIPS *"), "reader",
            graphPrivilegePlan("DenyRead", Qualifier("NODES *"), "reader",
              graphPrivilegePlan("DenyRead", Qualifier("NODES *"), "reader",
                assertDbmsAdminPlan("DENY PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Revoke read") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE READ {prop} ON GRAPH $DEFAULT_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeRead(DENIED)", DEFAULT_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("RevokeRead(GRANTED)", DEFAULT_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
            graphPrivilegePlan("RevokeRead(DENIED)", DEFAULT_DATABASE_NAME, Qualifier("NODES *"), "reader",
              graphPrivilegePlan("RevokeRead(GRANTED)", DEFAULT_DATABASE_NAME, Qualifier("NODES *"), "reader",
                assertDbmsAdminPlan("REVOKE PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  // Match

  test("Grant match") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN GRANT MATCH {*} ON GRAPH $DEFAULT_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantRead", DEFAULT_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("GrantRead", DEFAULT_DATABASE_NAME, Qualifier("NODES *"), "reader",
            graphPrivilegePlan("GrantTraverse", DEFAULT_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("GrantTraverse", DEFAULT_DATABASE_NAME, Qualifier("NODES *"), "reader",
                assertDbmsAdminPlan("GRANT PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny match all") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY MATCH {*} ON GRAPH $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyRead", SYSTEM_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("DenyRead", SYSTEM_DATABASE_NAME, Qualifier("NODES *"), "reader",
            graphPrivilegePlan("DenyTraverse", SYSTEM_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("DenyTraverse", SYSTEM_DATABASE_NAME, Qualifier("NODES *"), "reader",
                assertDbmsAdminPlan("DENY PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny match prop") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY MATCH {prop} ON GRAPH * TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyRead", Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("DenyRead", Qualifier("NODES *"), "reader",
            assertDbmsAdminPlan("DENY PRIVILEGE")
          )
        )
      ).toString
    )
  }

  // Write

  test("Grant write") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT WRITE ON GRAPH * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("GrantWrite", Qualifier("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("GrantWrite", Qualifier("NODES *"), "editor",
            graphPrivilegePlan("GrantWrite", Qualifier("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("GrantWrite", Qualifier("NODES *"), "reader",
                assertDbmsAdminPlan("GRANT PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny write") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY WRITE ON GRAPH $SYSTEM_DATABASE_NAME TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "editor",
          graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Qualifier("NODES *"), "editor",
            graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
              graphPrivilegePlan("DenyWrite", SYSTEM_DATABASE_NAME, Qualifier("NODES *"), "reader",
                assertDbmsAdminPlan("DENY PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Revoke grant write") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT WRITE ON GRAPH $DEFAULT_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeWrite(GRANTED)", DEFAULT_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("RevokeWrite(GRANTED)", DEFAULT_DATABASE_NAME, Qualifier("NODES *"), "reader",
            assertDbmsAdminPlan("REVOKE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke deny write") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY WRITE ON GRAPH $DEFAULT_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        graphPrivilegePlan("RevokeWrite(DENIED)", DEFAULT_DATABASE_NAME, Qualifier("RELATIONSHIPS *"), "reader",
          graphPrivilegePlan("RevokeWrite(DENIED)", DEFAULT_DATABASE_NAME, Qualifier("NODES *"), "reader",
            assertDbmsAdminPlan("REVOKE PRIVILEGE")
          )
        )
      ).toString
    )
  }
}
