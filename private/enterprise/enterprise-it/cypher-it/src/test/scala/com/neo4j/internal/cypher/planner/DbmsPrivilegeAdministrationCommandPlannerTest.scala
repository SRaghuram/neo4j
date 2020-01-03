/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.plandescription.Arguments.DbmsAction

class DbmsPrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Role privileges

  test("Grant show role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT SHOW ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("GrantDbmsAction", "SHOW ROLE", "editor",
          rolePrivilegePlan("GrantDbmsAction", "SHOW ROLE", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny show role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY SHOW ROLE ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("DenyDbmsAction", "SHOW ROLE", "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke show role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE SHOW ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("RevokeDbmsAction(DENIED)", "SHOW ROLE", "reader",
          rolePrivilegePlan("RevokeDbmsAction(GRANTED)", "SHOW ROLE", "reader",
            helperPlan("AssertValidRevoke", Seq(DbmsAction("SHOW ROLE"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Grant create role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("GrantDbmsAction", "CREATE ROLE", "editor",
          rolePrivilegePlan("GrantDbmsAction", "CREATE ROLE", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CREATE ROLE ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("DenyDbmsAction", "CREATE ROLE", "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke create role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE CREATE ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("RevokeDbmsAction(DENIED)", "CREATE ROLE", "reader",
          rolePrivilegePlan("RevokeDbmsAction(GRANTED)", "CREATE ROLE", "reader",
            helperPlan("AssertValidRevoke", Seq(DbmsAction("CREATE ROLE"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Grant drop role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("GrantDbmsAction", "DROP ROLE", "editor",
          rolePrivilegePlan("GrantDbmsAction", "DROP ROLE", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny drop role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY DROP ROLE ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("DenyDbmsAction", "DROP ROLE", "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke drop role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE DROP ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("RevokeDbmsAction(DENIED)", "DROP ROLE", "reader",
          rolePrivilegePlan("RevokeDbmsAction(GRANTED)", "DROP ROLE", "reader",
            helperPlan("AssertValidRevoke", Seq(DbmsAction("DROP ROLE"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Grant assign role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ASSIGN ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("GrantDbmsAction", "ASSIGN ROLE", "editor",
          rolePrivilegePlan("GrantDbmsAction", "ASSIGN ROLE", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny assign role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ASSIGN ROLE ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("DenyDbmsAction", "ASSIGN ROLE", "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke assign role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE ASSIGN ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("RevokeDbmsAction(DENIED)", "ASSIGN ROLE", "reader",
          rolePrivilegePlan("RevokeDbmsAction(GRANTED)", "ASSIGN ROLE", "reader",
            helperPlan("AssertValidRevoke", Seq(DbmsAction("ASSIGN ROLE"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Grant remove role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT REMOVE ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("GrantDbmsAction", "REMOVE ROLE", "editor",
          rolePrivilegePlan("GrantDbmsAction", "REMOVE ROLE", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny remove role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY REMOVE ROLE ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("DenyDbmsAction", "REMOVE ROLE", "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke remove role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE REMOVE ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("RevokeDbmsAction(DENIED)", "REMOVE ROLE", "reader",
          rolePrivilegePlan("RevokeDbmsAction(GRANTED)", "REMOVE ROLE", "reader",
            helperPlan("AssertValidRevoke", Seq(DbmsAction("REMOVE ROLE"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Grant role management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ROLE MANAGEMENT ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("GrantDbmsAction", "ROLE MANAGEMENT", "editor",
          rolePrivilegePlan("GrantDbmsAction", "ROLE MANAGEMENT", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny role management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ROLE MANAGEMENT ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("DenyDbmsAction", "ROLE MANAGEMENT", "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke role management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE ROLE MANAGEMENT ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        rolePrivilegePlan("RevokeDbmsAction(DENIED)", "SHOW ROLE", "reader",
          rolePrivilegePlan("RevokeDbmsAction(GRANTED)", "SHOW ROLE", "reader",
            rolePrivilegePlan("RevokeDbmsAction(DENIED)", "REMOVE ROLE", "reader",
              rolePrivilegePlan("RevokeDbmsAction(GRANTED)", "REMOVE ROLE", "reader",
                rolePrivilegePlan("RevokeDbmsAction(DENIED)", "ASSIGN ROLE", "reader",
                  rolePrivilegePlan("RevokeDbmsAction(GRANTED)", "ASSIGN ROLE", "reader",
                    rolePrivilegePlan("RevokeDbmsAction(DENIED)", "DROP ROLE", "reader",
                      rolePrivilegePlan("RevokeDbmsAction(GRANTED)", "DROP ROLE", "reader",
                        rolePrivilegePlan("RevokeDbmsAction(DENIED)", "CREATE ROLE", "reader",
                          rolePrivilegePlan("RevokeDbmsAction(GRANTED)", "CREATE ROLE", "reader",
                            rolePrivilegePlan("RevokeDbmsAction(DENIED)", "ROLE MANAGEMENT", "reader",
                              rolePrivilegePlan("RevokeDbmsAction(GRANTED)", "ROLE MANAGEMENT", "reader",
                                helperPlan("AssertValidRevoke", Seq(DbmsAction("ROLE MANAGEMENT"), roleArg("reader")),
                                  assertDbmsAdminPlan("REVOKE PRIVILEGE")
                                )
                              )
                            )
                          )
                        )
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
}
