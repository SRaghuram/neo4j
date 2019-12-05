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

  // User privileges

  test("Grant show user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT SHOW USER ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("GrantDbmsAction", "SHOW USER", "editor",
          userPrivilegePlan("GrantDbmsAction", "SHOW USER", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny show user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY SHOW USER ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("DenyDbmsAction", "SHOW USER", "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke show user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE SHOW USER ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("RevokeDbmsAction(DENIED)", "SHOW USER", "reader",
          userPrivilegePlan("RevokeDbmsAction(GRANTED)", "SHOW USER", "reader",
            helperPlan("AssertValidRevoke", Seq(DbmsAction("SHOW USER"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Grant create user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE USER ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("GrantDbmsAction", "CREATE USER", "editor",
          userPrivilegePlan("GrantDbmsAction", "CREATE USER", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CREATE USER ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("DenyDbmsAction", "CREATE USER", "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke create user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE CREATE USER ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("RevokeDbmsAction(DENIED)", "CREATE USER", "reader",
          userPrivilegePlan("RevokeDbmsAction(GRANTED)", "CREATE USER", "reader",
            helperPlan("AssertValidRevoke", Seq(DbmsAction("CREATE USER"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Grant drop user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP USER ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("GrantDbmsAction", "DROP USER", "editor",
          userPrivilegePlan("GrantDbmsAction", "DROP USER", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny drop user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY DROP USER ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("DenyDbmsAction", "DROP USER", "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke drop user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE DROP USER ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("RevokeDbmsAction(DENIED)", "DROP USER", "reader",
          userPrivilegePlan("RevokeDbmsAction(GRANTED)", "DROP USER", "reader",
            helperPlan("AssertValidRevoke", Seq(DbmsAction("DROP USER"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Grant alter user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ALTER USER ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("GrantDbmsAction", "ALTER USER", "editor",
          userPrivilegePlan("GrantDbmsAction", "ALTER USER", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny alter user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ALTER USER ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("DenyDbmsAction", "ALTER USER", "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke alter user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE ALTER USER ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("RevokeDbmsAction(DENIED)", "ALTER USER", "reader",
          userPrivilegePlan("RevokeDbmsAction(GRANTED)", "ALTER USER", "reader",
            helperPlan("AssertValidRevoke", Seq(DbmsAction("ALTER USER"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Grant user management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT USER MANAGEMENT ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("GrantDbmsAction", "USER MANAGEMENT", "editor",
          userPrivilegePlan("GrantDbmsAction", "USER MANAGEMENT", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny user management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY USER MANAGEMENT ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("DenyDbmsAction", "USER MANAGEMENT", "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke user management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE USER MANAGEMENT ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        userPrivilegePlan("RevokeDbmsAction(DENIED)", "SHOW USER", "reader",
          userPrivilegePlan("RevokeDbmsAction(GRANTED)", "SHOW USER", "reader",
            userPrivilegePlan("RevokeDbmsAction(DENIED)", "ALTER USER", "reader",
              userPrivilegePlan("RevokeDbmsAction(GRANTED)", "ALTER USER", "reader",
                userPrivilegePlan("RevokeDbmsAction(DENIED)", "DROP USER", "reader",
                  userPrivilegePlan("RevokeDbmsAction(GRANTED)", "DROP USER", "reader",
                    userPrivilegePlan("RevokeDbmsAction(DENIED)", "CREATE USER", "reader",
                      userPrivilegePlan("RevokeDbmsAction(GRANTED)", "CREATE USER", "reader",
                        userPrivilegePlan("RevokeDbmsAction(DENIED)", "USER MANAGEMENT", "reader",
                          userPrivilegePlan("RevokeDbmsAction(GRANTED)", "USER MANAGEMENT", "reader",
                            helperPlan("AssertValidRevoke", Seq(DbmsAction("USER MANAGEMENT"), roleArg("reader")),
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
      ).toString
    )
  }
}
