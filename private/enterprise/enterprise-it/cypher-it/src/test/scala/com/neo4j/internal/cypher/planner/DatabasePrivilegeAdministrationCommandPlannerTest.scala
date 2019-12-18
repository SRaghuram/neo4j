/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.plandescription.Arguments.DatabaseAction

class DatabasePrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {
  private val default = "DEFAULT"

  // Access/Start/Stop

  test("Grant access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ACCESS ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "ACCESS", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "ACCESS", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY ACCESS ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "ACCESS", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE ACCESS ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "ACCESS", default, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "ACCESS", default, "reader",
            helperPlan("AssertValidRevoke", Seq(DatabaseAction("ACCESS"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Revoke grant access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT ACCESS ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "ACCESS", default, "reader",
          helperPlan("AssertValidRevoke", Seq(DatabaseAction("ACCESS"), roleArg("reader")),
            assertDbmsAdminPlan("REVOKE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke deny access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY ACCESS ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "ACCESS", default, "reader",
          helperPlan("AssertValidRevoke", Seq(DatabaseAction("ACCESS"), roleArg("reader")),
            assertDbmsAdminPlan("REVOKE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant start") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT START ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "START", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "START", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny start") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY START ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "START", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke start") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE START ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "START", default, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "START", default, "reader",
            helperPlan("AssertValidRevoke", Seq(DatabaseAction("START"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Grant stop") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT STOP ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "STOP", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "STOP", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny stop") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY STOP ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "STOP", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke stop") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE STOP ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "STOP", default, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "STOP", default, "reader",
            helperPlan("AssertValidRevoke", Seq(DatabaseAction("STOP"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  // Schema privileges

  test("Grant create index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE INDEX ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE INDEX", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE INDEX", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE INDEX ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE INDEX", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke grant create index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT CREATE INDEX ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE INDEX", default, "reader",
          helperPlan("AssertValidRevoke", Seq(DatabaseAction("CREATE INDEX"), roleArg("reader")),
            assertDbmsAdminPlan("REVOKE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant drop index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP INDEX ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "DROP INDEX", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "DROP INDEX", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny drop index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY DROP INDEX ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "DROP INDEX", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke deny drop index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY DROP INDEX ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "DROP INDEX", default, "reader",
          helperPlan("AssertValidRevoke", Seq(DatabaseAction("DROP INDEX"), roleArg("reader")),
            assertDbmsAdminPlan("REVOKE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant index management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT INDEX MANAGEMENT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "DROP INDEX", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE INDEX", "editor",
            databasePrivilegePlan("GrantDatabaseAction", "DROP INDEX", "reader",
              databasePrivilegePlan("GrantDatabaseAction", "CREATE INDEX", "reader",
                assertDbmsAdminPlan("GRANT PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny index management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY INDEX MANAGEMENT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "DROP INDEX", SYSTEM_DATABASE_NAME, "reader",
          databasePrivilegePlan("DenyDatabaseAction", "CREATE INDEX", SYSTEM_DATABASE_NAME, "reader",
            assertDbmsAdminPlan("DENY PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke index management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE INDEX MANAGEMENT ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "DROP INDEX", default, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "DROP INDEX", default, "reader",
            databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE INDEX", default, "reader",
              databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE INDEX", default, "reader",
                databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "INDEX MANAGEMENT", default, "reader",
                  databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "INDEX MANAGEMENT", default, "reader",
                    helperPlan("AssertValidRevoke", Seq(DatabaseAction("INDEX MANAGEMENT"), roleArg("reader")),
                      assertDbmsAdminPlan("REVOKE PRIVILEGE")
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

  test("Grant create constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE CONSTRAINT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE CONSTRAINT", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE CONSTRAINT", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE CONSTRAINT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE CONSTRAINT", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke grant create constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT CREATE CONSTRAINT ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE CONSTRAINT", default, "reader",
          helperPlan("AssertValidRevoke", Seq(DatabaseAction("CREATE CONSTRAINT"), roleArg("reader")),
            assertDbmsAdminPlan("REVOKE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant drop constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP CONSTRAINT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "DROP CONSTRAINT", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "DROP CONSTRAINT", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny drop constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY DROP CONSTRAINT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "DROP CONSTRAINT", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke deny drop constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY DROP CONSTRAINT ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "DROP CONSTRAINT", default, "reader",
          helperPlan("AssertValidRevoke", Seq(DatabaseAction("DROP CONSTRAINT"), roleArg("reader")),
            assertDbmsAdminPlan("REVOKE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant constraint management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CONSTRAINT MANAGEMENT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "DROP CONSTRAINT", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE CONSTRAINT", "editor",
            databasePrivilegePlan("GrantDatabaseAction", "DROP CONSTRAINT", "reader",
              databasePrivilegePlan("GrantDatabaseAction", "CREATE CONSTRAINT", "reader",
                assertDbmsAdminPlan("GRANT PRIVILEGE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny constraint management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CONSTRAINT MANAGEMENT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "DROP CONSTRAINT", SYSTEM_DATABASE_NAME, "reader",
          databasePrivilegePlan("DenyDatabaseAction", "CREATE CONSTRAINT", SYSTEM_DATABASE_NAME, "reader",
            assertDbmsAdminPlan("DENY PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke constraint management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE CONSTRAINT MANAGEMENT ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "DROP CONSTRAINT", default, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "DROP CONSTRAINT", default, "reader",
            databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE CONSTRAINT", default, "reader",
              databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE CONSTRAINT", default, "reader",
                databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CONSTRAINT MANAGEMENT", default, "reader",
                  databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CONSTRAINT MANAGEMENT", default, "reader",
                    helperPlan("AssertValidRevoke", Seq(DatabaseAction("CONSTRAINT MANAGEMENT"), roleArg("reader")),
                      assertDbmsAdminPlan("REVOKE PRIVILEGE")
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

  // Token privileges

  test("Grant create label") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE NEW LABEL ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW NODE LABEL", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW NODE LABEL", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create label") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE NEW LABEL ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW NODE LABEL", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke grant create label") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT CREATE NEW LABEL ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE NEW NODE LABEL", default, "reader",
          helperPlan("AssertValidRevoke", Seq(DatabaseAction("CREATE NEW NODE LABEL"), roleArg("reader")),
            assertDbmsAdminPlan("REVOKE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant create type") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE NEW TYPE ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create type") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE NEW TYPE ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke deny create type") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY CREATE NEW TYPE ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE NEW RELATIONSHIP TYPE", default, "reader",
          helperPlan("AssertValidRevoke", Seq(DatabaseAction("CREATE NEW RELATIONSHIP TYPE"), roleArg("reader")),
            assertDbmsAdminPlan("REVOKE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant create property name") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE NEW NAME ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW PROPERTY NAME", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW PROPERTY NAME", "reader",
            assertDbmsAdminPlan("GRANT PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create property name") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE NEW NAME ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW PROPERTY NAME", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("DENY PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke create property name") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE CREATE NEW NAME ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE NEW PROPERTY NAME", default, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE NEW PROPERTY NAME", default, "reader",
            helperPlan("AssertValidRevoke", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), roleArg("reader")),
              assertDbmsAdminPlan("REVOKE PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Grant name management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT NAME MANAGEMENT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW PROPERTY NAME", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", "editor",
            databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW NODE LABEL", "editor",
              databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW PROPERTY NAME", "reader",
                databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", "reader",
                  databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW NODE LABEL", "reader",
                    assertDbmsAdminPlan("GRANT PRIVILEGE")
                  )
                )
              )
            )
          )
        )
      ).toString
    )
  }

  test("Deny name management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY NAME MANAGEMENT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW PROPERTY NAME", SYSTEM_DATABASE_NAME, "reader",
          databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", SYSTEM_DATABASE_NAME, "reader",
            databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW NODE LABEL", SYSTEM_DATABASE_NAME, "reader",
              assertDbmsAdminPlan("DENY PRIVILEGE")
            )
          )
        )
      ).toString
    )
  }

  test("Revoke name management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE NAME MANAGEMENT ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE NEW PROPERTY NAME", default, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE NEW PROPERTY NAME", default, "reader",
            databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE NEW RELATIONSHIP TYPE", default, "reader",
              databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE NEW RELATIONSHIP TYPE", default, "reader",
                databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE NEW NODE LABEL", default, "reader",
                  databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE NEW NODE LABEL", default, "reader",
                    databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "NAME MANAGEMENT", default, "reader",
                      databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "NAME MANAGEMENT", default, "reader",
                        helperPlan("AssertValidRevoke", Seq(DatabaseAction("NAME MANAGEMENT"), roleArg("reader")),
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
      ).toString
    )
  }

  // All database privilege

  test("Grant all database privileges") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ALL ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW PROPERTY NAME", "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", "editor",
            databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW NODE LABEL", "editor",
              databasePrivilegePlan("GrantDatabaseAction", "DROP INDEX", "editor",
                databasePrivilegePlan("GrantDatabaseAction", "CREATE INDEX", "editor",
                  databasePrivilegePlan("GrantDatabaseAction", "DROP CONSTRAINT", "editor",
                    databasePrivilegePlan("GrantDatabaseAction", "CREATE CONSTRAINT", "editor",
                      databasePrivilegePlan("GrantDatabaseAction", "STOP", "editor",
                        databasePrivilegePlan("GrantDatabaseAction", "START", "editor",
                          databasePrivilegePlan("GrantDatabaseAction", "ACCESS", "editor",
                            databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW PROPERTY NAME", "reader",
                              databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", "reader",
                                databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW NODE LABEL", "reader",
                                  databasePrivilegePlan("GrantDatabaseAction", "DROP INDEX", "reader",
                                    databasePrivilegePlan("GrantDatabaseAction", "CREATE INDEX", "reader",
                                      databasePrivilegePlan("GrantDatabaseAction", "DROP CONSTRAINT", "reader",
                                        databasePrivilegePlan("GrantDatabaseAction", "CREATE CONSTRAINT", "reader",
                                          databasePrivilegePlan("GrantDatabaseAction", "STOP", "reader",
                                            databasePrivilegePlan("GrantDatabaseAction", "START", "reader",
                                              databasePrivilegePlan("GrantDatabaseAction", "ACCESS", "reader",
                                                assertDbmsAdminPlan("GRANT PRIVILEGE")
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

  test("Deny all database privileges") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY ALL ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW PROPERTY NAME", SYSTEM_DATABASE_NAME, "reader",
          databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", SYSTEM_DATABASE_NAME, "reader",
            databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW NODE LABEL", SYSTEM_DATABASE_NAME, "reader",
              databasePrivilegePlan("DenyDatabaseAction", "DROP INDEX", SYSTEM_DATABASE_NAME, "reader",
                databasePrivilegePlan("DenyDatabaseAction", "CREATE INDEX", SYSTEM_DATABASE_NAME, "reader",
                  databasePrivilegePlan("DenyDatabaseAction", "DROP CONSTRAINT", SYSTEM_DATABASE_NAME, "reader",
                    databasePrivilegePlan("DenyDatabaseAction", "CREATE CONSTRAINT", SYSTEM_DATABASE_NAME, "reader",
                      databasePrivilegePlan("DenyDatabaseAction", "STOP", SYSTEM_DATABASE_NAME, "reader",
                        databasePrivilegePlan("DenyDatabaseAction", "START", SYSTEM_DATABASE_NAME, "reader",
                          databasePrivilegePlan("DenyDatabaseAction", "ACCESS", SYSTEM_DATABASE_NAME, "reader",
                            assertDbmsAdminPlan("DENY PRIVILEGE")
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

  test("Revoke all database privileges") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE ALL ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE NEW PROPERTY NAME", default, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE NEW PROPERTY NAME", default, "reader",
            databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE NEW RELATIONSHIP TYPE", default, "reader",
              databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE NEW RELATIONSHIP TYPE", default, "reader",
                databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE NEW NODE LABEL", default, "reader",
                  databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE NEW NODE LABEL", default, "reader",
                    databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "NAME MANAGEMENT", default, "reader",
                      databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "NAME MANAGEMENT", default, "reader",
                        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "DROP INDEX", default, "reader",
                          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "DROP INDEX", default, "reader",
                            databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE INDEX", default, "reader",
                              databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE INDEX", default, "reader",
                                databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "INDEX MANAGEMENT", default, "reader",
                                  databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "INDEX MANAGEMENT", default, "reader",
                                    databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "DROP CONSTRAINT", default, "reader",
                                      databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "DROP CONSTRAINT", default, "reader",
                                        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE CONSTRAINT", default, "reader",
                                          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE CONSTRAINT", default, "reader",
                                            databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CONSTRAINT MANAGEMENT", default, "reader",
                                              databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CONSTRAINT MANAGEMENT", default, "reader",
                                                databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "SCHEMA MANAGEMENT", default, "reader",
                                                  databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "SCHEMA MANAGEMENT", default, "reader",
                                                    databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "STOP", default, "reader",
                                                      databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "STOP", default, "reader",
                                                        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "START", default, "reader",
                                                          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "START", default, "reader",
                                                            databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "ACCESS", default, "reader",
                                                              databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "ACCESS", default, "reader",
                                                                databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "ALL DATABASE PRIVILEGES", default, "reader",
                                                                  databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "ALL DATABASE PRIVILEGES", default, "reader",
                                                                    helperPlan("AssertValidRevoke", Seq(DatabaseAction("ALL DATABASE PRIVILEGES"), roleArg("reader")),
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
                )
              )
            )
          )
        )
      ).toString
    )
  }
}
