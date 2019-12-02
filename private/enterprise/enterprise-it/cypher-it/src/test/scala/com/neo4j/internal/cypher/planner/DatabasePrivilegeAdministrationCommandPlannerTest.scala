/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.plandescription.Arguments.{Database, DatabaseAction, DbmsAction}
import org.neo4j.cypher.internal.plandescription.SingleChild

class DatabasePrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Access/Start/Stop

  test("Grant access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ACCESS ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDatabaseAction", Seq(DatabaseAction("ACCESS"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDatabaseAction", Seq(DatabaseAction("ACCESS"), Database("*"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY ACCESS ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDatabaseAction", Seq(DatabaseAction("ACCESS"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE ACCESS ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("ACCESS"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrant = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("ACCESS"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val revokeDeny = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("ACCESS"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeGrant))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDeny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke grant access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT ACCESS ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("ACCESS"), roleArg("reader")), SingleChild(assertAdmin))
    val revoke = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("ACCESS"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revoke))
    plan should include(expectedPlan.toString)
  }

  test("Revoke deny access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY ACCESS ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("ACCESS"), roleArg("reader")), SingleChild(assertAdmin))
    val revoke = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("ACCESS"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revoke))
    plan should include(expectedPlan.toString)
  }

  test("Grant start") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT START ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDatabaseAction", Seq(DatabaseAction("START"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDatabaseAction", Seq(DatabaseAction("START"), Database("*"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny start") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY START ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDatabaseAction", Seq(DatabaseAction("START"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke start") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE START ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("START"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrant = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("START"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val revokeDeny = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("START"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeGrant))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDeny))
    plan should include(expectedPlan.toString)
  }

  test("Grant stop") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT STOP ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDatabaseAction", Seq(DatabaseAction("STOP"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDatabaseAction", Seq(DatabaseAction("STOP"), Database("*"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny stop") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY STOP ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDatabaseAction", Seq(DatabaseAction("STOP"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke stop") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE STOP ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("STOP"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrant = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("STOP"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val revokeDeny = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("STOP"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeGrant))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDeny))
    plan should include(expectedPlan.toString)
  }

  // Schema privileges

  test("Grant create index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE INDEX ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE INDEX"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE INDEX"), Database("*"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny create index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE INDEX ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDatabaseAction", Seq(DatabaseAction("CREATE INDEX"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke grant create index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT CREATE INDEX ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("CREATE INDEX"), roleArg("reader")), SingleChild(assertAdmin))
    val revoke = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("CREATE INDEX"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revoke))
    plan should include(expectedPlan.toString)
  }

  test("Grant drop index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP INDEX ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDatabaseAction", Seq(DatabaseAction("DROP INDEX"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDatabaseAction", Seq(DatabaseAction("DROP INDEX"), Database("*"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny drop index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY DROP INDEX ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDatabaseAction", Seq(DatabaseAction("DROP INDEX"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke deny drop index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY DROP INDEX ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("DROP INDEX"), roleArg("reader")), SingleChild(assertAdmin))
    val revoke = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("DROP INDEX"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revoke))
    plan should include(expectedPlan.toString)
  }

  test("Grant index management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT INDEX MANAGEMENT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReaderCreate = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE INDEX"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantReaderDrop = planDescription("GrantDatabaseAction", Seq(DatabaseAction("DROP INDEX"), Database("*"), roleArg("reader")), SingleChild(grantReaderCreate))
    val grantEditorCreate = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE INDEX"), Database("*"), roleArg("editor")), SingleChild(grantReaderDrop))
    val grantEditorDrop = planDescription("GrantDatabaseAction", Seq(DatabaseAction("DROP INDEX"), Database("*"), roleArg("editor")), SingleChild(grantEditorCreate))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditorDrop))
    plan should include(expectedPlan.toString)
  }

  test("Deny index management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY INDEX MANAGEMENT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val denyCreate = planDescription("DenyDatabaseAction", Seq(DatabaseAction("CREATE INDEX"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertAdmin))
    val denyDrop = planDescription("DenyDatabaseAction", Seq(DatabaseAction("DROP INDEX"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(denyCreate))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(denyDrop))
    plan should include(expectedPlan.toString)
  }

  test("Revoke index management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE INDEX MANAGEMENT ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("INDEX MANAGEMENT"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrantManagement = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("INDEX MANAGEMENT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val revokeDenyManagement = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("INDEX MANAGEMENT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeGrantManagement))
    val revokeGrantCreate = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("CREATE INDEX"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeDenyManagement))
    val revokeDenyCreate = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("CREATE INDEX"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeGrantCreate))
    val revokeGrantDrop = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("DROP INDEX"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeDenyCreate))
    val revokeDenyDrop = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("DROP INDEX"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeGrantDrop))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDenyDrop))
    plan should include(expectedPlan.toString)
  }

  test("Grant create constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE CONSTRAINT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE CONSTRAINT"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE CONSTRAINT"), Database("*"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny create constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE CONSTRAINT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDatabaseAction", Seq(DatabaseAction("CREATE CONSTRAINT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke grant create constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT CREATE CONSTRAINT ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("CREATE CONSTRAINT"), roleArg("reader")), SingleChild(assertAdmin))
    val revoke = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("CREATE CONSTRAINT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revoke))
    plan should include(expectedPlan.toString)
  }

  test("Grant drop constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP CONSTRAINT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDatabaseAction", Seq(DatabaseAction("DROP CONSTRAINT"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDatabaseAction", Seq(DatabaseAction("DROP CONSTRAINT"), Database("*"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny drop constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY DROP CONSTRAINT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDatabaseAction", Seq(DatabaseAction("DROP CONSTRAINT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke deny drop constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY DROP CONSTRAINT ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("DROP CONSTRAINT"), roleArg("reader")), SingleChild(assertAdmin))
    val revoke = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("DROP CONSTRAINT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revoke))
    plan should include(expectedPlan.toString)
  }

  test("Revoke constraint management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE CONSTRAINT MANAGEMENT ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("CONSTRAINT MANAGEMENT"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrantManagement = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("CONSTRAINT MANAGEMENT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val revokeDenyManagement = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("CONSTRAINT MANAGEMENT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeGrantManagement))
    val revokeGrantCreate = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("CREATE CONSTRAINT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeDenyManagement))
    val revokeDenyCreate = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("CREATE CONSTRAINT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeGrantCreate))
    val revokeGrantDrop = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("DROP CONSTRAINT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeDenyCreate))
    val revokeDenyDrop = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("DROP CONSTRAINT"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(revokeGrantDrop))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDenyDrop))
    plan should include(expectedPlan.toString)
  }

  // Token privileges

  test("Grant create label") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE NEW LABEL ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW NODE LABEL"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW NODE LABEL"), Database("*"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny create label") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE NEW LABEL ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDatabaseAction", Seq(DatabaseAction("CREATE NEW NODE LABEL"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke grant create label") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT CREATE NEW LABEL ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("CREATE NEW NODE LABEL"), roleArg("reader")), SingleChild(assertAdmin))
    val revoke = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("CREATE NEW NODE LABEL"), databaseArg(SYSTEM_DATABASE_NAME), roleArg("reader")), SingleChild(assertValid))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revoke))
    plan should include(expectedPlan.toString)
  }

  test("Grant create type") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE NEW TYPE ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW RELATIONSHIP TYPE"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW RELATIONSHIP TYPE"), Database("*"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny create type") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE NEW TYPE ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDatabaseAction", Seq(DatabaseAction("CREATE NEW RELATIONSHIP TYPE"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke deny create type") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY CREATE NEW TYPE ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("CREATE NEW RELATIONSHIP TYPE"), roleArg("reader")), SingleChild(assertAdmin))
    val revoke = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("CREATE NEW RELATIONSHIP TYPE"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(assertValid))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revoke))
    plan should include(expectedPlan.toString)
  }

  test("Grant create property name") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE NEW NAME ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), Database("*"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny create property name") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE NEW NAME ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDatabaseAction", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke create property name") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE CREATE NEW NAME ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrant = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(assertValid))
    val revokeDeny = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(revokeGrant))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDeny))
    plan should include(expectedPlan.toString)
  }

  test("Grant name management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT NAME MANAGEMENT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReaderLabel = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW NODE LABEL"), Database("*"), roleArg("reader")), SingleChild(assertAdmin))
    val grantReaderType = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW RELATIONSHIP TYPE"), Database("*"), roleArg("reader")), SingleChild(grantReaderLabel))
    val grantReaderProp = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), Database("*"), roleArg("reader")), SingleChild(grantReaderType))
    val grantEditorLabel = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW NODE LABEL"), Database("*"), roleArg("editor")), SingleChild(grantReaderProp))
    val grantEditorType = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW RELATIONSHIP TYPE"), Database("*"), roleArg("editor")), SingleChild(grantEditorLabel))
    val grantEditorProp = planDescription("GrantDatabaseAction", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), Database("*"), roleArg("editor")), SingleChild(grantEditorType))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditorProp))
    plan should include(expectedPlan.toString)
  }

  test("Deny name management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY NAME MANAGEMENT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val denyLabel = planDescription("DenyDatabaseAction", Seq(DatabaseAction("CREATE NEW NODE LABEL"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(assertAdmin))
    val denyType = planDescription("DenyDatabaseAction", Seq(DatabaseAction("CREATE NEW RELATIONSHIP TYPE"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(denyLabel))
    val denyProp = planDescription("DenyDatabaseAction", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(denyType))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(denyProp))
    plan should include(expectedPlan.toString)
  }

  test("Revoke name management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE NAME MANAGEMENT ON DATABASE $SYSTEM_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DatabaseAction("NAME MANAGEMENT"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrantManagement = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("NAME MANAGEMENT"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(assertValid))
    val revokeDenyManagement = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("NAME MANAGEMENT"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(revokeGrantManagement))
    val revokeGrantLabel = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("CREATE NEW NODE LABEL"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(revokeDenyManagement))
    val revokeDenyLabel = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("CREATE NEW NODE LABEL"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(revokeGrantLabel))
    val revokeGrantType = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("CREATE NEW RELATIONSHIP TYPE"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(revokeDenyLabel))
    val revokeDenyType = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("CREATE NEW RELATIONSHIP TYPE"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(revokeGrantType))
    val revokeGrantProp = planDescription("RevokeDatabaseAction(GRANTED)", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(revokeDenyType))
    val revokeDenyProp = planDescription("RevokeDatabaseAction(DENIED)", Seq(DatabaseAction("CREATE NEW PROPERTY NAME"), databaseArg(SYSTEM_DATABASE_NAME),
      roleArg("reader")), SingleChild(revokeGrantProp))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDenyProp))
    plan should include(expectedPlan.toString)
  }
}
