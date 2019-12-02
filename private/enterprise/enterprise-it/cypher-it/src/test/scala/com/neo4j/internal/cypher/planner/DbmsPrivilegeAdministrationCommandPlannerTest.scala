/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.plandescription.Arguments.DbmsAction
import org.neo4j.cypher.internal.plandescription.SingleChild

class DbmsPrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Role privileges

  test("Grant show role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT SHOW ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDbmsAction", Seq(DbmsAction("SHOW ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDbmsAction", Seq(DbmsAction("SHOW ROLE"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny show role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY SHOW ROLE ON DBMS TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDbmsAction", Seq(DbmsAction("SHOW ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke show role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE SHOW ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DbmsAction("SHOW ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrant = planDescription("RevokeDbmsAction(GRANTED)", Seq(DbmsAction("SHOW ROLE"), roleArg("reader")), SingleChild(assertValid))
    val revokeDeny = planDescription("RevokeDbmsAction(DENIED)", Seq(DbmsAction("SHOW ROLE"), roleArg("reader")), SingleChild(revokeGrant))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDeny))
    plan should include(expectedPlan.toString)
  }

  test("Grant create role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDbmsAction", Seq(DbmsAction("CREATE ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDbmsAction", Seq(DbmsAction("CREATE ROLE"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny create role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CREATE ROLE ON DBMS TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDbmsAction", Seq(DbmsAction("CREATE ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke create role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE CREATE ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DbmsAction("CREATE ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrant = planDescription("RevokeDbmsAction(GRANTED)", Seq(DbmsAction("CREATE ROLE"), roleArg("reader")), SingleChild(assertValid))
    val revokeDeny = planDescription("RevokeDbmsAction(DENIED)", Seq(DbmsAction("CREATE ROLE"), roleArg("reader")), SingleChild(revokeGrant))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDeny))
    plan should include(expectedPlan.toString)
  }

  test("Grant drop role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDbmsAction", Seq(DbmsAction("DROP ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDbmsAction", Seq(DbmsAction("DROP ROLE"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny drop role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY DROP ROLE ON DBMS TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDbmsAction", Seq(DbmsAction("DROP ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke drop role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE DROP ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DbmsAction("DROP ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrant = planDescription("RevokeDbmsAction(GRANTED)", Seq(DbmsAction("DROP ROLE"), roleArg("reader")), SingleChild(assertValid))
    val revokeDeny = planDescription("RevokeDbmsAction(DENIED)", Seq(DbmsAction("DROP ROLE"), roleArg("reader")), SingleChild(revokeGrant))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDeny))
    plan should include(expectedPlan.toString)
  }

  test("Grant assign role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ASSIGN ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDbmsAction", Seq(DbmsAction("ASSIGN ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDbmsAction", Seq(DbmsAction("ASSIGN ROLE"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny assign role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ASSIGN ROLE ON DBMS TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDbmsAction", Seq(DbmsAction("ASSIGN ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke assign role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE ASSIGN ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DbmsAction("ASSIGN ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrant = planDescription("RevokeDbmsAction(GRANTED)", Seq(DbmsAction("ASSIGN ROLE"), roleArg("reader")), SingleChild(assertValid))
    val revokeDeny = planDescription("RevokeDbmsAction(DENIED)", Seq(DbmsAction("ASSIGN ROLE"), roleArg("reader")), SingleChild(revokeGrant))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDeny))
    plan should include(expectedPlan.toString)
  }

  test("Grant remove role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT REMOVE ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDbmsAction", Seq(DbmsAction("REMOVE ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDbmsAction", Seq(DbmsAction("REMOVE ROLE"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny remove role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY REMOVE ROLE ON DBMS TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDbmsAction", Seq(DbmsAction("REMOVE ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke remove role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE REMOVE ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DbmsAction("REMOVE ROLE"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrant = planDescription("RevokeDbmsAction(GRANTED)", Seq(DbmsAction("REMOVE ROLE"), roleArg("reader")), SingleChild(assertValid))
    val revokeDeny = planDescription("RevokeDbmsAction(DENIED)", Seq(DbmsAction("REMOVE ROLE"), roleArg("reader")), SingleChild(revokeGrant))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDeny))
    plan should include(expectedPlan.toString)
  }

  test("Grant role management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ROLE MANAGEMENT ON DBMS TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantReader = planDescription("GrantDbmsAction", Seq(DbmsAction("ROLE MANAGEMENT"), roleArg("reader")), SingleChild(assertAdmin))
    val grantEditor = planDescription("GrantDbmsAction", Seq(DbmsAction("ROLE MANAGEMENT"), roleArg("editor")), SingleChild(grantReader))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantEditor))
    plan should include(expectedPlan.toString)
  }

  test("Deny role management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ROLE MANAGEMENT ON DBMS TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val deny = planDescription("DenyDbmsAction", Seq(DbmsAction("ROLE MANAGEMENT"), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(deny))
    plan should include(expectedPlan.toString)
  }

  test("Revoke role management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE ROLE MANAGEMENT ON DBMS FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val assertValid = planDescription("AssertValidRevoke", Seq(DbmsAction("ROLE MANAGEMENT"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeGrantManagement = planDescription("RevokeDbmsAction(GRANTED)", Seq(DbmsAction("ROLE MANAGEMENT"), roleArg("reader")), SingleChild(assertValid))
    val revokeDenyManagement = planDescription("RevokeDbmsAction(DENIED)", Seq(DbmsAction("ROLE MANAGEMENT"), roleArg("reader")), SingleChild(revokeGrantManagement))
    val revokeGrantCreate = planDescription("RevokeDbmsAction(GRANTED)", Seq(DbmsAction("CREATE ROLE"), roleArg("reader")), SingleChild(revokeDenyManagement))
    val revokeDenyCreate = planDescription("RevokeDbmsAction(DENIED)", Seq(DbmsAction("CREATE ROLE"), roleArg("reader")), SingleChild(revokeGrantCreate))
    val revokeGrantDrop = planDescription("RevokeDbmsAction(GRANTED)", Seq(DbmsAction("DROP ROLE"), roleArg("reader")), SingleChild(revokeDenyCreate))
    val revokeDenyDrop = planDescription("RevokeDbmsAction(DENIED)", Seq(DbmsAction("DROP ROLE"), roleArg("reader")), SingleChild(revokeGrantDrop))
    val revokeGrantAssign = planDescription("RevokeDbmsAction(GRANTED)", Seq(DbmsAction("ASSIGN ROLE"), roleArg("reader")), SingleChild(revokeDenyDrop))
    val revokeDenyAssign = planDescription("RevokeDbmsAction(DENIED)", Seq(DbmsAction("ASSIGN ROLE"), roleArg("reader")), SingleChild(revokeGrantAssign))
    val revokeGrantRemove = planDescription("RevokeDbmsAction(GRANTED)", Seq(DbmsAction("REMOVE ROLE"), roleArg("reader")), SingleChild(revokeDenyAssign))
    val revokeDenyRemove = planDescription("RevokeDbmsAction(DENIED)", Seq(DbmsAction("REMOVE ROLE"), roleArg("reader")), SingleChild(revokeGrantRemove))
    val revokeGrantShow = planDescription("RevokeDbmsAction(GRANTED)", Seq(DbmsAction("SHOW ROLE"), roleArg("reader")), SingleChild(revokeDenyRemove))
    val revokeDenyShow = planDescription("RevokeDbmsAction(DENIED)", Seq(DbmsAction("SHOW ROLE"), roleArg("reader")), SingleChild(revokeGrantShow))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeDenyShow))
    plan should include(expectedPlan.toString)
  }
}
