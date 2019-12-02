/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.cypher.internal.plandescription.Arguments.{Database, DbmsAction, Qualifier}
import org.neo4j.cypher.internal.plandescription.SingleChild

class GraphPrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Traverse

  test("Grant traverse") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN GRANT TRAVERSE ON GRAPH $DEFAULT_DATABASE_NAME TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantNodes1 = planDescription("GrantTraverse", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(assertAdmin))
    val grantRelationships1 = planDescription("GrantTraverse", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(grantNodes1))
    val grantNodes2 = planDescription("GrantTraverse", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("NODES *"), roleArg("editor")), SingleChild(grantRelationships1))
    val grantRelationships2 = planDescription("GrantTraverse", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("editor")), SingleChild(grantNodes2))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantRelationships2))
    plan should include(expectedPlan.toString)
  }

  test("Deny traverse") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY TRAVERSE ON GRAPH $SYSTEM_DATABASE_NAME NODE A TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val denyNodes = planDescription("DenyTraverse", Seq(databaseArg(SYSTEM_DATABASE_NAME), qualifierArg("NODE", "A"), roleArg("reader")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(denyNodes))
    plan should include(expectedPlan.toString)
  }

  test("Revoke traverse") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE TRAVERSE ON GRAPH $DEFAULT_DATABASE_NAME RELATIONSHIPS A, B FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val revokeAGrant = planDescription("RevokeTraverse(GRANTED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), qualifierArg("RELATIONSHIP", "A"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeADeny = planDescription("RevokeTraverse(DENIED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), qualifierArg("RELATIONSHIP", "A"), roleArg("reader")), SingleChild(revokeAGrant))
    val revokeBGrant = planDescription("RevokeTraverse(GRANTED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), qualifierArg("RELATIONSHIP", "B"), roleArg("reader")), SingleChild(revokeADeny))
    val revokeBDeny = planDescription("RevokeTraverse(DENIED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), qualifierArg("RELATIONSHIP", "B"), roleArg("reader")), SingleChild(revokeBGrant))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeBDeny))
    plan should include(expectedPlan.toString)
  }

  // Read

  test("Grant read") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN GRANT READ {*} ON GRAPH $DEFAULT_DATABASE_NAME TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantNodes1 = planDescription("GrantRead", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(assertAdmin))
    val grantRelationships1 = planDescription("GrantRead", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(grantNodes1))
    val grantNodes2 = planDescription("GrantRead", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("NODES *"), roleArg("editor")), SingleChild(grantRelationships1))
    val grantRelationships2 = planDescription("GrantRead", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("editor")), SingleChild(grantNodes2))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantRelationships2))
    plan should include(expectedPlan.toString)
  }

  test("Deny read") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY READ {foo, prop} ON GRAPH $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val denyNodes1 = planDescription("DenyRead", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(assertAdmin))
    val denyNodes2 = planDescription("DenyRead", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(denyNodes1))
    val denyRelationships1 = planDescription("DenyRead", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(denyNodes2))
    val denyRelationships2 = planDescription("DenyRead", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(denyRelationships1))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(denyRelationships2))
    plan should include(expectedPlan.toString)
  }

  test("Revoke read") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE READ {prop} ON GRAPH $DEFAULT_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val revokeNodesGrant = planDescription("RevokeRead(GRANTED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeNodesDeny = planDescription("RevokeRead(DENIED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(revokeNodesGrant))
    val revokeRelationshipsGrant = planDescription("RevokeRead(GRANTED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(revokeNodesDeny))
    val revokeRelationshipsDeny = planDescription("RevokeRead(DENIED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(revokeRelationshipsGrant))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeRelationshipsDeny))
    plan should include(expectedPlan.toString)
  }

  // Match

  test("Grant match") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN GRANT MATCH {*} ON GRAPH $DEFAULT_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantNodes1 = planDescription("GrantTraverse", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(assertAdmin))
    val grantRelationships1 = planDescription("GrantTraverse", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(grantNodes1))
    val grantNodes2 = planDescription("GrantRead", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(grantRelationships1))
    val grantRelationships2 = planDescription("GrantRead", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(grantNodes2))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantRelationships2))
    plan should include(expectedPlan.toString)
  }

  test("Deny match all") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY MATCH {*} ON GRAPH $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val denyNodes1 = planDescription("DenyTraverse", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(assertAdmin))
    val denyRelationships1 = planDescription("DenyTraverse", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(denyNodes1))
    val denyNodes2 = planDescription("DenyRead", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(denyRelationships1))
    val denyRelationships2 = planDescription("DenyRead", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(denyNodes2))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(denyRelationships2))
    plan should include(expectedPlan.toString)
  }

  test("Deny match prop") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY MATCH {prop} ON GRAPH $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val denyNodes = planDescription("DenyRead", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(assertAdmin))
    val denyRelationships = planDescription("DenyRead", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(denyNodes))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(denyRelationships))
    plan should include(expectedPlan.toString)
  }

  // Write

  test("Grant write") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT WRITE ON GRAPH * TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("GRANT PRIVILEGE")))
    val grantNodes1 = planDescription("GrantWrite", Seq(Database("*"), Qualifier("NODES *"), roleArg("reader")), SingleChild(assertAdmin))
    val grantRelationships1 = planDescription("GrantWrite", Seq(Database("*"), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(grantNodes1))
    val grantNodes2 = planDescription("GrantWrite", Seq(Database("*"), Qualifier("NODES *"), roleArg("editor")), SingleChild(grantRelationships1))
    val grantRelationships2 = planDescription("GrantWrite", Seq(Database("*"), Qualifier("RELATIONSHIPS *"), roleArg("editor")), SingleChild(grantNodes2))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantRelationships2))
    plan should include(expectedPlan.toString)
  }

  test("Deny write") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY WRITE ON GRAPH $SYSTEM_DATABASE_NAME TO reader, editor").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DENY PRIVILEGE")))
    val denyNodes1 = planDescription("DenyWrite", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(assertAdmin))
    val denyRelationships1 = planDescription("DenyWrite", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(denyNodes1))
    val denyNodes2 = planDescription("DenyWrite", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("NODES *"), roleArg("editor")), SingleChild(denyRelationships1))
    val denyRelationships2 = planDescription("DenyWrite", Seq(databaseArg(SYSTEM_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("editor")), SingleChild(denyNodes2))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(denyRelationships2))
    plan should include(expectedPlan.toString)
  }

  test("Revoke grant write") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT WRITE ON GRAPH $DEFAULT_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val revokeNodes = planDescription("RevokeWrite(GRANTED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeRelationships = planDescription("RevokeWrite(GRANTED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(revokeNodes))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeRelationships))
    plan should include(expectedPlan.toString)
  }

  test("Revoke deny write") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY WRITE ON GRAPH $DEFAULT_DATABASE_NAME FROM reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REVOKE PRIVILEGE")))
    val revokeNodes = planDescription("RevokeWrite(DENIED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("NODES *"), roleArg("reader")), SingleChild(assertAdmin))
    val revokeRelationships = planDescription("RevokeWrite(DENIED)", Seq(databaseArg(DEFAULT_DATABASE_NAME), Qualifier("RELATIONSHIPS *"), roleArg("reader")), SingleChild(revokeNodes))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(revokeRelationships))
    plan should include(expectedPlan.toString)
  }
}
