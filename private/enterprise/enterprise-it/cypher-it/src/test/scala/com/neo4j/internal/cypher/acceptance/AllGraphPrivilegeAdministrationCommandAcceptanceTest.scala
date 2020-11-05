/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.security.AuthorizationViolationException

class AllGraphPrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase with EnterpriseComponentVersionTestSupport {

  Seq(
    ("grant", granted: privilegeFunction),
    ("deny", denied: privilegeFunction),
  ).foreach {
    case (grantOrDeny, grantedOrDenied: privilegeFunction) =>
      val grantOrDenyCommand = grantOrDeny.toUpperCase

      test(s"should $grantOrDeny all graph privileges for all graphs") {
        // GIVEN
        execute(s"CREATE ROLE $roleName")

        // WHEN
        execute(s"$grantOrDenyCommand ALL GRAPH PRIVILEGES ON GRAPH * TO $roleName")

        // THEN
        execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(grantedOrDenied(allGraphPrivileges).role(roleName).map))
      }

      test(s"should $grantOrDeny all graph privileges for default graph") {
        // GIVEN
        execute(s"CREATE ROLE $roleName")

        // WHEN
        execute(s"$grantOrDenyCommand ALL GRAPH PRIVILEGES ON DEFAULT GRAPH TO $roleName")

        // THEN
        execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(grantedOrDenied(allGraphPrivileges).role(roleName).graph(DEFAULT).map))
      }

      test(s"should $grantOrDeny all graph privileges for specific graph") {
        // GIVEN
        execute(s"CREATE ROLE $roleName")
        execute(s"CREATE DATABASE $databaseString")

        // WHEN
        execute(s"$grantOrDenyCommand ALL GRAPH PRIVILEGES ON GRAPH $databaseString TO $roleName")

        // THEN
        execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(grantedOrDenied(allGraphPrivileges).role(roleName).graph(databaseString).map))
      }

      test(s"should $grantOrDeny all graph privileges for multiple graphs using parameter") {
        // GIVEN
        execute(s"CREATE ROLE $roleName")
        execute(s"CREATE DATABASE $databaseString")
        execute(s"CREATE DATABASE $databaseString2")

        // WHEN
        execute(s"$grantOrDenyCommand ALL GRAPH PRIVILEGES ON GRAPH $databaseString, $$graph TO $roleName", Map("graph" -> databaseString2))

        // THEN
        execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
          grantedOrDenied(allGraphPrivileges).role(roleName).graph(databaseString).map,
          grantedOrDenied(allGraphPrivileges).role(roleName).graph(databaseString2).map
        ))
      }
  }

  test("should not revoke other graph privileges when revoking all graph privileges") {
    // GIVEN
    execute(s"CREATE ROLE $roleName AS COPY of editor")
    execute(s"CREATE DATABASE $databaseString")

    execute(s"GRANT TRAVERSE ON GRAPH $databaseString TO $roleName")
    execute(s"GRANT READ {prop} ON GRAPH * NODE A TO $roleName")
    execute(s"GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO $roleName")

    // WHEN
    execute(s"REVOKE ALL GRAPH PRIVILEGES ON GRAPH * FROM $roleName")

    // THEN
    execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
      granted(access).role(roleName).map,
      granted(matchPrivilege).node("*").role(roleName).map,
      granted(matchPrivilege).relationship("*").role(roleName).map,
      granted(write).node("*").role(roleName).map,
      granted(write).relationship("*").role(roleName).map,
      granted(traverse).node("*").graph(databaseString).role(roleName).map,
      granted(traverse).relationship("*").graph(databaseString).role(roleName).map,
      granted(read).node("A").property("prop").role(roleName).map
    ))
  }

  test("should revoke sub-privilege even if all graph privilege exists") {
    // GIVEN
    execute(s"CREATE ROLE $roleName AS COPY of reader")
    execute(s"GRANT SET LABEL foo ON GRAPH * TO $roleName")
    execute(s"GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO $roleName")

    // WHEN
    execute(s"REVOKE SET LABEL foo ON GRAPH * FROM $roleName")
    execute(s"REVOKE ACCESS ON DATABASE * FROM $roleName")
    execute(s"REVOKE MATCH {*} ON GRAPH * FROM $roleName")

    // THEN
    execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(granted(allGraphPrivileges).role(roleName).map))
  }

  // Enforcements tests

  withAllSystemGraphVersions(unsupportedBefore41) {
    test("should be allowed to traverse and read when granted all graph privileges") {
      // GIVEN
      setupUserWithCustomRole()
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (:A {prop: 'nodeValue'})-[:R {prop: 'relValue'}]->(:B)")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute(s"GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO $roleName")

      // THEN

      // Should be allowed to traverse and read nodes
      executeOnDefault(username, password, "MATCH (n:A) RETURN n.prop", resultHandler = (row, _) => {
        row.get("n.prop") should be("nodeValue")
      }) should be(1)

      // Should be allowed to traverse and read relationships
      executeOnDefault(username, password, "MATCH (:A)-[r:R]->(:B) RETURN r.prop", resultHandler = (row, _) => {
        row.get("r.prop") should be("relValue")
      }) should be(1)
    }
  }

  test("should be allowed to write when granted all graph privileges") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('A')")
    execute("CALL db.createLabel('B')")
    execute("CALL db.createRelationshipType('R')")
    execute("CALL db.createProperty('prop')")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO $roleName")

    // THEN

    // Should be allowed to create nodes and relationships
    executeOnDefault(username, password, "CREATE (:A {prop:'value'})-[r:R {prop: 'relValue'}]->()")

    // Should be allowed to set labels
    executeOnDefault(username, password, "MATCH (n:A) SET n:B")

    // Should be allowed to remove labels
    executeOnDefault(username, password, "MATCH (n:A:B) REMOVE n:A")

    // Should be allowed to set property
    executeOnDefault(username, password, "MATCH (n:B) SET n.prop = 'value2'")

    // Confirm that all writes took effect
    val result = execute("MATCH (n:B)-[r]->() RETURN n.prop, r.prop, labels(n) AS labels")
    result.toSet should be(Set(Map("n.prop" -> "value2", "r.prop" -> "relValue", "labels" -> List("B"))))
  }

  test("all graph privileges should not imply that database or dbms commands are allowed") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    execute(s"GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO $roleName")

    // THEN

    // not allowed to run database command
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault(username, password, "CREATE CONSTRAINT foo_constraint ON (n:Label) ASSERT exists(n.prop)")
    } should have message s"Schema operations are not allowed for user '$username' with roles [$PUBLIC, $roleName]."

     // not allowed to run dbms command
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, "CREATE ROLE role")
    } should have message PERMISSION_DENIED_CREATE_ROLE
  }

  test("should not be allowed to access or change the graph via commands when denied all graph privileges") {
    // GIVEN
    setupUserWithCustomAdminRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {prop: 'nodeValue'})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"DENY ALL GRAPH PRIVILEGES ON GRAPH * TO $roleName")

    // THEN

    // Should not be allowed traverse
    executeOnDefault(username, password, "MATCH (n:A) RETURN n") should be(0)

    // Should not be allowed read
    executeOnDefault(username, password, "MATCH (n) RETURN n.prop") should be(0)

    // Should not be allowed write
     the[AuthorizationViolationException] thrownBy {
      executeOnDefault(username, password, "CREATE (:A {prop:'value'})")
    } should have message s"Create node with labels 'A' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("should not be allowed graph commands on wrong graph when granted all graph privileges on other graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"CREATE DATABASE $databaseString")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {prop: 'nodeValue'})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT ALL GRAPH PRIVILEGES ON GRAPH $databaseString TO $roleName")

    // THEN

    // Should not be allowed traverse on default database
    executeOnDefault(username, password, "MATCH (n:A) RETURN n") should be(0)

    // Should not be allowed read on default database
    executeOnDefault(username, password, "MATCH (n) RETURN n.prop") should be(0)

    // Should not be allowed write on default database
     the[AuthorizationViolationException] thrownBy {
      executeOnDefault(username, password, "CREATE (:A {prop:'value'})")
    } should have message s"Create node with labels 'A' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("should behave correctly when granted all graph privileges but denied sub-privilege") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {prop: 1})")
    execute("CREATE (:B {prop: 2})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * NODES A TO $roleName")

    // THEN
    // Should only be allowed to traverse :B
       executeOnDefault(username, password, "MATCH (n) RETURN n.prop", resultHandler = (row, _) => {
      row.get("n.prop") should be(2)
    }) should be(1)

    // Should still be allowed write
    executeOnDefault(username, password, "CREATE (:A {prop: 3})")

    execute("MATCH (n) RETURN n.prop").toSet should be(Set(Map("n.prop" -> 1), Map("n.prop" -> 2), Map("n.prop" -> 3)))
  }
}
