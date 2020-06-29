/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.security.AuthorizationViolationException

class AllGraphPrivilegesAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase with EnterpriseComponentVersionTestSupport {

  Seq(
    ("grant", granted: privilegeFunction),
    ("deny", denied: privilegeFunction),
  ).foreach {
    case (grantOrDeny, grantedOrDenied: privilegeFunction) =>
      val grantOrDenyCommand = grantOrDeny.toUpperCase

      test(s"should $grantOrDeny all graph privileges for all graphs") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDenyCommand ALL GRAPH PRIVILEGES ON GRAPH * TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantedOrDenied(allGraphPrivileges).role("custom").map))
      }

      test(s"should $grantOrDeny all graph privileges for specific graph") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")

        // WHEN
        execute(s"$grantOrDenyCommand ALL GRAPH PRIVILEGES ON GRAPH foo TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantedOrDenied(allGraphPrivileges).role("custom").database("foo").map))
      }

      test(s"should $grantOrDeny all graph privileges for multiple graphs using parameter") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")

        // WHEN
        execute(s"$grantOrDenyCommand ALL GRAPH PRIVILEGES ON GRAPH foo, $$graph TO custom", Map("graph" -> "bar"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(allGraphPrivileges).role("custom").database("foo").map,
          grantedOrDenied(allGraphPrivileges).role("custom").database("bar").map
        ))
      }
  }

  test("should not revoke other graph privileges when revoking all graph privileges") {
    // GIVEN
    execute("CREATE ROLE custom AS COPY of editor")
    execute("CREATE DATABASE foo")

    execute("GRANT TRAVERSE ON GRAPH foo TO custom")
    execute("GRANT READ {prop} ON GRAPH * NODE A TO custom")
    execute("GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO custom")

    // WHEN
    execute("REVOKE ALL GRAPH PRIVILEGES ON GRAPH * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(access).role("custom").map,
      granted(matchPrivilege).node("*").role("custom").map,
      granted(matchPrivilege).relationship("*").role("custom").map,
      granted(write).node("*").role("custom").map,
      granted(write).relationship("*").role("custom").map,
      granted(traverse).node("*").database("foo").role("custom").map,
      granted(traverse).relationship("*").database("foo").role("custom").map,
      granted(read).node("A").property("prop").role("custom").map
    ))
  }

  test("should revoke sub-privilege even if all graph privilege exists") {
    // GIVEN
    execute("CREATE ROLE custom AS COPY of reader")
    execute("GRANT SET LABEL foo ON GRAPH * TO custom")
    execute("GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO custom")

    // WHEN
    execute("REVOKE SET LABEL foo ON GRAPH * FROM custom")
    execute("REVOKE ACCESS ON DATABASE * FROM custom")
    execute("REVOKE MATCH {*} ON GRAPH * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(granted(allGraphPrivileges).role("custom").map))
  }

  // Enforcements tests

  withAllSystemGraphVersions(unsupportedWhenNotLatest) {

    test("should be allowed to traverse and read when granted all graph privileges") {
      // GIVEN
      setupUserWithCustomRole()
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (:A {prop: 'nodeValue'})-[:R {prop: 'relValue'}]->(:B)")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO custom")

      // THEN

      // Should be allowed to traverse and read nodes
      executeOnDefault("joe", "soap", "MATCH (n:A) RETURN n.prop", resultHandler = (row, _) => {
        row.get("n.prop") should be("nodeValue")
      }) should be(1)

      // Should be allowed to traverse and read relationships
      executeOnDefault("joe", "soap", "MATCH (:A)-[r:R]->(:B) RETURN r.prop", resultHandler = (row, _) => {
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
    execute("GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO custom")

    // THEN

    // Should be allowed to create nodes and relationships
    executeOnDefault("joe", "soap", "CREATE (:A {prop:'value'})-[r:R {prop: 'relValue'}]->()")

    // Should be allowed to set labels
    executeOnDefault("joe", "soap", "MATCH (n:A) SET n:B")

    // Should be allowed to remove labels
    executeOnDefault("joe", "soap", "MATCH (n:A:B) REMOVE n:A")

    // Should be allowed to set property
    executeOnDefault("joe", "soap", "MATCH (n:B) SET n.prop = 'value2'")

    // Confirm that all writes took effect
    val result = execute("MATCH (n:B)-[r]->() RETURN n.prop, r.prop, labels(n) AS labels")
    result.toSet should be(Set(Map("n.prop" -> "value2", "r.prop" -> "relValue", "labels" -> List("B"))))
  }

  test("all graph privileges should not imply that database or dbms commands are allowed") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    execute("GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO custom")

    // THEN

    // not allowed to run database command
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT foo_constraint ON (n:Label) ASSERT exists(n.prop)")
    } should have message "Schema operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

     // not allowed to run dbms command
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("joe", "soap", "CREATE ROLE role")
    } should have message "Permission denied."
  }

  test("should not be allowed to access or change the graph via commands when denied all graph privileges") {
    // GIVEN
    setupUserWithCustomAdminRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {prop: 'nodeValue'})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY ALL GRAPH PRIVILEGES ON GRAPH * TO custom")

    // THEN

    // Should not be allowed traverse
    executeOnDefault("joe", "soap", "MATCH (n:A) RETURN n") should be(0)

    // Should not be allowed read
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.prop") should be(0)

    // Should not be allowed write
     the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE (:A {prop:'value'})")
    } should have message "Create node with labels 'A' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should not be allowed graph commands on wrong graph when granted all graph privileges on other graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute("CREATE DATABASE foo")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {prop: 'nodeValue'})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ALL GRAPH PRIVILEGES ON GRAPH foo TO custom")

    // THEN

    // Should not be allowed traverse on default database
    executeOnDefault("joe", "soap", "MATCH (n:A) RETURN n") should be(0)

    // Should not be allowed read on default database
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.prop") should be(0)

    // Should not be allowed write on default database
     the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE (:A {prop:'value'})")
    } should have message "Create node with labels 'A' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should behave correctly when granted all graph privileges but denied sub-privilege") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {prop: 1})")
    execute("CREATE (:B {prop: 2})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    // Should only be allowed to traverse :B
       executeOnDefault("joe", "soap", "MATCH (n) RETURN n.prop", resultHandler = (row, _) => {
      row.get("n.prop") should be(2)
    }) should be(1)

    // Should still be allowed write
    executeOnDefault("joe", "soap", "CREATE (:A {prop: 3})")

    execute("MATCH (n) RETURN n.prop").toSet should be(Set(Map("n.prop" -> 1), Map("n.prop" -> 2), Map("n.prop" -> 3)))
  }
}
