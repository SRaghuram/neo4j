/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.cypher.DatabaseManagementException
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

import scala.collection.Map

class PrivilegeDDLAcceptanceTest extends DDLAcceptanceTestBase {

  // Tests for showing privileges

  test("should show privileges for users") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("should show all privileges") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    execute("SHOW ALL PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("should fail when showing privileges for all users when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW ALL PRIVILEGES")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: SHOW PRIVILEGE"
  }

  test("should show privileges for specific role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ROLE editor PRIVILEGES")

    // THEN
    val expected = Set(
      grantGraph().role("editor").action("find").node("*").map,
      grantGraph().role("editor").action("read").node("*").map,
      grantGraph().role("editor").action("write").node("*").map,
      grantGraph().role("editor").action("find").relationship("*").map
    )

    result.toSet should be(expected)
  }

  test("should give nothing when showing privileges for non-existing role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val resultFoo = execute("SHOW ROLE foo PRIVILEGES")

    // THEN
    resultFoo.toSet should be(Set.empty)

    // and an invalid (non-existing) one
    // WHEN
    val resultEmpty = execute("SHOW ROLE `` PRIVILEGES")

    // THEN
    resultEmpty.toSet should be(Set.empty)
  }

  test("should fail when showing privileges for roles when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW ROLE editor PRIVILEGES")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: SHOW PRIVILEGE"
  }

  test("should show privileges for specific user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW USER neo4j PRIVILEGES")

    // THEN
    val expected = Set(
      grantGraph().role("admin").user("neo4j").action("find").node("*").map,
      grantGraph().role("admin").user("neo4j").action("read").node("*").map,
      grantGraph().role("admin").user("neo4j").action("write").node("*").map,
      grantSystem().role("admin").user("neo4j").action("write").node("*").map,
      grantToken().role("admin").user("neo4j").action("write").node("*").map,
      grantSchema().role("admin").user("neo4j").action("write").node("*").map,
      grantGraph().role("admin").user("neo4j").action("find").relationship("*").map
    )

    result.toSet should be(expected)
  }

  test("should give nothing when showing privileges for non-existing user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val resultFoo = execute("SHOW USER foo PRIVILEGES")

    // THEN
    resultFoo.toSet should be(Set.empty)

    // and an invalid (non-existing) one
    // WHEN
    val resultEmpty = execute("SHOW USER `` PRIVILEGES")

    // THEN
    resultEmpty.toSet should be(Set.empty)
  }

  test("should fail when showing privileges for users when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW USER neo4j PRIVILEGES")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: SHOW PRIVILEGE"
  }

  // Tests for granting traverse privileges

  test("should grant traversal privilege to custom role for all databases and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").map))
  }

  test("should grant traversal privilege to custom role for all databases and all relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").relationship("*").map))
  }

  test("should grant traversal privilege to custom role for all databases and all element types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map, grantTraverse().role("custom").relationship("*").map))
  }

  test("should fail granting traversal privilege for all databases and all labels to non-existing role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
      // THEN
    } should have message "Role 'custom' does not exist."

    // WHEN
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * (*) TO custom")
      // THEN
    } should have message "Role 'custom' does not exist."

    // WHEN
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT TRAVERSE ON GRAPH * TO custom")
      // THEN
    } should have message "Role 'custom' does not exist."

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set())
  }

  test("should grant traversal privilege to custom role for all databases but only a specific label (that does not need to exist)") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").node("A").map))
  }

  test("should grant traversal privilege to custom role for all databases but only a specific relationship type (that does not need to exist)") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").relationship("A").map))
  }

  test("should grant traversal privilege to custom role for a specific database and a specific label") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").database("foo").node("A").map))
  }

  test("should grant traversal privilege to custom role for a specific database and a specific relationship type") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").database("foo").relationship("A").map))
  }

  test("should grant traversal privilege to custom role for a specific database and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").database("foo").map))
  }

  test("should grant traversal privilege to custom role for a specific database and all relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").database("foo").relationship("*").map))
  }

  test("should grant traversal privilege to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").node("A").map,
      grantTraverse().role("custom").database("foo").node("B").map
    ))
  }

  test("should grant traversal privilege to custom role for a specific database and multiple relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").relationship("A").map,
      grantTraverse().role("custom").database("foo").relationship("B").map
    ))
  }

  test("should grant traversal privilege to custom role for a specific database and multiple labels in one grant") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo NODES A, B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").node("A").map,
      grantTraverse().role("custom").database("foo").node("B").map
    ))
  }

  test("should grant traversal privilege to custom role for a specific database and multiple relationship types in one grant") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS A, B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").relationship("A").map,
      grantTraverse().role("custom").database("foo").relationship("B").map
    ))
  }

  test("should grant traversal privilege to multiple roles for a specific database and multiple labels in one grant") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo NODES A, B (*) TO role1, role2")

    // THEN
    execute("SHOW ROLE role1 PRIVILEGES").toSet should be(Set(
      grantTraverse().role("role1").database("foo").node("A").map,
      grantTraverse().role("role1").database("foo").node("B").map
    ))
    execute("SHOW ROLE role2 PRIVILEGES").toSet should be(Set(
      grantTraverse().role("role2").database("foo").node("A").map,
      grantTraverse().role("role2").database("foo").node("B").map
    ))
  }

  test("should grant traversal privilege to multiple roles for a specific database and multiple relationship types in one grant") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS A, B (*) TO role1, role2")

    // THEN
    execute("SHOW ROLE role1 PRIVILEGES").toSet should be(Set(
      grantTraverse().role("role1").database("foo").relationship("A").map,
      grantTraverse().role("role1").database("foo").relationship("B").map
    ))
    execute("SHOW ROLE role2 PRIVILEGES").toSet should be(Set(
      grantTraverse().role("role2").database("foo").relationship("A").map,
      grantTraverse().role("role2").database("foo").relationship("B").map
    ))
  }

  test("should fail when granting traversal privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    the [DatabaseNotFoundException] thrownBy {
      execute("GRANT TRAVERSE ON GRAPH foo TO custom")
    } should have message "Database 'foo' does not exist."
  }

  test("should fail when granting traversal privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("GRANT TRAVERSE ON GRAPH * TO custom")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: GRANT TRAVERSE"
  }

  // Tests for granting read privileges

  test("should grant read privilege to custom role for all databases and all elements") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (*) ON GRAPH * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").resource("all_properties").node("*").map,
      grantRead().role("custom").resource("all_properties").relationship("*").map
    ))
  }

  test("should grant read privilege to custom role for all databases and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").resource("all_properties").map))
  }

  test("should grant read privilege to custom role for all databases and all relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").resource("all_properties").relationship("*").map))
  }

  test("should fail granting read privilege for all databases and all elements to non-existing role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT READ (*) ON GRAPH * TO custom")
      // THEN
    } should have message "Role 'custom' does not exist."

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set())
  }

  test("should grant read privilege to custom role for all databases but only a specific label") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (*) ON GRAPH * NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").resource("all_properties").node("A").map))
  }

  test("should grant read privilege to custom role for all databases but only a specific relationship type") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (*) ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").resource("all_properties").relationship("A").map))
  }

  test("should grant read privilege to custom role for a specific database and a specific label") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (*) ON GRAPH foo NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").database("foo").resource("all_properties").node("A").map))
  }

  test("should grant read privilege to custom role for a specific database and a specific relationship type") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (*) ON GRAPH foo RELATIONSHIP A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").database("foo").resource("all_properties").relationship("A").map))
  }

  test("should grant read privilege to custom role for a specific database and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (*) ON GRAPH foo NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").database("foo").resource("all_properties").map))
  }

  test("should grant read privilege to custom role for a specific database and all relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (*) ON GRAPH foo RELATIONSHIPS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").database("foo").resource("all_properties").relationship("*").map))
  }

  test("should grant read privilege to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (*) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (*) ON GRAPH foo NODES B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").database("foo").resource("all_properties").node("A").map,
      grantRead().role("custom").database("foo").resource("all_properties").node("B").map
    ))
  }

  test("should grant read privilege to custom role for a specific database and multiple relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (*) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT READ (*) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").database("foo").resource("all_properties").relationship("A").map,
      grantRead().role("custom").database("foo").resource("all_properties").relationship("B").map
    ))
  }

  test("should fail when granting read privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    the [DatabaseNotFoundException] thrownBy {
      execute("GRANT READ (*) ON GRAPH foo TO custom")
    } should have message "Database 'foo' does not exist."
  }

  test("should grant read privilege for specific property to custom role for all databases and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH * NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").property("bar").map))
  }

  test("should grant read privilege for specific property to custom role for all databases and all relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH * RELATIONSHIPS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").property("bar").relationship("*").map))
  }

  test("should grant read privilege for specific property to custom role for all databases but only a specific label") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH * NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").property("bar").node("A").map))
  }

  test("should grant read privilege for specific property to custom role for all databases but only a specific relationship type") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").property("bar").relationship("A").map))
  }

  test("should grant read privilege for specific property to custom role for a specific database and a specific label") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().database("foo").role("custom").property("bar").node("A").map))
  }

  test("should grant read privilege for specific property to custom role for a specific database and a specific relationship type") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().database("foo").role("custom").property("bar").relationship("A").map))
  }

  test("should grant read privilege for specific property to custom role for a specific database and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().database("foo").role("custom").property("bar").map))
  }

  test("should grant read privilege for specific property to custom role for a specific database and all relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().database("foo").role("custom").property("bar").relationship("*").map))
  }

  test("should grant read privilege for specific property to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))
  }

  test("should grant read privilege for specific property to custom role for a specific database and multiple relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))
  }

  test("should grant read privilege for multiple properties to custom role for a specific database and specific label") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (baz) ON GRAPH foo NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("baz").node("A").map
    ))
  }

  test("should grant read privilege for multiple properties to custom role for a specific database and specific relationship type") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT READ (baz) ON GRAPH foo RELATIONSHIPS A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("baz").relationship("A").map
    ))
  }

  test("should grant read privilege for multiple properties to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (baz) ON GRAPH foo NODES B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("baz").node("B").map
    ))
  }

  test("should grant read privilege for multiple properties to custom role for a specific database and multiple relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT READ (baz) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("baz").relationship("B").map
    ))
  }

  test("should grant read privilege for multiple properties to multiple roles for a specific database and multiple labels in a single grant") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (a, b, c) ON GRAPH foo NODES A, B, C (*) TO role1, role2, role3")

    // THEN
    val expected = for (p <- Seq("a", "b", "c"); l <- Seq("A", "B", "C")) yield {
      grantRead().database("foo").property(p).node(l)
    }
    execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
    execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
    execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
  }

  test("should grant read privilege for multiple properties to multiple roles for a specific database and multiple relationship types in a single grant") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (a, b, c) ON GRAPH foo RELATIONSHIPS A, B, C (*) TO role1, role2, role3")

    // THEN
    val expected = for (p <- Seq("a", "b", "c"); l <- Seq("A", "B", "C")) yield {
      grantRead().database("foo").property(p).relationship(l)
    }
    execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
    execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
    execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
  }

  test("should fail when granting read privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("GRANT READ (*) ON GRAPH * TO custom")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: GRANT READ"
  }

  // Tests for GRANT MATCH privileges

  test("should grant MATCH privilege to custom role for all databases and all elements") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MATCH (*) ON GRAPH * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").map,
      grantRead().role("custom").resource("all_properties").node("*").map,
      grantRead().role("custom").resource("all_properties").relationship("*").map
    ))
  }

  test("should grant MATCH privilege to custom role for all databases and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MATCH (*) ON GRAPH * NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").map,
      grantRead().role("custom").resource("all_properties").map
    ))
  }

  test("should grant MATCH privilege to custom role for all databases and all relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MATCH (*) ON GRAPH * RELATIONSHIPS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").relationship("*").map,
      grantRead().role("custom").relationship("*").resource("all_properties").map
    ))
  }

  test("should grant MATCH privilege to custom role for all databases but only a specific label") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MATCH (*) ON GRAPH * NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("A").map,
      grantRead().role("custom").resource("all_properties").node("A").map
    ))
  }

  test("should grant MATCH privilege to custom role for all databases but only a specific relationship type") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MATCH (*) ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").relationship("A").map,
      grantRead().role("custom").resource("all_properties").relationship("A").map
    ))
  }

  test("should grant MATCH privilege to custom role for a specific database and a specific label") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (*) ON GRAPH foo NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").node("A").map,
      grantRead().role("custom").database("foo").resource("all_properties").node("A").map
    ))
  }

  test("should grant MATCH privilege to custom role for a specific database and a specific relationship type") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (*) ON GRAPH foo RELATIONSHIPS A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").relationship("A").map,
      grantRead().role("custom").database("foo").resource("all_properties").relationship("A").map
    ))
  }

  test("should grant MATCH privilege to custom role for a specific database and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (*) ON GRAPH foo NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").map,
      grantRead().role("custom").database("foo").resource("all_properties").map
    ))
  }

  test("should grant MATCH privilege to custom role for a specific database and all relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (*) ON GRAPH foo RELATIONSHIPS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").relationship("*").map,
      grantRead().role("custom").database("foo").relationship("*").resource("all_properties").map
    ))
  }

  test("should grant MATCH privilege to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (*) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT MATCH (*) ON GRAPH foo NODES B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").node("A").map,
      grantTraverse().role("custom").database("foo").node("B").map,
      grantRead().role("custom").database("foo").resource("all_properties").node("A").map,
      grantRead().role("custom").database("foo").resource("all_properties").node("B").map
    ))
  }

  test("should grant MATCH privilege to custom role for a specific database and multiple relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (*) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT MATCH (*) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").relationship("A").map,
      grantTraverse().role("custom").database("foo").relationship("B").map,
      grantRead().role("custom").database("foo").resource("all_properties").relationship("A").map,
      grantRead().role("custom").database("foo").resource("all_properties").relationship("B").map
    ))
  }

  test("should fail grant MATCH privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    the [DatabaseNotFoundException] thrownBy {
      execute("GRANT MATCH (*) ON GRAPH foo TO custom")
    } should have message "Database 'foo' does not exist."
  }

  test("should grant MATCH privilege for specific property to custom role for all databases and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH * NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").map,
      grantRead().role("custom").property("bar").map
    ))
  }

  test("should grant MATCH privilege for specific property to custom role for all databases and all relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH * RELATIONSHIPS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").relationship("*").map,
      grantRead().role("custom").relationship("*").property("bar").map
    ))
  }

  test("should grant MATCH privilege for specific property to custom role for all databases but only a specific label") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH * NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("A").map,
      grantRead().role("custom").property("bar").node("A").map
    ))
  }

  test("should grant MATCH privilege for specific property to custom role for all databases but only a specific relationship type") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH * RELATIONSHIPS A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").relationship("A").map,
      grantRead().role("custom").property("bar").relationship("A").map
    ))
  }

  test("should grant MATCH privilege for specific property to custom role for a specific database and a specific label") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH foo NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map
    ))
  }

  test("should grant MATCH privilege for specific property to custom role for a specific database and a specific relationship type") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map
    ))
  }

  test("should grant MATCH privilege for specific property to custom role for a specific database and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH foo NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").map,
      grantRead().database("foo").role("custom").property("bar").map
    ))
  }

  test("should grant MATCH privilege for specific property to custom role for a specific database and all relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH foo RELATIONSHIPS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("bar").map
    ))
  }

  test("should grant MATCH privilege for specific property to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT MATCH (bar) ON GRAPH foo NODES B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("A").map,
      grantTraverse().database("foo").role("custom").node("B").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))
  }

  test("should grant MATCH privilege for specific property to custom role for a specific database and multiple relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT MATCH (bar) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))
  }

  test("should grant MATCH privilege for multiple properties to custom role for a specific database and specific label") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT MATCH (baz) ON GRAPH foo NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("baz").node("A").map
    ))
  }

  test("should grant MATCH privilege for multiple properties to custom role for a specific database and specific relationship type") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT MATCH (baz) ON GRAPH foo RELATIONSHIPS A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("baz").relationship("A").map
    ))
  }

  test("should grant MATCH privilege for multiple properties to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT MATCH (baz) ON GRAPH foo NODES B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("A").map,
      grantTraverse().database("foo").role("custom").node("B").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("baz").node("B").map
    ))
  }

  test("should grant MATCH privilege for multiple properties to custom role for a specific database and multiple relationship types") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT MATCH (baz) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("baz").relationship("B").map
    ))
  }

  test("should grant MATCH privilege for multiple properties to multiple roles for a specific database and multiple labels in a single grant") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (a, b, c) ON GRAPH foo NODES A, B, C (*) TO role1, role2, role3")

    // THEN
    val expected = (for (l <- Seq("A", "B", "C")) yield {
      (for(p <- Seq("a", "b", "c")) yield {
        grantRead().database("foo").property(p).node(l)
      }) :+ grantTraverse().database("foo").node(l)
    }).flatten
    execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
    execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
    execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
  }

  test("should grant MATCH privilege for multiple properties to multiple roles for a specific database and multiple relationship types in a single grant") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MATCH (a, b, c) ON GRAPH foo RELATIONSHIPS A, B, C (*) TO role1, role2, role3")

    // THEN
    val expected = (for (l <- Seq("A", "B", "C")) yield {
      (for(p <- Seq("a", "b", "c")) yield {
        grantRead().database("foo").property(p).relationship(l)
      }) :+ grantTraverse().database("foo").relationship(l)
    }).flatten
    execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
    execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
    execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
  }

  test("should fail when granting MATCH privilege when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("GRANT MATCH (*) ON GRAPH * TO custom")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: GRANT MATCH"
  }


  // Tests for granting write privileges

  test("should grant write privilege to custom role for all databases and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantWrite().role("custom").resource("all_properties").map,
      grantWrite().role("custom").resource("all_properties").relationship("*").map
    ))
  }

  test("should grant write privilege to custom role for a specific database and all labels") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT WRITE (*) ON GRAPH foo ELEMENTS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantWrite().role("custom").database("foo").resource("all_properties").map,
      grantWrite().role("custom").database("foo").resource("all_properties").relationship("*").map
    ))
  }

  test("should grant write privilege to multiple roles in a single grant") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT WRITE (*) ON GRAPH foo ELEMENTS * (*) TO role1, role2, role3")

    // THEN
    val expected: Seq[PrivilegeMapBuilder] = Seq(
      grantWrite().database("foo").resource("all_properties").node("*"),
      grantWrite().database("foo").resource("all_properties").relationship("*")
    )

    execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
    execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
    execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
  }

  test("should fail granting write privilege for all databases and all labels to non-existing role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")
      // THEN
    } should have message "Role 'custom' does not exist."

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set())
  }

  test("should fail when granting write privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    the [DatabaseNotFoundException] thrownBy {
      execute("GRANT WRITE (*) ON GRAPH foo ELEMENTS * (*) TO custom")
    } should have message "Database 'foo' does not exist."
  }

  test("should fail when granting write privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")
      // THEN
    } should have message "This is a DDL command and it should be executed against the system database: GRANT WRITE"
  }

  // Tests for REVOKE READ, TRAVERSE, MATCH and WRITE

  test("should revoke correct read privilege different label qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT READ (bar) ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES B (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS * (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").relationship("*").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map,
      grantRead().database("foo").role("custom").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))

    // WHEN
    execute("REVOKE READ (bar) ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").relationship("*").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map,
      grantRead().database("foo").role("custom").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))

    // WHEN
    execute("REVOKE READ (bar) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").relationship("*").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))
  }

  test("should revoke correct read privilege different relationship type qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT READ (bar) ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES B (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS * (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map,
      grantRead().database("foo").role("custom").property("bar").relationship("*").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))

    // WHEN
    execute("REVOKE READ (bar) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map,
      grantRead().database("foo").role("custom").property("bar").relationship("*").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))

    // WHEN
    execute("REVOKE READ (bar) ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))
  }

  test("should revoke correct read privilege different property") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT READ (*) ON GRAPH foo TO custom")
    execute("GRANT READ (a) ON GRAPH foo TO custom")
    execute("GRANT READ (b) ON GRAPH foo TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").node("*").property("a").map,
      grantRead().database("foo").role("custom").node("*").property("b").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").map
    ))

    // WHEN
    execute("REVOKE READ (a) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").node("*").property("b").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").map
    ))

    // WHEN
    execute("REVOKE READ (*) ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").node("*").property("b").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map
    ))

    // WHEN
    execute("REVOKE READ (b) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map
    ))
  }

  test("should revoke correct read privilege different databases") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT READ (*) ON GRAPH * TO custom")
    execute("GRANT READ (*) ON GRAPH foo TO custom")
    execute("GRANT READ (*) ON GRAPH bar TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").map,
      grantRead().role("custom").database("foo").map,
      grantRead().role("custom").database("bar").map,
      grantRead().role("custom").relationship("*").map,
      grantRead().role("custom").relationship("*").database("foo").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE READ (*) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").map,
      grantRead().role("custom").database("bar").map,
      grantRead().role("custom").relationship("*").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE READ (*) ON GRAPH * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").database("bar").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))
  }

  test("should revoke correct traverse node privilege different label qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").node("A").map,
      grantTraverse().database("foo").role("custom").node("B").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").node("B").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").node("B").map
    ))
  }

  test("should revoke correct traverse node privilege different databases") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH bar NODES * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").database("foo").map,
      grantTraverse().role("custom").database("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").database("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").database("bar").map
    ))
  }

  test("should revoke correct traverse relationships privilege different type qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").relationship("B").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("B").map
    ))
  }

  test("should revoke correct traverse relationship privilege different databases") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH bar RELATIONSHIPS * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").relationship("*").database("foo").map,
      grantTraverse().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH * RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").database("bar").map
    ))
  }

  test("should revoke correct MATCH privilege different label qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT MATCH (bar) ON GRAPH foo TO custom")
    execute("GRANT MATCH (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT MATCH (bar) ON GRAPH foo NODES B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").map,
      grantRead().database("foo").role("custom").property("bar").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("bar").map,
      grantTraverse().database("foo").role("custom").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantTraverse().database("foo").role("custom").node("B").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))

    // WHEN
    execute("REVOKE MATCH (bar) ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").map,
      grantRead().database("foo").role("custom").property("bar").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("bar").map,
      grantTraverse().database("foo").role("custom").node("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().database("foo").role("custom").node("B").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))

    // WHEN
    execute("REVOKE MATCH (bar) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("bar").map,
      grantTraverse().database("foo").role("custom").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().database("foo").role("custom").node("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().database("foo").role("custom").node("B").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))
  }

  test("should revoke correct MATCH privilege different relationship type qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT MATCH (bar) ON GRAPH foo TO custom")
    execute("GRANT MATCH (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT MATCH (bar) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").node("*").property("bar").map,
      grantRead().database("foo").role("custom").relationship("*").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))

    // WHEN
    execute("REVOKE MATCH (bar) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").node("*").property("bar").map,
      grantRead().database("foo").role("custom").relationship("*").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))

    // WHEN
    execute("REVOKE MATCH (bar) ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").node("*").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))
  }

  test("should revoke correct MATCH privilege different property") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT MATCH (*) ON GRAPH foo TO custom")
    execute("GRANT MATCH (a) ON GRAPH foo TO custom")
    execute("GRANT MATCH (b) ON GRAPH foo TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").map,
      grantRead().database("foo").role("custom").property("a").map,
      grantRead().database("foo").role("custom").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map
    ))

    // WHEN
    execute("REVOKE MATCH (a) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").map,
      grantRead().database("foo").role("custom").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map
    ))

    // WHEN
    execute("REVOKE MATCH (*) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map
    ))

    // WHEN
    execute("REVOKE MATCH (b) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      // TODO: this should be an empty set
      grantTraverse().database("foo").role("custom").map,
      grantTraverse().database("foo").role("custom").relationship("*").map
    ))
  }

  test("should revoke correct MATCH privilege different databases") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT MATCH (*) ON GRAPH * TO custom")
    execute("GRANT MATCH (*) ON GRAPH foo TO custom")
    execute("GRANT MATCH (*) ON GRAPH bar TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").map,
      grantTraverse().role("custom").database("foo").map,
      grantTraverse().role("custom").database("bar").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").relationship("*").database("foo").map,
      grantTraverse().role("custom").relationship("*").database("bar").map,
      grantRead().role("custom").map,
      grantRead().role("custom").database("foo").map,
      grantRead().role("custom").database("bar").map,
      grantRead().role("custom").relationship("*").map,
      grantRead().role("custom").relationship("*").database("foo").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE MATCH (*) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").map,
      grantTraverse().role("custom").database("foo").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().role("custom").database("bar").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").relationship("*").database("foo").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().role("custom").relationship("*").database("bar").map,
      grantRead().role("custom").map,
      grantRead().role("custom").database("bar").map,
      grantRead().role("custom").relationship("*").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE MATCH (*) ON GRAPH * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().role("custom").database("foo").map,
      grantTraverse().role("custom").database("bar").map,
      grantTraverse().role("custom").relationship("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().role("custom").relationship("*").database("foo").map,
      grantTraverse().role("custom").relationship("*").database("bar").map,
      grantRead().role("custom").database("bar").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))
  }

  test("should revoke correct write privilege different databases") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")
    execute("GRANT WRITE (*) ON GRAPH foo ELEMENTS * (*) TO custom")
    execute("GRANT WRITE (*) ON GRAPH bar ELEMENTS * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantWrite().role("custom").map,
      grantWrite().role("custom").database("foo").map,
      grantWrite().role("custom").database("bar").map,
      grantWrite().role("custom").relationship("*").map,
      grantWrite().role("custom").relationship("*").database("foo").map,
      grantWrite().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE WRITE (*) ON GRAPH foo ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantWrite().role("custom").map,
      grantWrite().role("custom").database("bar").map,
      grantWrite().role("custom").relationship("*").map,
      grantWrite().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE WRITE (*) ON GRAPH * ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantWrite().role("custom").database("bar").map,
      grantWrite().role("custom").relationship("*").database("bar").map
    ))
  }

  test("should revoke correct traverse and read privileges from different MATCH privileges") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT MATCH (foo,bar) ON GRAPH foo NODES A,B (*) TO custom")
    execute("GRANT MATCH (foo,bar) ON GRAPH foo RELATIONSHIPS A,B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("A").map,
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").node("A").property("foo").map,
      grantRead().database("foo").role("custom").node("B").property("foo").map,
      grantRead().database("foo").role("custom").relationship("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").node("B").property("bar").map,
      grantRead().database("foo").role("custom").relationship("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").node("A").property("foo").map,
      grantRead().database("foo").role("custom").node("B").property("foo").map,
      grantRead().database("foo").role("custom").relationship("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").node("B").property("bar").map,
      grantRead().database("foo").role("custom").relationship("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo RELATIONSHIPS B (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").node("A").property("foo").map,
      grantRead().database("foo").role("custom").node("B").property("foo").map,
      grantRead().database("foo").role("custom").relationship("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").node("B").property("bar").map,
      grantRead().database("foo").role("custom").relationship("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE READ (foo,bar) ON GRAPH foo NODES B (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").node("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE READ (foo,bar) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").node("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE READ (foo) ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE READ (foo) ON GRAPH foo RELATIONSHIPS B (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))
  }

  test("should revoke correct MATCH privilege from different traverse, read and MATCH privileges") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT MATCH (*) ON GRAPH foo TO custom")
    execute("GRANT MATCH (a) ON GRAPH foo TO custom")
    execute("GRANT READ  (b) ON GRAPH foo TO custom")

    execute("GRANT TRAVERSE  ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT MATCH (a) ON GRAPH foo NODES A (*) TO custom")

    execute("GRANT TRAVERSE  ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT MATCH (a) ON GRAPH foo RELATIONSHIPS A (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").map, // From both MATCH *
      grantTraverse().role("custom").database("foo").relationship("*").map, // From both MATCH *
      grantRead().database("foo").role("custom").map,
      grantRead().database("foo").role("custom").property("a").map,
      grantRead().database("foo").role("custom").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,

      grantTraverse().role("custom").database("foo").node("A").map, // From both MATCH and TRAVERSE
      grantTraverse().role("custom").database("foo").relationship("A").map, // From both MATCH and TRAVERSE
      grantRead().database("foo").role("custom").property("a").node("A").map,
      grantRead().database("foo").role("custom").property("a").relationship("A").map
    ))

    // WHEN
    execute("REVOKE MATCH (b) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").map,
      grantTraverse().role("custom").database("foo").relationship("*").map,
      grantRead().database("foo").role("custom").map,
      grantRead().database("foo").role("custom").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,

      grantTraverse().role("custom").database("foo").node("A").map,
      grantTraverse().role("custom").database("foo").relationship("A").map,
      grantRead().database("foo").role("custom").property("a").node("A").map,
      grantRead().database("foo").role("custom").property("a").relationship("A").map
    ))

    // WHEN
    execute("REVOKE MATCH (a) ON GRAPH foo RELATIONSHIP * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").map,
      grantTraverse().role("custom").database("foo").relationship("*").map,
      grantRead().database("foo").role("custom").map,
      grantRead().database("foo").role("custom").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,

      grantTraverse().role("custom").database("foo").node("A").map,
      grantTraverse().role("custom").database("foo").relationship("A").map,
      grantRead().database("foo").role("custom").property("a").node("A").map,
      grantRead().database("foo").role("custom").property("a").relationship("A").map
    ))

    // WHEN
    execute("REVOKE MATCH (a) ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").map,
      grantTraverse().role("custom").database("foo").relationship("*").map,
      grantRead().database("foo").role("custom").map,
      grantRead().database("foo").role("custom").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,

      grantTraverse().role("custom").database("foo").node("A").map,
      grantTraverse().role("custom").database("foo").relationship("A").map,
      grantRead().database("foo").role("custom").property("a").relationship("A").map
    ))

    // WHEN
    execute("REVOKE MATCH (a) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").map,
      grantTraverse().role("custom").database("foo").relationship("*").map,
      grantRead().database("foo").role("custom").map,
      grantRead().database("foo").role("custom").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,

      grantTraverse().role("custom").database("foo").node("A").map,
      grantTraverse().role("custom").database("foo").relationship("A").map
    ))
  }

  test("should fail revoke privilege from non-existent role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
    execute("GRANT MATCH (*) ON GRAPH * NODES A (*) TO custom")
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    val error1 = the [InvalidArgumentsException] thrownBy {
      execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM wrongRole")
    }

    // THEN
    error1.getMessage should (be("The role 'wrongRole' does not have the specified privilege: traverse ON GRAPH * NODES *.") or
      be("The role 'wrongRole' does not exist."))

    // WHEN
    val error2 = the [InvalidArgumentsException] thrownBy {
      execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM wrongRole")
    }

    // THEN
    error2.getMessage should (be("The role 'wrongRole' does not have the specified privilege: read * ON GRAPH * NODES *.") or
      be("The role 'wrongRole' does not exist."))

    // WHEN
    val error3 = the [InvalidArgumentsException] thrownBy {
      execute("REVOKE MATCH (*) ON GRAPH * NODES A (*) FROM wrongRole")
    }
    // THEN
    error3.getMessage should (include("The role 'wrongRole' does not have the specified privilege") or
      be("The role 'wrongRole' does not exist."))

    // WHEN
    val error4 = the [InvalidArgumentsException] thrownBy {
      execute("REVOKE WRITE (*) ON GRAPH * ELEMENTS * (*) FROM wrongRole")
    }

    // THEN
    error4.getMessage should (be("The role 'wrongRole' does not have the specified privilege: write * ON GRAPH * NODES *.") or
      be("The role 'wrongRole' does not exist."))
  }

  test("should fail revoke privilege not granted to role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE ROLE role")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
    execute("GRANT MATCH (*) ON GRAPH * NODES A (*) TO custom")
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    val errorTraverse = the [InvalidArgumentsException] thrownBy {
      execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM role")
    }
    // THEN
    errorTraverse.getMessage should include("The role 'role' does not have the specified privilege")

    // WHEN
    val errorRead = the [InvalidArgumentsException] thrownBy {
      execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM role")
    }
    // THEN
    errorRead.getMessage should include("The role 'role' does not have the specified privilege")

    // WHEN
    val errorMatch = the [InvalidArgumentsException] thrownBy {
      execute("REVOKE MATCH (*) ON GRAPH * NODES A (*) FROM role")
    }
    // THEN
    errorMatch.getMessage should include("The role 'role' does not have the specified privilege")

    // WHEN
    val errorWrite = the [InvalidArgumentsException] thrownBy {
      execute("REVOKE WRITE (*) ON GRAPH * ELEMENTS * (*) FROM role")
    }
    // THEN
    errorWrite.getMessage should include("The role 'role' does not have the specified privilege")
  }

  test("should fail when revoking traversal privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    the [InvalidArgumentsException] thrownBy {
      execute("REVOKE TRAVERSE ON GRAPH foo NODES * (*) FROM custom")
    } should have message "The privilege 'find  ON GRAPH foo NODES *' does not exist."
  }

  test("should fail when revoking read privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
    the [InvalidArgumentsException] thrownBy {
      execute("REVOKE READ (*) ON GRAPH foo NODES * (*) FROM custom")
    } should have message "The privilege 'read * ON GRAPH foo NODES *' does not exist."
  }

  test("should fail when revoking MATCH privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT MATCH (*) ON GRAPH * NODES * (*) TO custom")

    // WHEN
    val e = the [InvalidArgumentsException] thrownBy {
      execute("REVOKE MATCH (*) ON GRAPH foo NODES * (*) FROM custom")
    }
    // THEN
    e.getMessage should (be("The privilege 'find  ON GRAPH foo NODES *' does not exist.") or
      be("The privilege 'read * ON GRAPH foo NODES *' does not exist."))
  }

  test("should fail when revoking WRITE privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    val e = the [InvalidArgumentsException] thrownBy {
      execute("REVOKE WRITE (*) ON GRAPH foo ELEMENTS * (*) FROM custom")
    }
    // THEN
    e.getMessage should be("The privilege 'write * ON GRAPH foo NODES *' does not exist.")
  }


  test("should fail when revoking traversal privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: REVOKE TRAVERSE"
  }

  test("should fail when revoking read privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM custom")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: REVOKE READ"
  }

  test("should fail when revoking MATCH privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE MATCH (*) ON GRAPH * NODES * (*) FROM custom")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: REVOKE MATCH"
  }

  test("should fail when revoking WRITE privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE WRITE (*) ON GRAPH * ELEMENTS * (*) FROM custom")
      // THEN
    } should have message "This is a DDL command and it should be executed against the system database: REVOKE WRITE"
  }

  // Tests for actual behaviour of authorization rules for restricted users based on privileges

  test("should match nodes when granted traversal privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserJoeWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)", resultHandler = (row, _) => {
      row.get("labels(n)").asInstanceOf[util.Collection[String]] should contain("A")
    }) should be(1)
  }

  test("should read properties when granted read privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be(null)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)
  }

  test("should read properties when granted MATCH privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserJoeWithCustomRole()

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (name) ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE READ (name) ON GRAPH * NODES A (*) FROM custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be(null)
    }) should be(1)
  }

  test("should create node when granted WRITE privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserJoeWithCustomRole()

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) RETURN n.name")
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) RETURN 1 AS dummy", resultHandler = (row, _) => {
      row.get("dummy") should be(1)
    }) should be(1)

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))
  }

  test("should read you own writes when granted TRAVERSE and WRITE privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    val expected = List("b", null)

    executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) WITH n MATCH (m:A) RETURN m.name AS name ORDER BY name", resultHandler = (row, index) => {
      row.get("name") should be(expected(index))
    }) should be(2)

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))
  }

  test("should delete node when granted WRITE privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * (*) TO custom")

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n: A) WITH n, n.name as name DETACH DELETE n RETURN name")
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n: A) WITH n, n.name as name DETACH DELETE n RETURN name", resultHandler = (row, _) => {
      row.get("name") should be("a")
    }) should be(1)

    execute("MATCH (n) RETURN n.name").toSet should be(Set.empty)
  }

  test("should set and remove property when granted WRITE privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * (*) TO custom")

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a', prop: 'b'})")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n:A) SET n.name = 'b'")
    }

    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n:A) REMOVE n.prop")
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n:A) SET n.name = 'b' REMOVE n.prop") should be(0)

    execute("MATCH (n) RETURN properties(n) as props").toSet should be(Set(Map("props" -> Map("name" -> "b"))))
  }

  test("should not create new tokens, indexes or constraints when granted WRITE privilege") {
    // GIVEN
    setupUserJoeWithCustomRole()

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("b")
    }) should be(1)

    // Need token permission to create node with non-existing label
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:B) RETURN n")
    }

    // Need token permission to create node with non-existing property name
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:A {prop: 'b'}) RETURN n")
    }

    // Need schema permission to add index
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX ON :A(name)")
    }

    // Need schema permission to add constraint
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:A) ASSERT exists(n.name)")
    }
  }

  test("read privilege for node should not imply traverse privilege") {
    // GIVEN
    setupUserJoeWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)
  }

  test("read privilege for relationship should not imply traverse privilege") {
    // GIVEN
    setupUserJoeWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:REL {name:'a'}]->()")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * RELATIONSHIPS REL (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH ()-[r]-() RETURN n.name") should be(0)
  }

  test("write privilege should not imply traverse privilege") {
    // GIVEN
    setupUserJoeWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }
  }

  test("should see properties and nodes depending on granted traverse and read privileges for role") {
    // GIVEN
    setupMultilabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role1")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO role1")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role2")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO role2")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO role2")

    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO role3")

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ROLE role1 TO joe")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
    }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role1 FROM joe")
    execute("GRANT ROLE role2 TO joe")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
    }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role2 FROM joe")
    execute("GRANT ROLE role3 TO joe")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
    }) should be(2)
  }

  test("should see properties and nodes depending on granted MATCH privileges for role") {
    // GIVEN
    setupMultilabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")

    execute("GRANT MATCH (*) ON GRAPH * NODES * (*) TO role1")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role2")
    execute("GRANT MATCH (foo) ON GRAPH * NODES A (*) TO role2")
    execute("GRANT MATCH (bar) ON GRAPH * NODES B (*) TO role2")

    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO role3")

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ROLE role1 TO joe")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
    }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role1 FROM joe")
    execute("GRANT ROLE role2 TO joe")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
    }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role2 FROM joe")
    execute("GRANT ROLE role3 TO joe")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
    }) should be(2)
  }

  test("should see properties and nodes when revoking privileges for role") {
    // GIVEN
    setupMultilabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO custom")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
    }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM custom")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
    }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
    }) should be(2)
  }

  test("should find relationship when granted traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})")

    val expected = List(
      "a", "b"
    )
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, index) => {
      row.get("n.name") should be(expected(index))
    }) should be(2)

    executeOnDefault("joe", "soap", "MATCH (n)-->(m) RETURN n.name", resultHandler = (_, _) => {
      fail("should not get a match")
    }) should be(0)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")

    executeOnDefault("joe", "soap", "MATCH (n)-->(m) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)
  }

  test("should get correct count for all relationships with traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})")
    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(0)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(1)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(2)
    }) should be(1)
  }

  test("should get correct count for specific relationship with traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})")
    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(0)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(1)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (*) ON GRAPH * NODES B TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(2)
    }) should be(1)
  }

  test("should get relationships for a matched node") {

    import scala.collection.JavaConverters._
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:Start), (a)-[:A]->(), (a)-[:B]->()")

    executeOnDefault("joe", "soap", "MATCH (a:Start) RETURN a", resultHandler = (row, _) => {
      val node = row.get("a").asInstanceOf[Node]
      node.getDegree() should be(1)
      node.getRelationships().asScala.map(_.getType.name()).toSet should be(Set("A"))
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    executeOnDefault("joe", "soap", "MATCH (a:Start) RETURN a", resultHandler = (row, _) => {
      val node = row.get("a").asInstanceOf[Node]
      node.getDegree() should be(2)
      node.getRelationships().asScala.map(_.getType.name()).toSet should be(Set("A", "B"))
    }) should be(1)
  }

  test("should get relationship types and count from procedure") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    val query = "CALL db.relationshipTypes YIELD relationshipType as reltype, relationshipCount as count"

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:A), (a)-[:A]->(:A), (a)-[:B]->(:A), (a)-[:B]->(:B)")

    // WHEN ... THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("reltype") should be("A")
      row.get("count") should be(1)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS B TO custom")

    val expected1 = List(
      ("A", 1),
      ("B", 1)
    )

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("reltype"), row.get("count")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES B TO custom")

    val expected2 = List(
      ("A", 1),
      ("B", 2)
    )

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("reltype"), row.get("count")) should be(expected2(index))
    }) should be(2)
  }

  test("should only see properties on relationship with read privilege") {
    setupUserJoeWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2}]->(),
        |()-[:B {id: 3, foo: 4}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should equal(util.Collections.emptyMap())
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (id) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L),
      util.Collections.emptyMap()
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should equal(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected2 = List(
      util.Map.of("id", 1L, "foo", 2L),
      util.Map.of("foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should equal(expected2(index))
    }) should be(2)
  }

  test("should see properties and relationships depending on granted MATCH privileges for role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("GRANT READ (id) ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 1}]->(),
        |()-[:A {id: 2, bar: 1}]->(),
        |()-[:A {id: 3, foo: 0, bar: 0}]->(),
        |()-[:A {id: 4, foo: 0, bar: 1}]->(),
        |()-[:A {id: 5, foo: 1, bar: 1}]->(),
        |()-[:A {id: 6, foo: 1, bar: 0}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() WHERE r.foo = 1 OR r.bar = 1 RETURN r.id, r.foo, r.bar"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (foo) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected1 = List(
      (1, 1, null),
      (5, 1, null),
      (6, 1, null)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("r.id"), row.get("r.foo"), row.get("r.bar")) should be(expected1(index))
    }) should be(3)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (bar) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected2 = List(
      (1, 1, null),
      (2, null, 1),
      (4, 0, 1),
      (5, 1, 1),
      (6, 1, 0)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("r.id"), row.get("r.foo"), row.get("r.bar")) should be(expected2(index))
    }) should be(5)
  }

  test("should see properties and relationships depending on granted MATCH privileges for role fulltext index") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("GRANT READ (id) ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE
        |()-[:A {id: 1, foo: 'true'}]->(),
        |()-[:A {id: 2, bar: 'true'}]->(),
        |()-[:A {id: 3, foo: 'false', bar: 'false'}]->(),
        |()-[:A {id: 4, foo: 'false', bar: 'true'}]->(),
        |()-[:A {id: 5, foo: 'true', bar: 'true'}]->(),
        |()-[:A {id: 6, foo: 'true', bar: 'false'}]->()
      """.stripMargin)

    execute("CALL db.index.fulltext.createRelationshipIndex('relIndex', ['A'], ['foo', 'bar'])")
    execute("CALL db.awaitIndexes()")

    val query = "CALL db.index.fulltext.queryRelationships('relIndex', 'true') YIELD relationship AS r RETURN r.id, r.foo, r.bar ORDER BY r.id"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (foo) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (bar) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected = List(
      (1, "true", null),
      (2, null, "true"),
      (4, "false", "true"),
      (5, "true", "true"),
      (6, "true", "false")
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("r.id"), row.get("r.foo"), row.get("r.bar")) should be(expected(index))
    }) should be(5)
  }

  test("should give correct results with relationship") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE (:A)-[:A {prop: 'string with words in it'}]->(:A),
        |(:A)-[:B {prop: 'another string with words in it'}]->(:A),
        |(:C)-[:B {prop: 'this string contains words'}]->(:A),
        |(:A)-[:B {prop: 'this is just a string'}]->(:A),
        |()-[:A {prop: 'words words words'}]->(:A)
      """.stripMargin)

    val query = "MATCH ()-[r]->() WHERE r.prop contains 'words' RETURN r.prop AS prop"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    val expected1 = List(
      "string with words in it",
      "another string with words in it"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES C TO custom")

    // THEN
    val expected2 = List(
      "string with words in it",
      "another string with words in it",
      "this string contains words"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected2(index))
    }) should be(3)
  }

  test("should give correct results with relationship fulltext index") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE (:A)-[:A {prop: 'string with words in it'}]->(:A),
        |(:A)-[:B {prop: 'another string with words in it'}]->(:A),
        |(:C)-[:B {prop: 'this string contains words'}]->(:A),
        |(:A)-[:B {prop: 'this is just a string'}]->(:A),
        |()-[:A {prop: 'words words words'}]->(:A)
      """.stripMargin)

    execute("CALL db.index.fulltext.createRelationshipIndex('relIndex', ['A', 'B'], ['prop'])")
    execute("CALL db.awaitIndexes()")

    val query = "CALL db.index.fulltext.queryRelationships('relIndex', 'words') YIELD relationship RETURN relationship.prop AS prop"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    val expected1 = List(
      "string with words in it",
      "another string with words in it"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES C TO custom")

    // THEN
    val expected2 = List(
      "string with words in it",
      "another string with words in it",
      "this string contains words"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected2(index))
    }) should be(3)
  }

  test("should read properties when granted MATCH privilege to custom role for a specific database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:B {name:'b'})")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name:'a'})")
    selectDatabase(SYSTEM_DATABASE_NAME)

    setupUserJoeWithCustomRole()

    // WHEN
    execute(s"GRANT MATCH (*) ON GRAPH $DEFAULT_DATABASE_NAME NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)

    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "Read operations are not allowed for user 'joe' with roles [custom]."
  }

  test("should create nodes when granted WRITE privilege to custom role for a specific database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:B {name:'b'})")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name:'a'})")

    setupUserJoeWithCustomRole()

    // WHEN
    execute(s"GRANT WRITE (*) ON GRAPH ${DEFAULT_DATABASE_NAME} ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) RETURN 1 AS dummy", resultHandler = (row, _) => {
      row.get("dummy") should be(1)
    }) should be(1)

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))

    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "CREATE (n:B {name: 'a'}) RETURN 1 AS dummy")
    } should have message "Write operations are not allowed for user 'joe' with roles [custom]."

    selectDatabase("foo")
    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "b")))
  }

  // Tests for granting roles to users

  test("should grant role to user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER user SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom TO user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> "user"))
  }

  test("should grant roles and list users with roles") {
    // GIVEN
    // User  : Roles
    // neo4j : admin
    // Bar   : dragon, fairy
    // Baz   :
    // Zet   : fairy
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE USER Zet SET PASSWORD 'NeX'")
    execute("CREATE ROLE dragon")
    execute("CREATE ROLE fairy")

    // WHEN
    execute("GRANT ROLE dragon TO Bar")
    execute("GRANT ROLE fairy TO Bar")
    execute("GRANT ROLE fairy TO Zet")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet shouldBe Set(
      neo4jUser,
      user("Bar", Seq("fairy", "dragon")),
      user("Baz"),
      user("Zet", Seq("fairy"))
    )
  }

  test("should grant role to several users") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom TO userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> "userA")
      + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> "userB"))
  }

  test("should grant multiple roles to user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom1, custom2 TO userA")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom1", "isBuiltIn" -> false, "member" -> "userA")
      + Map("role" -> "custom2", "isBuiltIn" -> false, "member" -> "userA"))
  }

  test("should grant multiple roles to several users") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom1, custom2 TO userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers
      + Map("role" -> "custom1", "isBuiltIn" -> false, "member" -> "userA")
      + Map("role" -> "custom1", "isBuiltIn" -> false, "member" -> "userB")
      + Map("role" -> "custom2", "isBuiltIn" -> false, "member" -> "userA")
      + Map("role" -> "custom2", "isBuiltIn" -> false, "member" -> "userB"))
  }

  test("should be able to grant already granted role to user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE ROLE dragon")
    execute("GRANT ROLE dragon TO Bar")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar", Seq("dragon")))

    // WHEN
    execute("GRANT ROLE dragon TO Bar")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar", Seq("dragon")))
  }

  test("should fail when granting non-existing role to user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar"))
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message "Cannot grant non-existent role 'dragon' to user 'Bar'"

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar"))
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE `` TO Bar")
      // THEN
    } should have message "Cannot grant non-existent role '' to user 'Bar'"

    // AND
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar"))
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers
  }

  test("should fail when granting role to non-existing user") {
    // GIVEN
    val rolesWithUsers = defaultRolesWithUsers ++ Set(Map("role" -> "dragon", "isBuiltIn" -> false, "member" -> null))
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE dragon")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe rolesWithUsers

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message "Cannot grant role 'dragon' to non-existent user 'Bar'"

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe rolesWithUsers

    // and an invalid (non-existing) one

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO ``")
      // THEN
    } should have message "Cannot grant role 'dragon' to non-existent user ''"

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe rolesWithUsers
  }

  test("should fail when granting non-existing role to non-existing user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message "Cannot grant non-existent role 'dragon' to user 'Bar'"

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers

    // and an invalid (non-existing) ones
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE `` TO ``")
      // THEN
    } should have message "Cannot grant non-existent role '' to user ''"

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers
  }

  test("should fail when granting role to user when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: GRANT ROLE"

    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE ROLE dragon")
    selectDatabase(DEFAULT_DATABASE_NAME)

    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: GRANT ROLE"
  }

  // Tests for revoking roles from users

  test("should revoke role from user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER user SET PASSWORD 'neo'")
    execute("GRANT ROLE custom TO user")

    // WHEN
    execute("REVOKE ROLE custom FROM user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> null))
  }

  test("should revoke role from several users") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")
    execute("GRANT ROLE custom TO userA")
    execute("GRANT ROLE custom TO userB")

    // WHEN
    execute("REVOKE ROLE custom FROM userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> null))
  }

  test("should revoke multiple roles from user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("GRANT ROLE custom1 TO userA")
    execute("GRANT ROLE custom2 TO userA")

    // WHEN
    execute("REVOKE ROLE custom1, custom2 FROM userA")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom1", "isBuiltIn" -> false, "member" -> null)
      + Map("role" -> "custom2", "isBuiltIn" -> false, "member" -> null))
  }

  test("should revoke multiple roles from several users") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")
    execute("CREATE USER userC SET PASSWORD 'neo'")
    execute("CREATE USER userD SET PASSWORD 'neo'")
    execute("GRANT ROLE custom1 TO userA")
    execute("GRANT ROLE custom1 TO userB")
    execute("GRANT ROLE custom1 TO userC")
    execute("GRANT ROLE custom2 TO userA")
    execute("GRANT ROLE custom2 TO userB")
    execute("GRANT ROLE custom2 TO userD")

    // WHEN
    execute("REVOKE ROLE custom1, custom2 FROM userA, userB, userC, userD")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom1", "isBuiltIn" -> false, "member" -> null)
      + Map("role" -> "custom2", "isBuiltIn" -> false, "member" -> null))
  }

  test("should be able to revoke already revoked role from user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER user SET PASSWORD 'neo'")
    execute("GRANT ROLE custom TO user")
    execute("REVOKE ROLE custom FROM user")

    // WHEN
    execute("REVOKE ROLE custom FROM user").toSet should be(Set.empty)

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> null))
  }

  test("should grant and revoke multiple roles to multiple users") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE ROLE foo")
    execute("CREATE ROLE fum")
    val admin = Map("role" -> PredefinedRoles.ADMIN, "isBuiltIn" -> true, "member" -> "neo4j")
    def role(r: String, u: String) = Map("role" -> r, "member" -> u, "isBuiltIn" -> false)

    // WHEN using single user and role version of GRANT
    execute("GRANT ROLE foo TO Bar")
    execute("GRANT ROLE foo TO Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin, role("foo", "Bar"), role("foo", "Baz")))

    // WHEN using single user and role version of REVOKE
    execute("REVOKE ROLE foo FROM Bar")
    execute("REVOKE ROLE foo FROM Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin))

    // WHEN granting with multiple users and roles version
    execute("GRANT ROLE foo, fum TO Bar, Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin, role("foo", "Bar"), role("foo", "Baz"), role("fum", "Bar"), role("fum", "Baz")))

    // WHEN revoking only one of many
    execute("REVOKE ROLE foo FROM Bar")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin, role("foo", "Baz"), role("fum", "Bar"), role("fum", "Baz")))

    // WHEN revoking with multiple users and roles version
    execute("REVOKE ROLE foo, fum FROM Bar, Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin))
  }

  test("should fail revoking non-existent role from (existing) user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER user SET PASSWORD 'neo'")

    the [InvalidArgumentsException] thrownBy {
      // WHEN
      execute("REVOKE ROLE custom FROM user")
      // THEN
    } should have message "Cannot revoke non-existent role 'custom' from user 'user'"

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers)
  }

  test("should fail revoking role from non-existing user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    the [InvalidArgumentsException] thrownBy {
      // WHEN
      execute("REVOKE ROLE custom FROM user")
      // THEN
    } should have message "Cannot revoke role 'custom' from non-existent user 'user'"

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> null))
  }

  test("should fail revoking non-existing role from non-existing user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("REVOKE ROLE custom FROM user")
      // THEN
    } should have message "Cannot revoke non-existent role 'custom' from user 'user'"

    // AND
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers)
  }

  test("should fail when revoking role from user when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE ROLE dragon FROM Bar")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: REVOKE ROLE"

    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE ROLE dragon")
    execute("GRANT ROLE dragon TO Bar")
    selectDatabase(DEFAULT_DATABASE_NAME)

    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE ROLE dragon FROM Bar")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: REVOKE ROLE"
  }

  // helper variable, methods and class

  private def setupUserJoeWithCustomRole(): Unit = {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE custom")
    execute("GRANT ROLE custom TO joe")
  }

  private def setupMultilabelData = {
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {foo:1, bar:2})")
    execute("CREATE (n:B {foo:3, bar:4})")
    execute("CREATE (n:A:B {foo:5, bar:6})")
    execute("CREATE (n {foo:7, bar:8})")
  }
}
