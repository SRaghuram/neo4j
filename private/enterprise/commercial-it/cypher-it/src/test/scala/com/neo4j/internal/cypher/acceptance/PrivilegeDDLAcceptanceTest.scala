/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util.Collection

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.DatabaseManagementException
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.server.security.auth.SecurityTestUtils

import scala.collection.Map

class PrivilegeDDLAcceptanceTest extends DDLAcceptanceTestBase {

  // Tests for showing privileges

  test("should show privileges for users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW PRIVILEGES")

    // THEN
    val expected = Set(
      grantGraph().role("reader").action("find").map,
      grantGraph().role("reader").action("read").map,

      grantGraph().role("editor").action("find").map,
      grantGraph().role("editor").action("read").map,
      grantGraph().role("editor").action("write").map,

      grantGraph().role("publisher").action("find").map,
      grantGraph().role("publisher").action("read").map,
      grantGraph().role("publisher").action("write").map,
      grantToken().role("publisher").action("write").map,

      grantGraph().role("architect").action("find").map,
      grantGraph().role("architect").action("read").map,
      grantGraph().role("architect").action("write").map,
      grantToken().role("architect").action("write").map,
      grantSchema().role("architect").action("write").map,

      grantGraph().role("admin").action("find").map,
      grantGraph().role("admin").action("read").map,
      grantGraph().role("admin").action("write").map,
      grantSystem().role("admin").action("write").map,
      grantToken().role("admin").action("write").map,
      grantSchema().role("admin").action("write").map,
    )

    result.toSet should be(expected)
  }

  test("should show all privileges") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ALL PRIVILEGES")

    // THEN
    val expected = Set(
      grantGraph().role("reader").action("find").map,
      grantGraph().role("reader").action("read").map,

      grantGraph().role("editor").action("find").map,
      grantGraph().role("editor").action("read").map,
      grantGraph().role("editor").action("write").map,

      grantGraph().role("publisher").action("find").map,
      grantGraph().role("publisher").action("read").map,
      grantGraph().role("publisher").action("write").map,
      grantToken().role("publisher").action("write").map,

      grantGraph().role("architect").action("find").map,
      grantGraph().role("architect").action("read").map,
      grantGraph().role("architect").action("write").map,
      grantToken().role("architect").action("write").map,
      grantSchema().role("architect").action("write").map,

      grantGraph().role("admin").action("find").map,
      grantGraph().role("admin").action("read").map,
      grantGraph().role("admin").action("write").map,
      grantSystem().role("admin").action("write").map,
      grantToken().role("admin").action("write").map,
      grantSchema().role("admin").action("write").map,
    )

    result.toSet should be(expected)
  }

  test("should fail when showing privileges for all users when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW ALL PRIVILEGES")
      // THEN
    } should have message "Trying to run `CATALOG SHOW PRIVILEGE` against non-system database."
  }

  test("should show privileges for specific role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ROLE editor PRIVILEGES")

    // THEN
    val expected = Set(
      grantGraph().role("editor").action("find").map,
      grantGraph().role("editor").action("read").map,
      grantGraph().role("editor").action("write").map
    )

    result.toSet should be(expected)
  }

  test("should give nothing when showing privileges for non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

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
    } should have message "Trying to run `CATALOG SHOW PRIVILEGE` against non-system database."
  }

  test("should show privileges for specific user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW USER neo4j PRIVILEGES")

    // THEN
    val expected = Set(
      grantGraph().role("admin").user("neo4j").action("find").map,
      grantGraph().role("admin").user("neo4j").action("read").map,
      grantGraph().role("admin").user("neo4j").action("write").map,
      grantSystem().role("admin").user("neo4j").action("write").map,
      grantToken().role("admin").user("neo4j").action("write").map,
      grantSchema().role("admin").user("neo4j").action("write").map
    )

    result.toSet should be(expected)
  }

  test("should give nothing when showing privileges for non-existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

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
    } should have message "Trying to run `CATALOG SHOW PRIVILEGE` against non-system database."
  }

  // Tests for granting traverse privileges

  test("should grant traversal privilege to custom role for all databases and all labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").map))
  }

  test("should fail granting traversal privilege for all databases and all labels to non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
      // THEN
    } should have message "Role 'custom' does not exist."

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set())
  }

  test("should fail when granting traversal privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
      // THEN
    } should have message "Trying to run `CATALOG GRANT TRAVERSE` against non-system database."
  }

  test("should grant traversal privilege to custom role for all databases but only a specific label (that does not need to exist)") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").label("A").map))
  }

  test("should grant traversal privilege to custom role for a specific database and a specific label") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").database("foo").label("A").map))
  }

  test("should grant traversal privilege to custom role for a specific database and all labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantTraverse().role("custom").database("foo").map))
  }

  test("should grant traversal privilege to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").label("A").map,
      grantTraverse().role("custom").database("foo").label("B").map
    ))
  }

  test("should grant traversal privilege to custom role for a specific database and multiple labels in one grant") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo NODES A, B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").label("A").map,
      grantTraverse().role("custom").database("foo").label("B").map
    ))
  }

  test("should grant traversal privilege to multiple roles for a specific database and multiple labels in one grant") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH foo NODES A, B (*) TO role1, role2")

    // THEN
    execute("SHOW ROLE role1 PRIVILEGES").toSet should be(Set(
      grantTraverse().role("role1").database("foo").label("A").map,
      grantTraverse().role("role1").database("foo").label("B").map
    ))
    execute("SHOW ROLE role2 PRIVILEGES").toSet should be(Set(
      grantTraverse().role("role2").database("foo").label("A").map,
      grantTraverse().role("role2").database("foo").label("B").map
    ))
  }

  // Tests for actual behaviour of authorization rules for restricted users based on privileges

  test("should fail when granting traversal privilege with missing database") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    the [DatabaseNotFoundException] thrownBy {
      execute("GRANT TRAVERSE ON GRAPH foo NODES B (*) TO custom")
    } should have message "Database 'foo' does not exist."
  }

  test("should match nodes when granted traversal privilege to custom role for all databases and all labels") {
    //TODO: This should be an integration test

    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE custom")
    execute("GRANT ROLE custom TO joe")

    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }

    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)", (row, _) => {
      row.get("labels(n)").asInstanceOf[Collection[String]] should contain("A")
    }) should be(1)
  }

  test("should read properties when granted read privilege to custom role for all databases and all labels") {
    //TODO: This should be an integration test

    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE custom")
    execute("GRANT ROLE custom TO joe")
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", (row, _) => {
      row.get("n.name") should be(null)
    }) should be(1)

    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)
  }

  test("read privilege should not imply traverse privilege") {
    //TODO: This should be an integration test

    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE custom")
    execute("GRANT ROLE custom TO joe")

    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)
  }

  test("should see properties and nodes depending on privileges for role") {
    //TODO: This should be an integration test

    // GIVEN
    setupMultilabelData
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
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

    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("GRANT ROLE role1 TO joe")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar", (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
    }) should be(4)

    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role1 FROM joe")
    execute("GRANT ROLE role2 TO joe")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar", (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
    }) should be(4)

    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role2 FROM joe")
    execute("GRANT ROLE role3 TO joe")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar", (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
    }) should be(2)
  }

  test("should see properties and nodes when revoking privileges for role") {
    // GIVEN
    setupMultilabelData
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("GRANT ROLE role1 TO joe")

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role1")
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO role1")

    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO role1")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO role1")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO role1")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar", (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
    }) should be(4)

    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM role1")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar", (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
    }) should be(4)

    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM role1")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar", (row, index) => {
      (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
    }) should be(2)
  }

  // Tests for granting read privileges

  test("should grant read privilege to custom role for all databases and all labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").resource("all_properties").map))
  }

  test("should fail granting read privilege for all databases and all labels to non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
      // THEN
    } should have message "Role 'custom' does not exist."

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set())
  }

  test("should grant read privilege to custom role for all databases but only a specific label") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (*) ON GRAPH * NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").resource("all_properties").label("A").map))
  }

  test("should grant read privilege to custom role for a specific database and a specific label") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (*) ON GRAPH foo NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").database("foo").resource("all_properties").label("A").map))
  }

  test("should grant read privilege to custom role for a specific database and all labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (*) ON GRAPH foo NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").database("foo").resource("all_properties").map))
  }

  test("should grant read privilege to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (*) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (*) ON GRAPH foo NODES B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").database("foo").resource("all_properties").label("A").map,
      grantRead().role("custom").database("foo").resource("all_properties").label("B").map
    ))
  }

  test("should fail when granting read privilege with missing database") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    the [DatabaseNotFoundException] thrownBy {
      execute("GRANT READ (*) ON GRAPH foo NODES * (*) TO custom")
    } should have message "Database 'foo' does not exist."
  }

  test("should grant read privilege for specific property to custom role for all databases and all labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH * NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").property("bar").map))
  }

  test("should grant read privilege for specific property to custom role for all databases but only a specific label") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH * NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().role("custom").property("bar").label("A").map))
  }

  test("should grant read privilege for specific property to custom role for a specific database and a specific label") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().database("foo").role("custom").property("bar").label("A").map))
  }

  test("should grant read privilege for specific property to custom role for a specific database and all labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo NODES * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantRead().database("foo").role("custom").property("bar").map))
  }

  test("should grant read privilege for specific property to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").label("A").map,
      grantRead().database("foo").role("custom").property("bar").label("B").map
    ))
  }

  test("should grant read privilege for multiple properties to custom role for a specific database and specific label") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (baz) ON GRAPH foo NODES A (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").label("A").map,
      grantRead().database("foo").role("custom").property("baz").label("A").map
    ))
  }

  test("should grant read privilege for multiple properties to custom role for a specific database and multiple labels") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (baz) ON GRAPH foo NODES B (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").label("A").map,
      grantRead().database("foo").role("custom").property("baz").label("B").map
    ))
  }

  test("should grant read privilege for multiple properties to multiple roles for a specific database and multiple labels in a single grant") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT READ (a, b, c) ON GRAPH foo NODES A, B, C (*) TO role1, role2, role3")

    // THEN
    val expected = for (p <- Seq("a", "b", "c"); l <- Seq("A", "B", "C")) yield {
      grantRead().database("foo").property(p).label(l)
    }
    execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
    execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
    execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
  }

  // Tests for revoking privileges

  test("should revoke correct read privilege different label qualifier") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT READ (bar) ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").label("A").map,
      grantRead().database("foo").role("custom").property("bar").label("B").map
    ))

    // WHEN
    execute("REVOKE READ (bar) ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").label("B").map
    ))

    // WHEN
    execute("REVOKE READ (bar) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").label("B").map
    ))
  }

  test("should revoke correct read privilege different property") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT READ (*) ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT READ (a) ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT READ (b) ON GRAPH foo NODES * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("a").map,
      grantRead().database("foo").role("custom").property("b").map,
      grantRead().database("foo").role("custom").map
    ))

    // WHEN
    execute("REVOKE READ (a) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").map,
      grantRead().database("foo").role("custom").property("b").map
    ))

    // WHEN
    execute("REVOKE READ (*) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("b").map
    ))
  }

  test("should revoke correct read privilege different databases") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
    execute("GRANT READ (*) ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT READ (*) ON GRAPH bar NODES * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").map,
      grantRead().role("custom").database("foo").map,
      grantRead().role("custom").database("bar").map
    ))

    // WHEN
    execute("REVOKE READ (*) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").map,
      grantRead().role("custom").database("bar").map
    ))

    // WHEN
    execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").database("bar").map
    ))
  }

  test("should revoke correct traverse privilege different label qualifier") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").map,
      grantTraverse().database("foo").role("custom").label("A").map,
      grantTraverse().database("foo").role("custom").label("B").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").map,
      grantTraverse().database("foo").role("custom").label("B").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").label("B").map
    ))
  }

  test("should revoke correct traverse privilege different databases") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH bar NODES * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").map,
      grantTraverse().role("custom").database("foo").map,
      grantTraverse().role("custom").database("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").map,
      grantTraverse().role("custom").database("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("bar").map
    ))
  }

  test("should fail revoke privilege from non-existent role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")

    // WHEN
    the [InvalidArgumentsException] thrownBy {
      execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM wrongRole")
    } should have message "The role 'wrongRole' does not exist."
  }

  test("should fail revoke privilege not granted to role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE ROLE role")
    execute("CREATE DATABASE foo")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")

    // WHEN
    val e = the [InvalidArgumentsException] thrownBy {
      execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM role")
    }
    e.getMessage should include("The role 'role' does not have the specified privilege")
  }

  // Tests for granting roles to users

  test("should grant role to user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER user SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom TO user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "is_built_in" -> false, "member" -> "user"))
  }

  test("should grant role to several users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom TO userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "is_built_in" -> false, "member" -> "userA")
      + Map("role" -> "custom", "is_built_in" -> false, "member" -> "userB"))
  }

  test("should grant multiple roles to user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom1, custom2 TO userA")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom1", "is_built_in" -> false, "member" -> "userA")
      + Map("role" -> "custom2", "is_built_in" -> false, "member" -> "userA"))
  }

  test("should grant multiple roles to several users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom1, custom2 TO userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers
      + Map("role" -> "custom1", "is_built_in" -> false, "member" -> "userA")
      + Map("role" -> "custom1", "is_built_in" -> false, "member" -> "userB")
      + Map("role" -> "custom2", "is_built_in" -> false, "member" -> "userA")
      + Map("role" -> "custom2", "is_built_in" -> false, "member" -> "userB"))
  }

  test("should grant roles and list users with roles") {
    // GIVEN
    // User  : Roles
    // neo4j : admin
    // Bar   : dragon, fairy
    // Baz   :
    // Zet   : fairy
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
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

  test("should be able to grant already granted role to user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE ROLE dragon")
    execute("GRANT ROLE dragon TO Bar")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar", Seq("dragon")))

    // WHEN
    execute("GRANT ROLE dragon TO Bar")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar", Seq("dragon")))
  }

  test("should fail grant non existent role to user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER user SET PASSWORD 'neo'")

    the [InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE custom TO user")
      // THEN
    } should have message "Cannot grant non-existent role 'custom' to user 'user'"

    // AND
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers)
  }

  test("should fail when granting non-existing role to user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
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
    val rolesWithUsers = defaultRolesWithUsers ++ Set(Map("role" -> "dragon", "is_built_in" -> false, "member" -> null))
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
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
    } should have message "Trying to run `CATALOG GRANT ROLE` against non-system database."

    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE ROLE dragon")
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)

    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message "Trying to run `CATALOG GRANT ROLE` against non-system database."
  }

  // Tests for revoking roles from users

  test("should revoke role from user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER user SET PASSWORD 'neo'")
    execute("GRANT ROLE custom TO user")

    // WHEN
    execute("REVOKE ROLE custom FROM user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "is_built_in" -> false, "member" -> null))
  }

  test("should be able to revoke already revoked role from user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER user SET PASSWORD 'neo'")
    execute("GRANT ROLE custom TO user")
    execute("REVOKE ROLE custom FROM user")

    // WHEN
    execute("REVOKE ROLE custom FROM user").toSet should be(Set.empty)

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "is_built_in" -> false, "member" -> null))
  }

  test("should fail revoking non-existent role from (existing) user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    the [InvalidArgumentsException] thrownBy {
      // WHEN
      execute("REVOKE ROLE custom FROM user")
      // THEN
    } should have message "Cannot revoke role 'custom' from non-existent user 'user'"

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "is_built_in" -> false, "member" -> null))
  }

  test("should fail revoking non-existing role from non-existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("REVOKE ROLE custom FROM user")
      // THEN
    } should have message "Cannot revoke non-existent role 'custom' from user 'user'"

    // AND
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers)
  }

  test("should revoke role from multiple users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")
    execute("GRANT ROLE custom TO userA")
    execute("GRANT ROLE custom TO userB")

    // WHEN
    execute("REVOKE ROLE custom FROM userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "is_built_in" -> false, "member" -> null))
  }

  test("should revoke several roles from user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("GRANT ROLE custom1 TO userA")
    execute("GRANT ROLE custom2 TO userA")

    // WHEN
    execute("REVOKE ROLE custom1, custom2 FROM userA")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom1", "is_built_in" -> false, "member" -> null)
      + Map("role" -> "custom2", "is_built_in" -> false, "member" -> null))
  }

  test("should revoke several roles from multiple users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
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
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom1", "is_built_in" -> false, "member" -> null)
      + Map("role" -> "custom2", "is_built_in" -> false, "member" -> null))
  }

  test("should fail when revoking role from user when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE ROLE dragon FROM Bar")
      // THEN
    } should have message "Trying to run `CATALOG REVOKE ROLE` against non-system database."

    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE ROLE dragon")
    execute("GRANT ROLE dragon TO Bar")
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)

    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE ROLE dragon FROM Bar")
      // THEN
    } should have message "Trying to run `CATALOG REVOKE ROLE` against non-system database."
  }

  // helper variable, methods and class

  private val neo4jUser = user("neo4j", Seq("admin"))

  private def user(username: String, roles: Seq[String] = Seq.empty, suspended: Boolean = false, passwordChangeRequired: Boolean = true) = {
    Map("user" -> username, "roles" -> roles, "suspended" -> suspended, "passwordChangeRequired" -> passwordChangeRequired)
  }

  private def executeOnDefault(username: String, password: String, query: String, resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {}): Int = {
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
    val login = authManager.login(SecurityTestUtils.authToken(username, password))
    val tx = graph.beginTransaction(Transaction.Type.explicit, login)
    try {
      var count = 0
      val result: Result = new RichGraphDatabaseQueryService(graph).execute(query)
      result.accept(row => {
        resultHandler(row, count)
        count = count + 1
        true
      })
      tx.success()
      count
    } finally {
      tx.close()
    }
  }

  private def setupMultilabelData = {
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {foo:1, bar:2})")
    execute("CREATE (n:B {foo:3, bar:4})")
    execute("CREATE (n:A:B {foo:5, bar:6})")
    execute("CREATE (n {foo:7, bar:8})")
  }

  private case class PrivilegeMapBuilder(map: Map[String, AnyRef]) {
    def action(action: String) = PrivilegeMapBuilder(map + ("action" -> action))

    def role(role: String) = PrivilegeMapBuilder(map + ("role" -> role))

    def label(label: String) = PrivilegeMapBuilder(map + ("label" -> label))

    def database(database: String) = PrivilegeMapBuilder(map + ("database" -> database))

    def resource(resource: String) = PrivilegeMapBuilder(map + ("resource" -> resource))

    def user(user: String) = PrivilegeMapBuilder(map + ("user" -> user))

    def property(property: String) = PrivilegeMapBuilder(map + ("resource" -> s"property($property)"))
  }
  private val grantMap = Map("grant" -> "GRANTED", "database" -> "*", "label" -> "*")
  private def grantTraverse(): PrivilegeMapBuilder = grantGraph().action("find")
  private def grantRead(): PrivilegeMapBuilder = grantGraph().action("read").resource("all_properties")
  private def grantGraph(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "graph"))
  private def grantSchema(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "schema"))
  private def grantToken(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "token"))
  private def grantSystem(): PrivilegeMapBuilder = PrivilegeMapBuilder(grantMap + ("resource" -> "system"))

}
