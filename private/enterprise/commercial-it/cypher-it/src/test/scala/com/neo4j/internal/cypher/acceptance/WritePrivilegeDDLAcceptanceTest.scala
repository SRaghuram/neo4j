/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.cypher.DatabaseManagementException
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

import scala.collection.Map

class WritePrivilegeDDLAcceptanceTest extends DDLAcceptanceTestBase {

  // Tests for granting write privileges

  test("should grant write privilege to custom role for all databases and all elements") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantWrite().role("custom").node("*").map,
      grantWrite().role("custom").relationship("*").map
    ))
  }

  test("should grant write privilege to custom role for a specific database and all elements") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT WRITE (*) ON GRAPH foo ELEMENTS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantWrite().role("custom").database("foo").node("*").map,
      grantWrite().role("custom").database("foo").relationship("*").map
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
      grantWrite().database("foo").node("*"),
      grantWrite().database("foo").relationship("*")
    )

    execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
    execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
    execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
  }

  test("should fail granting write privilege for all databases and all elements to non-existing role") {
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
    the[DatabaseNotFoundException] thrownBy {
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

  // Tests for revoking write privileges

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
      grantWrite().role("custom").node("*").map,
      grantWrite().role("custom").node("*").database("foo").map,
      grantWrite().role("custom").node("*").database("bar").map,
      grantWrite().role("custom").relationship("*").map,
      grantWrite().role("custom").relationship("*").database("foo").map,
      grantWrite().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE WRITE (*) ON GRAPH foo ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantWrite().role("custom").node("*").map,
      grantWrite().role("custom").node("*").database("bar").map,
      grantWrite().role("custom").relationship("*").map,
      grantWrite().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE WRITE (*) ON GRAPH * ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantWrite().role("custom").node("*").database("bar").map,
      grantWrite().role("custom").relationship("*").database("bar").map
    ))
  }

  test("should fail revoke write privilege from non-existent role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    val error = the[InvalidArgumentsException] thrownBy {
      execute("REVOKE WRITE (*) ON GRAPH * ELEMENTS * (*) FROM wrongRole")
    }

    // THEN
    error.getMessage should (be("The role 'wrongRole' does not have the specified privilege: write * ON GRAPH * NODES *.") or
      be("The role 'wrongRole' does not exist."))
  }

  test("should fail revoke privilege not granted to role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE ROLE role")
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    val error = the[InvalidArgumentsException] thrownBy {
      execute("REVOKE WRITE (*) ON GRAPH * ELEMENTS * (*) FROM role")
    }
    // THEN
    error.getMessage should include("The role 'role' does not have the specified privilege")
  }

  test("should fail when revoking WRITE privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    val e = the[InvalidArgumentsException] thrownBy {
      execute("REVOKE WRITE (*) ON GRAPH foo ELEMENTS * (*) FROM custom")
    }
    // THEN
    e.getMessage should be("The privilege 'write * ON GRAPH foo NODES *' does not exist.")
  }

  test("should fail when revoking WRITE privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE WRITE (*) ON GRAPH * ELEMENTS * (*) FROM custom")
      // THEN
    } should have message "This is a DDL command and it should be executed against the system database: REVOKE WRITE"
  }

  // Tests for actual behaviour of authorization rules for restricted users based on privileges

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

  test("should read you own writes on nodes when granted TRAVERSE and WRITE privilege to custom role for all databases and all labels") {
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

  test("should read you own writes on relationships when granted TRAVERSE and WRITE privilege to custom role for all databases and all types") {
    // GIVEN
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * (*) TO custom")

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A)-[:REL {name:'a'}]->()")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE (*) ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    val expected = List("b", null)

    executeOnDefault("joe", "soap", "CREATE (n:A)-[:REL {name:'b'}]->() WITH n MATCH (A)-[r:REL]->() RETURN r.name AS name ORDER BY name",
      resultHandler = (row, index) => {
        row.get("name") should be(expected(index))
      }) should be(2)

    execute("MATCH (A)-[r:REL]->() RETURN r.name").toSet should be(Set(Map("r.name" -> "a"), Map("r.name" -> "b")))
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
    execute(s"GRANT WRITE (*) ON GRAPH $DEFAULT_DATABASE_NAME ELEMENTS * (*) TO custom")

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

}
