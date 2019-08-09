/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.cypher.DatabaseAdministrationException
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

import scala.collection.Map
import org.scalatest.enablers.Messaging.messagingNatureOfThrowable

class WritePrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom" -> 2,
      "REVOKE WRITE {*} ON GRAPH * ELEMENTS * (*) FROM custom" -> 2,
      "DENY WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom" -> 2,
      "REVOKE DENY WRITE {*} ON GRAPH * ELEMENTS * (*) FROM custom" -> 2
    ))
  }

  Seq(
    ("grant", "GRANT", "GRANTED" ),
    ("deny", "DENY", "DENIED" ),
  ).foreach {
    case (grantOrDeny, grantOrDenyCommand, grantOrDenyRelType) =>

      // Tests for granting and denying write privileges

      test(s"should $grantOrDeny write privilege to custom role for all databases and all elements") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
         write(grantOrDenyRelType).role("custom").node("*").map,
         write(grantOrDenyRelType).role("custom").relationship("*").map
        ))
      }

      test(s"should $grantOrDeny write privilege to custom role for a specific database and all elements") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")

        // WHEN
        execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH foo ELEMENTS * (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          write(grantOrDenyRelType).role("custom").database("foo").node("*").map,
          write(grantOrDenyRelType).role("custom").database("foo").relationship("*").map
        ))
      }

      test(s"should $grantOrDeny write privilege to multiple roles in a single grant") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE role1")
        execute("CREATE ROLE role2")
        execute("CREATE ROLE role3")
        execute("CREATE DATABASE foo")

        // WHEN
        execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH foo ELEMENTS * (*) TO role1, role2, role3")

        // THEN
        val expected: Seq[PrivilegeMapBuilder] = Seq(
          write(grantOrDenyRelType).database("foo").node("*"),
          write(grantOrDenyRelType).database("foo").relationship("*")
        )

        execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
        execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
        execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
      }

      test(s"should fail ${grantOrDeny}ing write privilege for all databases and all elements to non-existing role") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)

        // WHEN
        the[InvalidArgumentsException] thrownBy {
          // WHEN
          execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")
          // THEN
        } should have message s"Failed to $grantOrDeny write privilege to role 'custom': Role 'custom' does not exist."

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set())
      }

      test(s"should fail when ${grantOrDeny}ing write privilege with missing database") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        the[DatabaseNotFoundException] thrownBy {
          execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH foo ELEMENTS * (*) TO custom")
        } should have message s"Failed to $grantOrDeny write privilege to role 'custom': Database 'foo' does not exist."
      }

      test(s"should fail when ${grantOrDeny}ing write privilege to custom role when not on system database") {
        the[DatabaseAdministrationException] thrownBy {
          // WHEN
          execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")
          // THEN
        } should have message s"This is an administration command and it should be executed against the system database: $grantOrDenyCommand WRITE"
      }

      // Tests for revoke grant and revoke deny write privileges

      test(s"should revoke correct $grantOrDeny write privilege different databases") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH foo ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH bar ELEMENTS * (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          write(grantOrDenyRelType).role("custom").node("*").map,
          write(grantOrDenyRelType).role("custom").node("*").database("foo").map,
          write(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          write(grantOrDenyRelType).role("custom").relationship("*").map,
          write(grantOrDenyRelType).role("custom").relationship("*").database("foo").map,
          write(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $grantOrDenyCommand WRITE {*} ON GRAPH foo ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          write(grantOrDenyRelType).role("custom").node("*").map,
          write(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          write(grantOrDenyRelType).role("custom").relationship("*").map,
          write(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          write(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          write(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))
      }

      test(s"should be able to revoke write if only having $grantOrDeny") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          write(grantOrDenyRelType).role("custom").node("*").map,
          write(grantOrDenyRelType).role("custom").relationship("*").map
        ))

        // WHEN
        execute("REVOKE WRITE {*} ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)

      }

      test(s"should fail revoke $grantOrDeny write privilege from non-existent role") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

        // WHEN
        val error = the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) FROM wrongRole")
        }

        // THEN
        error.getMessage should (be(s"Failed to revoke write privilege from role 'wrongRole': The role 'wrongRole' does not have the specified privilege: $grantOrDenyCommand write * ON GRAPH * NODES *.") or
          be("Failed to revoke write privilege from role 'wrongRole': The role 'wrongRole' does not exist."))
      }

      test(s"should fail revoke $grantOrDeny write privilege not granted to role") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE ROLE role")
        execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

        // WHEN
        val error = the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) FROM role")
        }
        // THEN
        error.getMessage should include("The role 'role' does not have the specified privilege")
      }

      test(s"should fail when revoking $grantOrDeny write privilege with missing database") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

        // WHEN
        val e = the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $grantOrDenyCommand WRITE {*} ON GRAPH foo ELEMENTS * (*) FROM custom")
        }
        // THEN
        e.getMessage should be(s"Failed to revoke write privilege from role 'custom': The privilege '$grantOrDenyCommand write * ON GRAPH foo NODES *' does not exist.")
      }

      test(s"should fail when revoking $grantOrDeny write privilege to custom role when not on system database") {
        the[DatabaseAdministrationException] thrownBy {
          // WHEN
          execute(s"REVOKE $grantOrDenyCommand WRITE {*} ON GRAPH * ELEMENTS * (*) FROM custom")
          // THEN
        } should have message s"This is an administration command and it should be executed against the system database: REVOKE $grantOrDenyCommand WRITE"
      }
  }

  // Tests for revoke write privileges

  test("should revoke correct write privilege different databases") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")

    // WHEN
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")
    execute("GRANT WRITE {*} ON GRAPH foo ELEMENTS * (*) TO custom")
    execute("GRANT WRITE {*} ON GRAPH bar ELEMENTS * (*) TO custom")

    execute("DENY WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE {*} ON GRAPH foo ELEMENTS * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      write("GRANTED").role("custom").node("*").map,
      write("GRANTED").role("custom").node("*").database("foo").map,
      write("GRANTED").role("custom").node("*").database("bar").map,
      write("DENIED").role("custom").node("*").map,
      write("DENIED").role("custom").node("*").database("foo").map,

      write("GRANTED").role("custom").relationship("*").map,
      write("GRANTED").role("custom").relationship("*").database("foo").map,
      write("GRANTED").role("custom").relationship("*").database("bar").map,
      write("DENIED").role("custom").relationship("*").map,
      write("DENIED").role("custom").relationship("*").database("foo").map
    ))

    // WHEN
    execute("REVOKE WRITE {*} ON GRAPH foo ELEMENTS * (*) FROM custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      write("GRANTED").role("custom").node("*").map,
      write("GRANTED").role("custom").node("*").database("bar").map,
      write("DENIED").role("custom").node("*").map,

      write("GRANTED").role("custom").relationship("*").map,
      write("GRANTED").role("custom").relationship("*").database("bar").map,
      write("DENIED").role("custom").relationship("*").map,
    ))

    // WHEN
    execute("REVOKE WRITE {*} ON GRAPH * ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      write("GRANTED").role("custom").node("*").database("bar").map,
      write("GRANTED").role("custom").relationship("*").database("bar").map,
    ))

    // WHEN
    execute("REVOKE WRITE {*} ON GRAPH bar ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail revoke write privilege from non-existent role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    val error = the[InvalidArgumentsException] thrownBy {
      execute(s"REVOKE WRITE {*} ON GRAPH * ELEMENTS * (*) FROM wrongRole")
    }

    // THEN
    error.getMessage should (be(s"Failed to revoke write privilege from role 'wrongRole': The role 'wrongRole' does not have the specified privilege: write * ON GRAPH * NODES *.") or
      be("Failed to revoke write privilege from role 'wrongRole': The role 'wrongRole' does not exist."))
  }

  test("should fail revoke write privilege not granted to role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE ROLE role")
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    val error = the[InvalidArgumentsException] thrownBy {
      execute(s"REVOKE WRITE {*} ON GRAPH * ELEMENTS * (*) FROM role")
    }
    // THEN
    error.getMessage should include("The role 'role' does not have the specified privilege")
  }

  test("should fail when revoking write privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    val e = the[InvalidArgumentsException] thrownBy {
      execute(s"REVOKE WRITE {*} ON GRAPH foo ELEMENTS * (*) FROM custom")
    }
    // THEN
    e.getMessage should be(s"Failed to revoke write privilege from role 'custom': The privilege 'write * ON GRAPH foo NODES *' does not exist.")
  }

  test("should fail when revoking write privilege to custom role when not on system database") {
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("REVOKE WRITE {*} ON GRAPH * ELEMENTS * (*) FROM custom")
      // THEN
    } should have message s"This is an administration command and it should be executed against the system database: REVOKE WRITE"
  }

  // Mixed tests for write privileges

  test("should be able to have both grant and deny privilege for write") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      write().role("custom").node("*").map,
      write().role("custom").relationship("*").map,
      write("DENIED").role("custom").node("*").map,
      write("DENIED").role("custom").relationship("*").map
    ))
  }

  test("should revoke correct write privilege with a mix of grant and deny") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      write("GRANTED").role("custom").node("*").map,
      write("GRANTED").role("custom").relationship("*").map,
      write("DENIED").role("custom").node("*").map,
      write("DENIED").role("custom").relationship("*").map
    ))

    // WHEN
    execute(s"REVOKE GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      write("DENIED").role("custom").node("*").map,
      write("DENIED").role("custom").relationship("*").map,
    ))

    // WHEN
    execute(s"GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) To custom")
    execute(s"REVOKE DENY WRITE {*} ON GRAPH * ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      write("GRANTED").role("custom").node("*").map,
      write("GRANTED").role("custom").relationship("*").map
    ))
  }

  test("should not be able to revoke grant write privilege when only having deny") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("DENY WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      write("DENIED").role("custom").node("*").map,
      write("DENIED").role("custom").relationship("*").map
    ))

    // WHEN
    val error = the[InvalidArgumentsException] thrownBy {
      execute(s"REVOKE GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) FROM custom")
    }
    // THEN
    error.getMessage should include("The role 'custom' does not have the specified privilege: GRANT write * ON GRAPH * NODES *.")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      write("DENIED").role("custom").node("*").map,
      write("DENIED").role("custom").relationship("*").map,
    ))
  }

  test("should not be able to revoke deny write privilege when only having grant") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      write().role("custom").node("*").map,
      write().role("custom").relationship("*").map
    ))

    // WHEN
    val error = the[InvalidArgumentsException] thrownBy {
      execute(s"REVOKE DENY WRITE {*} ON GRAPH * ELEMENTS * (*) FROM custom")
    }
    // THEN
    error.getMessage should include("The role 'custom' does not have the specified privilege: DENY write * ON GRAPH * NODES *.")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      write().role("custom").node("*").map,
      write().role("custom").relationship("*").map,
    ))
  }

  // Tests for actual behaviour of authorization rules for restricted users based on privileges

  test("should create node when granted WRITE privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) RETURN 1 as dummy")
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) RETURN 1 AS dummy", resultHandler = (row, _) => {
      row.get("dummy") should be(1)
    }) should be(1)

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))
  }

  test("should not be able to create node when denied WRITE privilege to custom role for all databases")
  {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) RETURN 1 AS dummy", resultHandler = (row, _) => {
      row.get("dummy") should be(1)
    }) should be(1)

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:A {name: 'c'}) RETURN 1 as dummy")
    }

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:A {name: 'd'}) RETURN 1 as dummy")
    }

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))

  }

  test("should read you own writes on nodes when granted TRAVERSE and WRITE privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    val expected = List("b", null)

    executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) WITH n MATCH (m:A) RETURN m.name AS name ORDER BY name", resultHandler = (row, index) => {
      row.get("name") should be(expected(index))
    }) should be(2)

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))
  }

  test("should read you own writes on relationships when granted TRAVERSE and WRITE privilege to custom role for all databases and all types") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * (*) TO custom")

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A)-[:REL {name:'a'}]->()")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

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
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * NODES * (*) TO custom")

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n: A) WITH n, n.name as name DETACH DELETE n RETURN name")
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n: A) WITH n, n.name as name DETACH DELETE n RETURN name", resultHandler = (row, _) => {
      row.get("name") should be("a")
    }) should be(1)

    execute("MATCH (n) RETURN n.name").toSet should be(Set.empty)
  }

  test("should set and remove property when granted WRITE privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * NODES * (*) TO custom")

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a', prop: 'b'})")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n:A) SET n.name = 'b'")
    }

    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n:A) REMOVE n.prop")
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n:A) SET n.name = 'b' REMOVE n.prop") should be(0)

    execute("MATCH (n) RETURN properties(n) as props").toSet should be(Set(Map("props" -> Map("name" -> "b"))))
  }

  test("should not create new tokens, indexes or constraints when granted WRITE privilege") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES * (*) TO custom")
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

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
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")

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

    setupUserWithCustomRole()

    // WHEN
    execute(s"GRANT WRITE {*} ON GRAPH $DEFAULT_DATABASE_NAME ELEMENTS * (*) TO custom")

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

  test("should not be able to create nodes when denied WRITE privilege to custom role for a specific database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:B {name:'b'})")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name:'a'})")

    setupUserWithCustomRole()

    // WHEN
    execute("GRANT WRITE {*} ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE {*} ON GRAPH foo ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (:A {name: 'b'}) RETURN 1 AS dummy", resultHandler = (row, _) => {
      row.get("dummy") should be(1)
    }) should be(1)

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))

    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "CREATE (:B {name: 'a'}) RETURN 1 AS dummy")
    } should have message "Write operations are not allowed for user 'joe' with roles [custom]."

    selectDatabase("foo")
    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "b")))
  }

}
