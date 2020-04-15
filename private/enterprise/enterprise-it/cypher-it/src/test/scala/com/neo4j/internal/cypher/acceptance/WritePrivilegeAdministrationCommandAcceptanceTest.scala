/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.scalatest.enablers.Messaging.messagingNatureOfThrowable

//noinspection RedundantDefaultArgument
class WritePrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    // GIVEN
    execute("CREATE ROLE custom")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom" -> 2,
      "REVOKE WRITE ON GRAPH * ELEMENTS * (*) FROM custom" -> 2,
      "DENY WRITE ON GRAPH * ELEMENTS * (*) TO custom" -> 2,
      "REVOKE DENY WRITE ON GRAPH * ELEMENTS * (*) FROM custom" -> 2,

      "GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom" -> 2,
      "DENY WRITE ON GRAPH * ELEMENTS * (*) TO custom" -> 2,
      "REVOKE WRITE ON GRAPH * ELEMENTS * (*) FROM custom" -> 4,
    ))
  }

  Seq(
    ("grant", "GRANT", granted: privilegeFunction),
    ("deny", "DENY", denied: privilegeFunction),
  ).foreach {
    case (grantOrDeny, grantOrDenyCommand, grantedOrDenied) =>

      // Tests for granting and denying write privileges

      test(s"should $grantOrDeny write privilege to custom role for all databases and all elements") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDenyCommand WRITE ON GRAPH * ELEMENTS * (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(write).role("custom").node("*").map,
          grantedOrDenied(write).role("custom").relationship("*").map
        ))
      }

      test(s"should $grantOrDeny write privilege to custom role for a specific database and all elements") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")

        // WHEN
        execute(s"$grantOrDenyCommand WRITE ON GRAPH foo ELEMENTS * (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(write).role("custom").database("foo").node("*").map,
          grantedOrDenied(write).role("custom").database("foo").relationship("*").map
        ))
      }

      test(s"should $grantOrDeny write privilege to custom role for multiple databases and all elements") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")

        // WHEN
        execute(s"$grantOrDenyCommand WRITE ON GRAPH foo, bar ELEMENTS * (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(write).role("custom").database("foo").node("*").map,
          grantedOrDenied(write).role("custom").database("foo").relationship("*").map,
          grantedOrDenied(write).role("custom").database("bar").node("*").map,
          grantedOrDenied(write).role("custom").database("bar").relationship("*").map
        ))
      }

      test(s"should $grantOrDeny write privilege to custom role for specific database and all elements using parameter") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDenyCommand WRITE ON GRAPH $$db ELEMENTS * (*) TO custom", Map("db" -> DEFAULT_DATABASE_NAME))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(write).database(DEFAULT_DATABASE_NAME).role("custom").node("*").map,
          grantedOrDenied(write).database(DEFAULT_DATABASE_NAME).role("custom").relationship("*").map
        ))
      }

      test(s"should $grantOrDeny write privilege to multiple roles in a single grant") {
        // GIVEN
        execute("CREATE ROLE role1")
        execute("CREATE ROLE role2")
        execute("CREATE ROLE role3")
        execute("CREATE DATABASE foo")

        // WHEN
        execute(s"$grantOrDenyCommand WRITE ON GRAPH foo ELEMENTS * (*) TO role1, role2, role3")

        // THEN
        val expected: Seq[PrivilegeMapBuilder] = Seq(
          grantedOrDenied(write).database("foo").node("*"),
          grantedOrDenied(write).database("foo").relationship("*")
        )

        execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
        execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
        execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
      }

      test(s"should fail with 'not-supported-yet' message when ${grantOrDeny}ing write privilege with limited scope") {
        // WHEN
        val e = the[SyntaxException] thrownBy {
          execute(s"$grantOrDenyCommand WRITE ON GRAPH * ELEMENTS A (*) TO role")
        }
        // THEN
        e.getMessage should startWith("The use of ELEMENT, NODE or RELATIONSHIP with the WRITE privilege is not supported in this version.")
      }

      // Tests for revoke grant and revoke deny write privileges

      test(s"should revoke correct $grantOrDeny write privilege different databases") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand WRITE ON GRAPH * ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand WRITE ON GRAPH foo ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand WRITE ON GRAPH bar ELEMENTS * (*) TO custom")

        // WHEN
        execute(s"REVOKE $grantOrDenyCommand WRITE ON GRAPH foo ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(write).role("custom").node("*").map,
          grantedOrDenied(write).role("custom").node("*").database("bar").map,
          grantedOrDenied(write).role("custom").relationship("*").map,
          grantedOrDenied(write).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $grantOrDenyCommand WRITE ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(write).role("custom").node("*").database("bar").map,
          grantedOrDenied(write).role("custom").relationship("*").database("bar").map
        ))
      }

      test(s"should be able to revoke write if only having $grantOrDeny") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDenyCommand WRITE ON GRAPH * ELEMENTS * (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(write).role("custom").node("*").map,
          grantedOrDenied(write).role("custom").relationship("*").map
        ))

        // WHEN
        execute("REVOKE WRITE ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)

      }

      test(s"should be able to revoke $grantOrDeny write using parameter") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand WRITE ON GRAPH foo TO custom")

        // WHEN
        execute(s"REVOKE $grantOrDenyCommand WRITE ON GRAPH $$db FROM custom", Map("db" -> "foo"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should do nothing when revoking $grantOrDeny write privilege from non-existent role") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand WRITE ON GRAPH * ELEMENTS * (*) TO custom")

        // WHEN
        execute(s"REVOKE $grantOrDenyCommand WRITE ON GRAPH * ELEMENTS * (*) FROM wrongRole")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(write).role("custom").node("*").map,
          grantedOrDenied(write).role("custom").relationship("*").map
        ))
      }

      test(s"should do nothing when revoking $grantOrDeny write privilege not granted to role") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE ROLE role")
        execute(s"$grantOrDenyCommand WRITE ON GRAPH * ELEMENTS * (*) TO custom")

        // WHEN
        execute(s"REVOKE $grantOrDenyCommand WRITE ON GRAPH * ELEMENTS * (*) FROM role")
        // THEN
        execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
      }
  }

  // Tests for revoke write privileges

  test("should revoke correct write privilege different databases") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")

    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")
    execute("GRANT WRITE ON GRAPH foo ELEMENTS * (*) TO custom")
    execute("GRANT WRITE ON GRAPH bar ELEMENTS * (*) TO custom")

    execute("DENY WRITE ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE ON GRAPH foo ELEMENTS * (*) TO custom")

    // WHEN
    execute("REVOKE WRITE ON GRAPH foo ELEMENTS * (*) FROM custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(write).role("custom").node("*").map,
      granted(write).role("custom").node("*").database("bar").map,
      denied(write).role("custom").node("*").map,

      granted(write).role("custom").relationship("*").map,
      granted(write).role("custom").relationship("*").database("bar").map,
      denied(write).role("custom").relationship("*").map,
    ))

    // WHEN
    execute("REVOKE WRITE ON GRAPH * ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(write).role("custom").node("*").database("bar").map,
      granted(write).role("custom").relationship("*").database("bar").map,
    ))

    // WHEN
    execute("REVOKE WRITE ON GRAPH bar ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("should do nothing when revoking write privilege from non-existent role") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    execute(s"REVOKE WRITE ON GRAPH * ELEMENTS * (*) FROM wrongRole")

    // THEN
    execute("SHOW ROLE wrongRole PRIVILEGES").toSet should be(Set.empty)
  }

  test("should do nothing when revoking write privilege not granted to role") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE ROLE role")
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    execute(s"REVOKE WRITE ON GRAPH * ELEMENTS * (*) FROM role")
    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(write).role("custom").node("*").map,
      granted(write).role("custom").relationship("*").map,
      denied(write).role("custom").node("*").map,
      denied(write).role("custom").relationship("*").map
    ))
  }

  // Mixed tests for write privileges

  test("should be able to have both grant and deny privilege for write") {
    // GIVEN
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO $role", Map("role" -> "custom"))
    execute("DENY WRITE ON GRAPH * ELEMENTS * (*) TO $role", Map("role" -> "custom"))

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(write).role("custom").node("*").map,
      granted(write).role("custom").relationship("*").map,
      denied(write).role("custom").node("*").map,
      denied(write).role("custom").relationship("*").map
    ))
  }

  test("should revoke correct write privilege with a mix of grant and deny") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    execute("REVOKE GRANT WRITE ON GRAPH * ELEMENTS * (*) FROM $role", Map("role" -> "custom"))

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      denied(write).role("custom").node("*").map,
      denied(write).role("custom").relationship("*").map,
    ))

    // WHEN
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) To custom")
    execute("REVOKE DENY WRITE ON GRAPH * ELEMENTS * (*) FROM $role", Map("role" -> "custom"))

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(write).role("custom").node("*").map,
      granted(write).role("custom").relationship("*").map
    ))
  }

  test("should do nothing when revoking grant write privilege when only having deny") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("DENY WRITE ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    execute(s"REVOKE GRANT WRITE ON GRAPH * ELEMENTS * (*) FROM custom")
    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      denied(write).role("custom").node("*").map,
      denied(write).role("custom").relationship("*").map
    ))
  }

  test("should do nothing when revoking deny write privilege when only having grant") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

    // WHEN
    execute(s"REVOKE DENY WRITE ON GRAPH * ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(write).role("custom").node("*").map,
      granted(write).role("custom").relationship("*").map
    ))
  }

  test("should revoke part of write privilege when not having all") {
    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE custom")
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE ON GRAPH foo ELEMENTS * (*) TO custom")

    // WHEN
    execute(s"REVOKE WRITE ON GRAPH * ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      denied(write).role("custom").database("foo").node("*").map,
      denied(write).role("custom").database("foo").relationship("*").map
    ))

    // WHEN
    execute(s"REVOKE WRITE ON GRAPH foo ELEMENTS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("should normalize graph name for write privileges") {
    // GIVEN)
    execute("CREATE DATABASE BaR")
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT WRITE ON GRAPH BAR TO custom")
    execute("DENY WRITE ON GRAPH baR TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(write).role("custom").database("bar").node("*").map,
      granted(write).role("custom").database("bar").relationship("*").map,
      denied(write).role("custom").database("bar").node("*").map,
      denied(write).role("custom").database("bar").relationship("*").map
    ))

    // WHEN
    execute("REVOKE WRITE ON GRAPH Bar FROM custom")

    //THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
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
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) RETURN 1 AS dummy", resultHandler = (row, _) => {
      row.get("dummy") should be(1)
    }) should be(1)

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))
  }

  test("should not be able to create node when denied WRITE privilege to custom role for all databases") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) RETURN 1 AS dummy", resultHandler = (row, _) => {
      row.get("dummy") should be(1)
    }) should be(1)

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY WRITE ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:A {name: 'c'}) RETURN 1 as dummy")
    }

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:A {name: 'd'}) RETURN 1 as dummy")
    }

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))

  }

  Seq("interpreted", "slotted").foreach { runtime =>
    test(s"should read you own writes on nodes with WRITE and ACCESS privilege with $runtime") {
      // GIVEN
      clearPublicRole()
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A {name:'a'})")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val query = s"CYPHER runtime=$runtime CREATE (n:A {name: 'b'}) WITH 1 AS ignore MATCH (m:A) RETURN m.name AS name ORDER BY name"
      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        row.get("name") should be("b")
      }) should be(1)

      execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))
      executeOnDefault("joe", "soap", s"CYPHER runtime=$runtime MATCH (n:A) RETURN n.name") should be(0)
    }

    test(s"should read you own writes on nodes with WRITE and TRAVERSE privilege with $runtime") {
      // GIVEN
      clearPublicRole()
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A {name:'a'})")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val expected = List("b", null)

      val query = s"CYPHER runtime=$runtime CREATE (n:A {name: 'b'}) WITH 1 AS ignore MATCH (m:A) RETURN m.name AS name ORDER BY name"
      executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
        row.get("name") should be(expected(index))
      }) should be(2)

      execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))
      executeOnDefault("joe", "soap", s"CYPHER runtime=$runtime MATCH (n:A) RETURN n.name AS name", resultHandler = (row, _) => {
        row.get("name") should be(null)
      }) should be(2)
    }

    test(s"should read you own writes on nodes with WRITE and restricted READ privilege with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A {name:'a', age: 21})")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT MATCH {name} ON GRAPH * NODES * (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val expected1 = List(("a", null), ("b", 22))

      val query = s"CYPHER runtime=$runtime CREATE (n:A {name: 'b', age: 22}) WITH 1 AS ignore MATCH (m:A) RETURN m.name AS name, m.age AS age ORDER BY name"
      executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
        (row.get("name"), row.get("age")) should be(expected1(index))
      }) should be(2)

      // THEN
      execute(s"CYPHER runtime=$runtime MATCH (n) RETURN n.name, n.age").toSet should be(Set(Map("n.name" -> "a", "n.age" -> 21), Map("n.name" -> "b", "n.age" -> 22)))

      val expected2 = List(("a", null), ("b", null))
      executeOnDefault("joe", "soap", s"CYPHER runtime=$runtime MATCH (n:A) RETURN n.name AS name, n.age AS age ORDER BY name", resultHandler = (row, index) => {
        (row.get("name"), row.get("age")) should be(expected2(index))
      }) should be(2)
    }

    test(s"should read you own writes on nodes with WRITE and restricted READ and TRAVERSE privileges with $runtime") {
      // GIVEN
      clearPublicRole()
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A {name:'a'})")
      execute("CREATE (n:A:B {name:'ab'})")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")
      execute("GRANT READ {name} ON GRAPH * NODES B (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val expected1 = List("ab", "b")

      val query = s"CYPHER runtime=$runtime CREATE (n:B {name: 'b'}) WITH 1 AS ignore MATCH (m:B) RETURN m.name AS name ORDER BY name"
      executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
        row.get("name") should be(expected1(index))
      }) should be(2)

      // THEN
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute(s"CYPHER runtime=$runtime MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "ab"), Map("n.name" -> "b")))

      executeOnDefault("joe", "soap", s"CYPHER runtime=$runtime MATCH (n:B) RETURN n.name AS name ORDER BY name", resultHandler = (row, _) => {
        row.get("name") should be("ab")
      }) should be(1)

      val expected2 = List("ab", null)
      executeOnDefault("joe", "soap", s"CYPHER runtime=$runtime MATCH (n:A) RETURN n.name AS name ORDER BY name", resultHandler = (row, index) => {
        row.get("name") should be(expected2(index))
      }) should be(2)
    }

    test(s"should read your own writes on nodes with WRITE and denied TRAVERSE privileges with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A:B {name:'ab'})")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT MATCH {name} ON GRAPH * NODES * (*) TO custom")
      execute("DENY TRAVERSE ON GRAPH * NODES B (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val query = s"CYPHER runtime=$runtime CREATE (n:B {name: 'b'}) WITH 1 AS ignore MATCH (m:B) RETURN m.name AS name ORDER BY name"
      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        row.get("name") should be("b")
      }) should be(1)

      // THEN
      executeOnDefault("joe", "soap", s"CYPHER runtime=$runtime MATCH (n:B) RETURN n.name AS name ORDER BY name") should be(0)

      execute(s"CYPHER runtime=$runtime MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "ab"), Map("n.name" -> "b")))
    }

    test(s"should read your own writes on nodes with WRITE and denied READ privileges with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A:B {name:'ab'})")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT MATCH {name} ON GRAPH * NODES * (*) TO custom")
      execute("DENY READ {name} ON GRAPH * NODES B (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val expected = Seq("b", null)
      val query = s"CYPHER runtime=$runtime CREATE (n:B {name: 'b'}) WITH 1 AS ignore MATCH (m:B) RETURN m.name AS name ORDER BY name"
      executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
        row.get("name") should be(expected(index))
      }) should be(2)

      // THEN
      executeOnDefault("joe", "soap", s"CYPHER runtime=$runtime MATCH (n:B) RETURN n.name AS name ORDER BY name", resultHandler = (row, _) => {
        row.get("name") should be(null)
      }) should be(2)

      execute(s"CYPHER runtime=$runtime MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "ab"), Map("n.name" -> "b")))
    }

    test(s"should see property until end of transaction after setting denied label with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A {name:'a'})")
      execute("CREATE (n:B {name:'b'})")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT MATCH {name} ON GRAPH * NODES A (*) TO custom")
      execute("DENY TRAVERSE ON GRAPH * NODES B (*) TO custom")
      execute("DENY READ {name} ON GRAPH * NODES B (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val query =
        s"""CYPHER runtime=$runtime
           |MATCH (a:A)
           |SET a:B
           |RETURN a.name AS name""".stripMargin

      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        row.get("name") should be("a")
      }) should be(1)
    }

    test(s"should find node and read possibly cached property in new MATCH until end of transaction after setting a denied label with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A {name:'a'})")
      execute("CREATE (n:B {name:'b'})")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT MATCH {name} ON GRAPH * NODES A (*) TO custom")
      execute("DENY TRAVERSE ON GRAPH * NODES B (*) TO custom")
      execute("DENY READ {name} ON GRAPH * NODES B (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val query =
        s"""CYPHER runtime=$runtime
           |MATCH (a:A)
           |WHERE a.name = 'a'
           |SET a:B
           |WITH 1 AS ignore
           |MATCH (a:A)
           |RETURN a.name AS name""".stripMargin

      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        row.get("name") should be("a")
      }) should be(1)
    }

    test(s"should find node and read property in new MATCH until end of transaction after setting a denied label with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A {name:'a'})")
      execute("CALL db.createLabel('B')")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT MATCH {name} ON GRAPH * NODES A (*) TO custom")
      execute("DENY TRAVERSE ON GRAPH * NODES B (*) TO custom")
      execute("DENY READ {name} ON GRAPH * NODES B (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val query =
        s"""CYPHER runtime=$runtime
           |MATCH (a:A)
           |SET a:B
           |WITH 1 AS ignore
           |MATCH (a:A)
           |RETURN a.name AS name""".stripMargin

      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        row.get("name") should be("a")
      }) should be(1)
    }

    test(s"setting a deny traverse label on a node that can be found should still be found when in same transaction (with two queries) and with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A {name:'a'})")
      execute("CALL db.createLabel('B')")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")
      execute("GRANT MATCH {name} ON GRAPH * NODES A (*) TO custom")
      execute("DENY TRAVERSE ON GRAPH * NODES B (*) TO custom")

      // THEN: check read-only
      executeOnDefault("joe", "soap", "MATCH (a:A) RETURN a.name AS name", resultHandler = (row, _) => {
        row.get("name") should be("a")
      }) should be(1)

      // THEN: two part query
      executeOnDefault("joe", "soap", s"CYPHER runtime=$runtime MATCH (a:A) SET a:B WITH 1 AS ignore MATCH (a:A) RETURN a.name AS name", resultHandler = (row, _) => {
        row.get("name") should be("a")
      }) should be(1)

      // remove label B to go back to original set-up
      execute("MATCH (n:A) REMOVE n:B")

      // THEN: two queries in same transaction
      executeOnDefault("joe", "soap", executeBefore = tx => tx.execute("MATCH (a:A) SET a:B"), query = s"CYPHER runtime=$runtime MATCH (a:A) RETURN a.name AS name",  resultHandler = (row, _) => {
        row.get("name") should be("a")
      }) should be(1)
    }

    // TODO: We are considering doing the opposite behaviour (see Read.java:334 'nodeLabelScan')
    test(s"should find explicitly denied node when label is set in current transaction with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A {name:'a'})")
      execute("CALL db.createLabel('B')")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT MATCH {name} ON GRAPH * NODES A (*) TO custom")
      execute("DENY TRAVERSE ON GRAPH * NODES B (*) TO custom")
      execute("DENY READ {name} ON GRAPH * NODES B (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val query =
        s"""CYPHER runtime=$runtime
           |MATCH (a:A)
           |SET a:B
           |WITH 1 AS ignore
           |MATCH (b:B)
           |RETURN b.name AS name""".stripMargin

      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        row.get("name") should be("a")
      }) should be(1)
    }

    test(s"should find node and read property after creating and setting a denied label with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A {name:'a'})")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT MATCH {name} ON GRAPH * NODES A (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")
      execute("GRANT CREATE NEW LABEL ON DATABASE * TO custom")
      execute("DENY TRAVERSE ON GRAPH * NODES B (*) TO custom")
      execute("DENY READ {name} ON GRAPH * NODES B (*) TO custom")

      // THEN: label B is not a existing token
      val query = s"CYPHER runtime=$runtime MATCH (a:A) WHERE a.name = 'a' SET a:B WITH 1 AS ignore MATCH (a:A) RETURN a.name AS name"
      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        row.get("name") should be("a")
      })
    }

    test(s"should not be able to read property after removing denied read label when in same transaction with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A:B {name:'a'})")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")
      execute("GRANT MATCH {name} ON GRAPH * NODES A (*) TO custom")
      execute("DENY READ {name} ON GRAPH * NODES B (*) TO custom")

      // THEN: check read-only
      executeOnDefault("joe", "soap", "MATCH (a:A) WHERE exists(a.name) RETURN a.name AS name") should be(0)

      // THEN: two part query
      executeOnDefault("joe", "soap", s"CYPHER runtime=$runtime MATCH (a:A:B) REMOVE a:B WITH 1 AS ignore MATCH (a:A) RETURN a.name AS name", resultHandler = (row, _) => {
        row.get("name") should be(null)
      }) should be(1)

      // set label B to go back to original set-up
      execute("MATCH (n:A) SET n:B")

      // THEN: two queries in same transaction
      executeOnDefault("joe", "soap", executeBefore = tx => tx.execute("MATCH (a:A:B) REMOVE a:B"), query = s"CYPHER runtime=$runtime MATCH (a:A) RETURN a.name AS name",  resultHandler = (row, _) => {
        row.get("name") should be(null)
      }) should be(1)

      // THEN: check read-only after removal of denied label
      executeOnDefault("joe", "soap", "MATCH (a:A) WHERE exists(a.name) RETURN a.name AS name", resultHandler = (row, _) => {
        row.get("name") should be("a")
      }) should be(1)
    }

    test(s"should find node and read property in new MATCH with index until end of transaction after setting a denied traverse label with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A {name:'a'})")
      execute("CALL db.createLabel('B')")
      graph.createIndex("A", "name")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")
      execute("GRANT MATCH {name} ON GRAPH * NODES A (*) TO custom")
      execute("DENY TRAVERSE ON GRAPH * NODES B (*) TO custom")
      execute("DENY READ {name} ON GRAPH * NODES B (*) TO custom")

      // THEN
      val query =
        s"""CYPHER runtime=$runtime
           |MATCH (a:A)
           |SET a:B
           |WITH 1 AS ignore
           |MATCH (a:A)
           |WHERE a.name = 'a'
           |RETURN a.name AS name""".stripMargin

      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        row.get("name") should be("a")
      }, requiredOperator = Some("NodeIndexSeek")) should be(1)
    }

    test(s"should not find node with index seek after removing a denied traverse label when in same transaction with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A:B {name:'a'})")
      graph.createIndex("A", "name")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")
      execute("GRANT MATCH {name} ON GRAPH * NODES A (*) TO custom")
      execute("DENY READ {name} ON GRAPH * NODES B (*) TO custom")

      // THEN: read-only check
      executeOnDefault("joe", "soap", "MATCH (a:A) WHERE exists(a.name) RETURN a.name AS name", requiredOperator = Some("NodeIndexScan")) should be(0)

      // THEN
      val query =
        s"""CYPHER runtime=$runtime
           |MATCH (a:A)
           |REMOVE a:B
           |WITH 1 AS ignore
           |MATCH (a:A) WHERE exists(a.name)
           |RETURN a.name AS name""".stripMargin

      executeOnDefault("joe", "soap", query, requiredOperator = Some("NodeIndexScan")) should be(0)

      // THEN: read-only check
      executeOnDefault("joe", "soap", "MATCH (a:A) WHERE exists(a.name) RETURN a.name AS name", resultHandler = (row, _) => {
        row.get("name") should be("a")
      }, requiredOperator = Some("NodeIndexScan")) should be(1)
    }

    test(s"should read your own writes on relationships when granted ACCESS and WRITE privilege with $runtime") {
      // GIVEN
      clearPublicRole()
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (:A)-[:REL {name:'a'}]->()")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val query = s"CYPHER runtime=$runtime CREATE (:A)-[:REL {name:'b'}]->() WITH 1 AS ignore MATCH (:A)-[r:REL]->() RETURN r.name AS name"
      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        row.get("name") should be("b")
      }) should be(1)

      execute("MATCH (:A)-[r:REL]->() RETURN r.name").toSet should be(Set(Map("r.name" -> "a"), Map("r.name" -> "b")))

      executeOnDefault("joe", "soap", "MATCH (:A)-[r:REL]->() RETURN r.name AS name") should be(0)
    }

    test(s"should read your own writes on relationships when granted TRAVERSE and WRITE privilege with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (:A)-[:REL {name:'a'}]->()")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
      execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val expected = List("b", null)

      val query = s"CYPHER runtime=$runtime CREATE (:A)-[:REL {name:'b'}]->() WITH 1 AS ignore MATCH (:A)-[r:REL]->() RETURN r.name AS name ORDER BY name"
      executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
        row.get("name") should be(expected(index))
      }) should be(2)

      execute("MATCH (:A)-[r:REL]->() RETURN r.name").toSet should be(Set(Map("r.name" -> "a"), Map("r.name" -> "b")))

      executeOnDefault("joe", "soap", "MATCH (:A)-[r:REL]->() RETURN r.name AS name", resultHandler = (row, _) => {
        row.get("name") should be(null)
      }) should be(2)
    }

    test(s"should read your own writes on relationships with WRITE and restricted TRAVERSE privilege with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:A)-[:REL {name:'a'}]->()")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
      execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * (*) TO custom")
      execute("DENY TRAVERSE ON GRAPH * RELATIONSHIPS REL (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val query = s"CYPHER runtime=$runtime CREATE (:A)-[:REL {name:'b'}]->() WITH 1 AS ignore MATCH (:A)-[r:REL]->() RETURN r.name AS name"
      executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
        row.get("name") should be("b")
      }) should be(1)

      // THEN
      execute(s"CYPHER runtime=$runtime MATCH (:A)-[r:REL]->() RETURN r.name AS name").toSet should be(Set(Map("name" -> "a"), Map("name" -> "b")))

      executeOnDefault("joe", "soap", s"CYPHER runtime=$runtime MATCH (:A)-[r:REL]->() RETURN r.name AS name") should be(0)
    }

    test(s"should read your own writes on relationships with WRITE and restricted READ privilege with $runtime") {
      // GIVEN
      setupUserWithCustomRole()

      // Setup to create tokens
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (:A)-[:REL {name:'a', age: 21, pets: true}]->()")

      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
      execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * (*) TO custom")
      execute("GRANT READ {name} ON GRAPH * RELATIONSHIPS * (*) TO custom")
      execute("DENY READ {pets} ON GRAPH * RELATIONSHIPS * (*) TO custom")
      execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

      // THEN
      val expected1 = List(("a", null, null), ("b", 22, false))

      val query = s"CYPHER runtime=$runtime CREATE (:A)-[:REL {name:'b', age: 22, pets: false}]->() WITH 1 AS ignore MATCH (:A)-[r:REL]->() RETURN r.name AS name, r.age AS age, r.pets AS pets ORDER BY name"
      executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
        (row.get("name"), row.get("age"), row.get("pets")) should be(expected1(index))
      }) should be(2)

      // THEN
      execute(s"CYPHER runtime=$runtime MATCH (:A)-[r:REL]->() RETURN r.name AS name, r.age AS age, r.pets AS pets").toSet should be(Set(Map("name" -> "a", "age" -> 21, "pets" -> true), Map("name" -> "b", "age" -> 22, "pets" -> false)))

      val expected2 = List(("a", null, null), ("b", null, null))
      executeOnDefault("joe", "soap", s"CYPHER runtime=$runtime MATCH (:A)-[r:REL]->() RETURN r.name AS name, r.age AS age, r.pets AS pets ORDER BY name", resultHandler = (row, index) => {
        (row.get("name"), row.get("age"), row.get("pets")) should be(expected2(index))
      }) should be(2)
    }
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
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

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
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

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
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

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
      executeOnDefault("joe", "soap", "CREATE INDEX FOR (n:A) ON (n.name)")
    }

    // Need schema permission to add constraint
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:A) ASSERT exists(n.name)")
    }
  }

  test("write privilege should not imply access privilege") {
    // GIVEN
    clearPublicRole()
    setupUserWithCustomRole(access = false)
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }
  }

  test("write privilege should not imply traverse privilege") {
    // GIVEN
    clearPublicRole()
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)") should be(0)
  }

  test("should create nodes when granted WRITE privilege to custom role for a specific database") {
    // GIVEN
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:B {name:'b'})")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name:'a'})")

    setupUserWithCustomRole()

    // WHEN
    execute(s"GRANT WRITE ON GRAPH $DEFAULT_DATABASE_NAME ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (n:A {name: 'b'}) RETURN 1 AS dummy", resultHandler = (row, _) => {
      row.get("dummy") should be(1)
    }) should be(1)

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))

    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "CREATE (n:B {name: 'a'}) RETURN 1 AS dummy")
    } should have message "Write operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    selectDatabase("foo")
    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "b")))
  }

  test("should not be able to create nodes when denied WRITE privilege to custom role for a specific database") {
    // GIVEN
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:B {name:'b'})")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name:'a'})")

    setupUserWithCustomRole()

    // WHEN
    execute("GRANT WRITE ON GRAPH * ELEMENTS * (*) TO custom")
    execute("DENY WRITE ON GRAPH foo ELEMENTS * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "CREATE (:A {name: 'b'}) RETURN 1 AS dummy", resultHandler = (row, _) => {
      row.get("dummy") should be(1)
    }) should be(1)

    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "a"), Map("n.name" -> "b")))

    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "CREATE (:B {name: 'a'}) RETURN 1 AS dummy")
    } should have message "Write operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    selectDatabase("foo")
    execute("MATCH (n) RETURN n.name").toSet should be(Set(Map("n.name" -> "b")))
  }

  test("should be able to set and remove a label with just WRITE privilege") {
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT WRITE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Label')")
    execute("CREATE ({name:'Bob'})")

    executeOnDefault("joe", "soap", "MATCH (n) SET n:Label")
    execute("MATCH (n:Label) RETURN n.name").toSet should be(Set(Map("n.name" -> "Bob")))

    executeOnDefault("joe", "soap", "MATCH (n) REMOVE n:Label")
    execute("MATCH (n:Label) RETURN n.name").toSet should be(Set.empty)
  }

}
