/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction.Type

// Tests for actual behaviour of authorization rules for restricted users based on element privileges
class ElementsAndMixedPrivilegeEnforcementAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("read privilege for element should not imply traverse privilege") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name: 'n1'})-[:A {name:'r'}]->(:A {name: 'n2'})")
    val query = "MATCH (n1)-[r]->(n2) RETURN n1.name, r.name, n2.name"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {name} ON GRAPH * ELEMENTS A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * ELEMENTS A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(("n1", "r", "n2"))
    }) should be(1)
  }

  test("should see correct things when granted element privileges") {
    // GIVEN
    setupUserWithCustomRole(access = false)
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name: 'a1'})-[:A {name: 'ra1'}]->(:A {name: 'a2'})")
    execute("CREATE (:A {name: 'a3'})-[:B {name: 'rb1'}]->(:A {name: 'a4'})")
    execute("CREATE (:B {name: 'b1'})-[:A {name: 'ra2'}]->(:B {name: 'b2'})")
    execute("CREATE (:B {name: 'b3'})-[:B {name: 'rb2'}]->(:B {name: 'b4'})")
    val query = "MATCH (n1)-[r]->(n2) RETURN n1.name, r.name, n2.name ORDER BY n1.name, r.name"

    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", query)
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ACCESS ON DATABASE * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * ELEMENTS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be((null, null, null))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {name} ON GRAPH * ELEMENTS A, B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(("a1", "ra1", "a2"))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH {name} ON GRAPH * ELEMENTS B TO custom")

    // THEN
    val expected1 = Seq(
      ("a1", "ra1", "a2"),
      ("a3", "rb1", "a4"),
      ("b1", "ra2", "b2"),
      ("b3", "rb2", "b4")
    )
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(expected1(index))
    }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE READ {name} ON GRAPH * ELEMENTS A FROM custom")

    // THEN
    val expected2 = Seq(
      ("b1", null, "b2"),
      ("b3", "rb2", "b4"),
      (null, "rb1", null),
      (null, null, null)
    )
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(expected2(index))
    }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE READ {name} ON GRAPH * ELEMENTS B FROM custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be((null, null, null))
    }) should be(4) // TODO: should be 1 when revoking MATCH also revokes traverse

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * ELEMENTS C TO custom") // unrelated privilege, just so we don't remove all access
    execute("REVOKE TRAVERSE ON GRAPH * ELEMENTS A, B FROM custom") // TODO: won't work when revoking MATCH also revokes traverse, need to re-add traverse B

    // THEN
    executeOnDefault("joe", "soap", query) should be(0)
  }

  test("should rollback transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    val tx = graph.beginTransaction(Type.explicit, LoginContext.AUTH_DISABLED)
    try {
      val result: Result = tx.execute("GRANT TRAVERSE ON GRAPH * NODES A,B TO custom")
      result.accept(_ => true)
      tx.rollback()
    } finally {
      tx.close()
    }
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("should get correct result from propertyKeys procedure depending on read privileges") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)-[:X]->(:B)<-[:Y]-(:C)")
    execute("MATCH (n:A) SET n.prop1 = 1")
    execute("MATCH (n:A) SET n.prop2 = 2")
    execute("MATCH (n:A) SET n.prop3 = 3")
    execute("MATCH (n:B) SET n.prop3 = 3")
    execute("MATCH (n:B) SET n.prop4 = 4")
    execute("MATCH (n:C) SET n.prop5 = 5")
    execute("MATCH (n:C) SET n.prop1 = 1")
    execute("MATCH ()<-[x:X]-() SET x.prop6 = 6")
    execute("MATCH ()<-[x:Y]-() SET x.prop7 = 7")
    execute("MATCH (n:A) REMOVE n.prop2") // -> unused prop2

    val query = "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey ORDER BY propertyKey"

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES ignore TO custom")

    // THEN
    val all = List("prop1", "prop2", "prop3", "prop4", "prop5", "prop6", "prop7")
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * NODES A TO custom")

    // THEN
    // expect no change
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {prop3} ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * NODES C TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {prop5} ON GRAPH * NODES * TO custom") // won't do anything new because there could be a REL that has this propKey

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {prop5} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val withoutFive = List("prop1", "prop2", "prop3", "prop4", "prop6", "prop7")
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(withoutFive(index))
    }) should be(withoutFive.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * NODES * TO custom") // won't do anything new because there could be a REL that has this propKey

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(withoutFive(index))
    }) should be(withoutFive.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("Should be empty because all properties are denied on everything")
    }) should be(0)
  }

  test("Counting queries should work with restricted user") {
    // The two queries are used by the browser
    val countingNodesQuery = "MATCH () RETURN { name:'nodes', data:count(*) } AS result"
    val countingRelsQuery = "MATCH ()-[]->() RETURN { name:'relationships', data: count(*)} AS result"

    // Given
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Person)-[:WROTE]->(:Letter)<-[:HAS_STAMP]-(:Stamp)")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER tim SET PASSWORD '123' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO tim")
    execute("GRANT ACCESS ON DATABASE * TO role")
    execute("GRANT MATCH {*} ON GRAPH * ELEMENTS * TO role")
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIP WROTE TO role")

    // RELS
    selectDatabase(DEFAULT_DATABASE_NAME)
    // When & Then

    // unrestricted:
    execute(countingRelsQuery).toList should be(List(Map("result" -> Map("data" -> 2, "name" -> "relationships"))))

    // restricted
    executeOnDefault("tim", "123", countingRelsQuery,
      resultHandler = (row, _) => {
        val result = row.get("result").asInstanceOf[util.Map[String, AnyRef]]
        result.get("data") should be (1L)
        result.get("name") should be ("relationships")
      }
    ) should be(1)

    // Given
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY TRAVERSE ON GRAPH * NODES Person TO role")

    // NODES
    selectDatabase(DEFAULT_DATABASE_NAME)
    // When & Then

    // unrestricted:
    execute(countingNodesQuery).toList should be(List(Map("result" -> Map("data" -> 3, "name" -> "nodes"))))

    // restricted
    executeOnDefault("tim", "123", countingNodesQuery,
      resultHandler = (row, _) => {
        val result = row.get("result").asInstanceOf[util.Map[String, AnyRef]]
        result.get("data") should be (2L)
        result.get("name") should be ("nodes")
      }
    ) should be(1)
  }
}
