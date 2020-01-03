/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.{RelationshipType, Result}
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction.Type

import scala.collection.JavaConverters._

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
    val tx = graph.beginTransaction(Type.EXPLICIT, LoginContext.AUTH_DISABLED)
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
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("Should be empty because no properties are whitelisted")
    }) should be(0)

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
    execute("DENY READ {prop5} ON GRAPH * NODES * TO custom")

    // THEN
    val withoutFive = all.filter(_ != "prop5")
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(withoutFive(index))
    }) should be(withoutFive.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {prop5} ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(withoutFive(index))
    }) should be(withoutFive.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("DENY READ {*} ON GRAPH * NODES * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("Should be empty because all properties are denied on nodes and not whitelisted on relationships")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO custom")

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

  test("should see label and type but not properties when returning path with only full traverse privilege") {
    // Given
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Person {name: 'Alice'})-[:KNOWS {since: '2019'}]->(:Person {name: 'Bob'})")

    // When
    executeOnDefault("joe", "soap", "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
      val path = row.getPath("p")
      val node1 = path.startNode()
      val node2 = path.endNode()
      val relationship = path.lastRelationship()

      // Then
      node1.labels should be(List("Person"))
      node1.getAllProperties.asScala should be(Map.empty)
      node2.labels should be(List("Person"))
      node2.getAllProperties.asScala should be(Map.empty)
      relationship.isType(RelationshipType.withName("KNOWS"))
      relationship.getAllProperties.asScala should be(Map.empty)
    }) should be(1)
  }

  test("should see label, type and properties when returning path with full traverse and read privileges") {
    // Given
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT READ {*} ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Person {name: 'Alice'})-[:KNOWS {since: '2019'}]->(:Person {name: 'Bob'})")

    // When
    executeOnDefault("joe", "soap", "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
      val path = row.getPath("p")
      val node1 = path.startNode()
      val node2 = path.endNode()
      val relationship = path.lastRelationship()

      // Then
      node1.labels should be(List("Person"))
      node1.getAllProperties.asScala should be(Map("name" -> "Alice"))
      node2.labels should be(List("Person"))
      node2.getAllProperties.asScala should be(Map("name" -> "Bob"))
      relationship.isType(RelationshipType.withName("KNOWS"))
      relationship.getAllProperties.asScala should be(Map("since" -> "2019"))
    }) should be(1)
  }

  test("should see allowed label and type but not properties when returning path with only traverse privilege") {
    // Given
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES Person, Dog TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP KNOWS TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """CREATE (a:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019'}]->(:Person {name: 'Bob', age: 23}),
        |(a)-[:KNOWS {since: '2010'}]->(:Friend {name: 'Charlie', age: 27}),
        |(a)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis'})
        |""".stripMargin)

    // When
    executeOnDefault("joe", "soap", "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
      val path = row.getPath("p")
      val node1 = path.startNode()
      val node2 = path.endNode()
      val relationship = path.lastRelationship()

      // Then
      node1.labels should be(List("Person"))
      node1.getAllProperties.asScala should be(Map.empty)
      node2.labels should be(List("Person"))
      node2.getAllProperties.asScala should be(Map.empty)
      relationship.isType(RelationshipType.withName("KNOWS"))
      relationship.getAllProperties.asScala should be(Map.empty)
    }) should be(1)
  }

  test("should see allowed label, type and properties when returning path with traverse and read privileges") {
    // Given
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES Person, Dog TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP KNOWS TO custom")
    execute("GRANT READ {name} ON GRAPH * NODES Person, Dog TO custom")
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIP KNOWS TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """CREATE (a:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019'}]->(:Person {name: 'Bob', age: 23}),
        |(a)-[:KNOWS {since: '2010'}]->(:Friend {name: 'Charlie', age: 27}),
        |(a)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis'})
        |""".stripMargin)

    // When
    executeOnDefault("joe", "soap", "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
      val path = row.getPath("p")
      val node1 = path.startNode()
      val node2 = path.endNode()
      val relationship = path.lastRelationship()

      // Then
      node1.labels should be(List("Person"))
      node1.getAllProperties.asScala should be(Map("name" -> "Alice"))
      node2.labels should be(List("Person"))
      node2.getAllProperties.asScala should be(Map("name" -> "Bob"))
      relationship.isType(RelationshipType.withName("KNOWS"))
      relationship.getAllProperties.asScala should be(Map("since" -> "2019"))
    }) should be(1)
  }

  test("should see not denied label and type but not properties when returning path with only traverse privilege") {
    // Given
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES Friend TO custom")
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIP HAS_PET TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """CREATE (a:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019', met_at: 'library'}]->(:Person {name: 'Bob', age: 23}),
        |(a)-[:KNOWS {since: '2010'}]->(:Person:Friend {name: 'Charlie', age: 27}),
        |(a)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis'})
        |""".stripMargin)

    // When
    executeOnDefault("joe", "soap", "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
      val path = row.getPath("p")
      val node1 = path.startNode()
      val node2 = path.endNode()
      val relationship = path.lastRelationship()

      // Then
      node1.labels should be(List("Person"))
      node1.getAllProperties.asScala should be(Map.empty)
      node2.labels should be(List("Person"))
      node2.getAllProperties.asScala should be(Map.empty)
      relationship.isType(RelationshipType.withName("KNOWS"))
      relationship.getAllProperties.asScala should be(Map.empty)
    }) should be(1)
  }

  test("should see not denied label, type and properties when returning path with traverse and read privileges") {
    // Given
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES Friend TO custom")
    execute("DENY TRAVERSE ON GRAPH * RELATIONSHIP HAS_PET TO custom")
    execute("GRANT READ {*} ON GRAPH * TO custom")
    execute("DENY READ {age} ON GRAPH * NODES Person TO custom")
    execute("DENY READ {met_at} ON GRAPH * RELATIONSHIP KNOWS TO custom")
    execute("DENY READ {*} ON GRAPH * RELATIONSHIP HAS_PET TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """CREATE (a:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019', met_at: 'library'}]->(:Person {name: 'Bob', age: 23}),
        |(a)-[:KNOWS {since: '2010'}]->(:Person:Friend {name: 'Charlie', age: 27}),
        |(a)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis'})
        |""".stripMargin)

    // When
    executeOnDefault("joe", "soap", "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
      val path = row.getPath("p")
      val node1 = path.startNode()
      val node2 = path.endNode()
      val relationship = path.lastRelationship()

      // Then
      node1.labels should be(List("Person"))
      node1.getAllProperties.asScala should be(Map("name" -> "Alice"))
      node2.labels should be(List("Person"))
      node2.getAllProperties.asScala should be(Map("name" -> "Bob"))
      relationship.isType(RelationshipType.withName("KNOWS"))
      relationship.getAllProperties.asScala should be(Map("since" -> "2019"))
    }) should be(1)
  }

  test("should see labels and types but not properties for nodes with only traverse privilege when returning path") {
    // Given
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT READ {*} ON GRAPH * NODES Dog TO custom")
    execute("GRANT READ {name} ON GRAPH * NODES Friend TO custom")
    execute("GRANT READ {*} ON GRAPH * RELATIONSHIP HAS_PET TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(// Alice knows Bob, Bob knows Charlie, Charlie has pet Dennis
      """CREATE (:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019', met_at: 'library'}]->(b:Person {name: 'Bob', age: 25}),
        |(b)-[:KNOWS {since: '2010'}]->(c:Person:Friend {name: 'Charlie', age: 27}),
        |(c)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis', age: 5})
        |""".stripMargin)

    // When
    executeOnDefault("joe", "soap", "MATCH p=()-[*3]->() RETURN p", resultHandler = (row, _) => {
      val path = row.getPath("p")
      val nodes = path.nodes().asScala
      val relationships = path.relationships().asScala

      // Then
      nodes.flatMap(n => Set((n.getAllProperties.asScala, n.labels.toSet))).toList should be(List(
        (Map.empty, Set("Person")),
        (Map.empty, Set("Person")),
        (Map("name" -> "Charlie"), Set("Person", "Friend")),
        (Map("name" -> "Dennis", "age" -> 5), Set("Dog"))
      ))
      relationships.flatMap(r => Set((r.getAllProperties.asScala, r.getType))).toList should be(List(
        (Map.empty, RelationshipType.withName("KNOWS")),
        (Map.empty, RelationshipType.withName("KNOWS")),
        (Map("since" -> "2018"), RelationshipType.withName("HAS_PET"))
      ))
    }) should be(1)
  }

  test("should see labels and types but not properties for nodes with traverse and denied read privilege when returning path") {
    // Given
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("DENY READ {*} ON GRAPH * NODES Person TO custom")
    execute("DENY READ {age} ON GRAPH * NODES Friend TO custom")
    execute("DENY READ {since} ON GRAPH * RELATIONSHIP KNOWS TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(// Alice knows Bob, Bob knows Charlie, Charlie has pet Dennis
      """CREATE (:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019', met_at: 'library'}]->(b:Person {name: 'Bob', age: 25}),
        |(b)-[:KNOWS {since: '2010'}]->(c:Person:Friend {name: 'Charlie', age: 27}),
        |(c)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis', age: 5})
        |""".stripMargin)

    // When
    executeOnDefault("joe", "soap", "MATCH p=()-[*3]->() RETURN p", resultHandler = (row, _) => {
      val path = row.getPath("p")
      val nodes = path.nodes().asScala
      val relationships = path.relationships().asScala

      // Then
      nodes.flatMap(n => Set((n.getAllProperties.asScala, n.labels.toSet))).toList should be(List(
        (Map.empty, Set("Person")),
        (Map.empty, Set("Person")),
        (Map.empty, Set("Person", "Friend")),
        (Map("name" -> "Dennis", "age" -> 5), Set("Dog"))
      ))
      relationships.flatMap(r => Set((r.getAllProperties.asScala, r.getType))).toSet should be(Set(
        (Map("met_at" -> "library"), RelationshipType.withName("KNOWS")),
        (Map.empty, RelationshipType.withName("KNOWS")),
        (Map("since" -> "2018"), RelationshipType.withName("HAS_PET"))
      ))
    }) should be(1)
  }
}
