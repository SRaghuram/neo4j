/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction.Type

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter

// Tests for actual behaviour of authorization rules for restricted users based on element privileges
class ElementsAndMixedPrivilegeEnforcementAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  override protected def onNewGraphDatabase(): Unit = clearPublicRole()

  test("read privilege for element should not imply traverse privilege") {
    // GIVEN
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name: 'n1'})-[:A {name:'r'}]->(:A {name: 'n2'})")
    val query = "MATCH (n1)-[r]->(n2) RETURN n1.name, r.name, n2.name"

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT READ {name} ON GRAPH * ELEMENTS A (*) TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT TRAVERSE ON GRAPH * ELEMENTS A (*) TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (row, _) => {
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
      executeOnDBMSDefault(username, password, query)
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT ACCESS ON DATABASE * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT TRAVERSE ON GRAPH * ELEMENTS A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (row, _) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be((null, null, null))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT READ {name} ON GRAPH * ELEMENTS A, B TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (row, _) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(("a1", "ra1", "a2"))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT MATCH {name} ON GRAPH * ELEMENTS B TO $roleName")

    // THEN
    val expected1 = Seq(
      ("a1", "ra1", "a2"),
      ("a3", "rb1", "a4"),
      ("b1", "ra2", "b2"),
      ("b3", "rb2", "b4")
    )
    executeOnDBMSDefault(username, password, query, resultHandler = (row, index) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(expected1(index))
    }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE READ {name} ON GRAPH * ELEMENTS A FROM $roleName")

    // THEN
    val expected2 = Seq(
      ("b1", null, "b2"),
      ("b3", "rb2", "b4"),
      (null, "rb1", null),
      (null, null, null)
    )
    executeOnDBMSDefault(username, password, query, resultHandler = (row, index) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be(expected2(index))
    }) should be(4)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE MATCH {name} ON GRAPH * ELEMENTS B FROM $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (row, _) => {
      (row.get("n1.name"), row.get("r.name"), row.get("n2.name")) should be((null, null, null))
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE TRAVERSE ON GRAPH * ELEMENTS A FROM $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query) should be(0)
  }

  test("should rollback transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"CREATE ROLE $roleName")
    val tx = graph.beginTransaction(Type.EXPLICIT, LoginContext.AUTH_DISABLED)
    try {
      val result: Result = tx.execute(s"GRANT TRAVERSE ON GRAPH * NODES A,B TO $roleName")
      result.accept(_ => true)
      tx.rollback()
    } finally {
      tx.close()
    }
    execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
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
    val all = List("prop1", "prop2", "prop3", "prop4", "prop5", "prop6", "prop7")

    val query = "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey ORDER BY propertyKey"

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT EXECUTE PROCEDURE db.propertyKeys ON DBMS TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * NODES ignore TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (_, _) => {
      fail("Should be empty because no properties are whitelisted")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT READ {*} ON GRAPH * NODES A TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"DENY READ {prop3} ON GRAPH * NODES B TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"DENY READ {*} ON GRAPH * NODES C TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(all(index))
    }) should be(all.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"DENY READ {prop5} ON GRAPH * NODES * TO $roleName")

    // THEN
    val withoutFive = all.filter(_ != "prop5")
    executeOnDBMSDefault(username, password, query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(withoutFive(index))
    }) should be(withoutFive.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"DENY READ {prop5} ON GRAPH * RELATIONSHIPS * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(withoutFive(index))
    }) should be(withoutFive.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"DENY READ {*} ON GRAPH * NODES * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (_, _) => {
      fail("Should be empty because all properties are denied on nodes and not whitelisted on relationships")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT READ {*} ON GRAPH * RELATIONSHIPS * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (row, index) => {
      row.get("propertyKey") should be(withoutFive(index))
    }) should be(withoutFive.size)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"DENY READ {*} ON GRAPH * RELATIONSHIPS * TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, query, resultHandler = (_, _) => {
      fail("Should be empty because all properties are denied on everything")
    }) should be(0)
  }

  test("should see label and type but not properties when returning path with only full traverse privilege") {
    // Given
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Person {name: 'Alice'})-[:KNOWS {since: '2019'}]->(:Person {name: 'Bob'})")

    // When
    executeOnDBMSDefault(username, password, "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
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
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"GRANT READ {*} ON GRAPH * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Person {name: 'Alice'})-[:KNOWS {since: '2019'}]->(:Person {name: 'Bob'})")

    // When
    executeOnDBMSDefault(username, password, "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
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
    execute(s"GRANT TRAVERSE ON GRAPH * NODES Person, Dog TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIP KNOWS TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """CREATE (a:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019'}]->(:Person {name: 'Bob', age: 23}),
        |(a)-[:KNOWS {since: '2010'}]->(:Friend {name: 'Charlie', age: 27}),
        |(a)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis'})
        |""".stripMargin)

    // When
    executeOnDBMSDefault(username, password, "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
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
    execute(s"GRANT TRAVERSE ON GRAPH * NODES Person, Dog TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIP KNOWS TO $roleName")
    execute(s"GRANT READ {name} ON GRAPH * NODES Person, Dog TO $roleName")
    execute(s"GRANT READ {*} ON GRAPH * RELATIONSHIP KNOWS TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """CREATE (a:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019'}]->(:Person {name: 'Bob', age: 23}),
        |(a)-[:KNOWS {since: '2010'}]->(:Friend {name: 'Charlie', age: 27}),
        |(a)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis'})
        |""".stripMargin)

    // When
    executeOnDBMSDefault(username, password, "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
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

  test("should see not denied label and type and no properties when returning path with only traverse privilege") {
    // Given
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * NODES Friend TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * RELATIONSHIP HAS_PET TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """CREATE (a:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019', met_at: 'library'}]->(:Person {name: 'Bob', age: 23}),
        |(a)-[:KNOWS {since: '2010'}]->(:Person:Friend {name: 'Charlie', age: 27}),
        |(a)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis'})
        |""".stripMargin)

    // When
    executeOnDBMSDefault(username, password, "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
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
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * NODES Friend TO $roleName")
    execute(s"DENY TRAVERSE ON GRAPH * RELATIONSHIP HAS_PET TO $roleName")
    execute(s"GRANT READ {*} ON GRAPH * TO $roleName")
    execute(s"DENY READ {age} ON GRAPH * NODES Person TO $roleName")
    execute(s"DENY READ {met_at} ON GRAPH * RELATIONSHIP KNOWS TO $roleName")
    execute(s"DENY READ {*} ON GRAPH * RELATIONSHIP HAS_PET TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """CREATE (a:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019', met_at: 'library'}]->(:Person {name: 'Bob', age: 23}),
        |(a)-[:KNOWS {since: '2010'}]->(:Person:Friend {name: 'Charlie', age: 27}),
        |(a)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis'})
        |""".stripMargin)

    // When
    executeOnDBMSDefault(username, password, "MATCH p=()-[]->() RETURN p", resultHandler = (row, _) => {
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
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"GRANT READ {*} ON GRAPH * NODES Dog TO $roleName")
    execute(s"GRANT READ {name} ON GRAPH * NODES Friend TO $roleName")
    execute(s"GRANT READ {*} ON GRAPH * RELATIONSHIP HAS_PET TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute( // Alice knows Bob, Bob knows Charlie, Charlie has pet Dennis
      """CREATE (:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019', met_at: 'library'}]->(b:Person {name: 'Bob', age: 25}),
        |(b)-[:KNOWS {since: '2010'}]->(c:Person:Friend {name: 'Charlie', age: 27}),
        |(c)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis', age: 5})
        |""".stripMargin)

    // When
    executeOnDBMSDefault(username, password, "MATCH p=()-[*3]->() RETURN p", resultHandler = (row, _) => {
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
    execute(s"GRANT MATCH {*} ON GRAPH * TO $roleName")
    execute(s"DENY READ {*} ON GRAPH * NODES Person TO $roleName")
    execute(s"DENY READ {age} ON GRAPH * NODES Friend TO $roleName")
    execute(s"DENY READ {since} ON GRAPH * RELATIONSHIP KNOWS TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute( // Alice knows Bob, Bob knows Charlie, Charlie has pet Dennis
      """CREATE (:Person {name: 'Alice', age: 28})-[:KNOWS {since: '2019', met_at: 'library'}]->(b:Person {name: 'Bob', age: 25}),
        |(b)-[:KNOWS {since: '2010'}]->(c:Person:Friend {name: 'Charlie', age: 27}),
        |(c)-[:HAS_PET {since: '2018'}]->(:Dog {name: 'Dennis', age: 5})
        |""".stripMargin)

    // When
    executeOnDBMSDefault(username, password, "MATCH p=()-[*3]->() RETURN p", resultHandler = (row, _) => {
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
