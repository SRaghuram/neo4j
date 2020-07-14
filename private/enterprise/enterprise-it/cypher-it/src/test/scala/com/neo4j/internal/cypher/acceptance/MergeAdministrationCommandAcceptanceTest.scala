/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.security.AuthorizationViolationException

class MergeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    // GIVEN
    execute("CREATE ROLE custom")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT MERGE { prop1 } ON GRAPH * TO custom" -> 2,
      "REVOKE GRANT MERGE { prop1 } ON GRAPH * RELATIONSHIPS * FROM custom" -> 1,
      "GRANT MERGE { prop1, prop2 } ON GRAPH * TO custom" -> 3,
      "REVOKE MERGE { prop1, prop2 } ON GRAPH * NODES * FROM custom" -> 2,
    ))
  }

  // Tests for granting merge privileges

  test("should grant merge privilege to custom role for all graphs and a specific property") {
    // GIVEN
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MERGE {a} ON GRAPH * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(merge).role("custom").property("a").node("*").map,
      granted(merge).role("custom").property("a").relationship("*").map,
    ))
  }

  test("should grant merge privilege to custom role for a specific graph and all properties") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MERGE {*} ON GRAPH foo TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(merge).role("custom").graph("foo").node("*").map,
      granted(merge).role("custom").graph("foo").relationship("*").map,
    ))
  }

  test("should grant merge privilege to custom role for the default graph and all properties") {
    // GIVEN
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MERGE {*} ON DEFAULT GRAPH TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(merge).role("custom").graph(DEFAULT).node("*").map,
      granted(merge).role("custom").graph(DEFAULT).relationship("*").map,
    ))
  }

  test("should grant merge privilege to custom role for specific nodes and relationships") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MERGE {*} ON GRAPH * NODES a, b TO custom")
    execute("GRANT MERGE {prop} ON GRAPH foo RELATIONSHIP c TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(merge).role("custom").graph("*").node("a").map,
      granted(merge).role("custom").graph("*").node("b").map,
      granted(merge).role("custom").graph("foo").property("prop").relationship("c").map,
    ))
  }

  test("should grant merge to custom role for multiple graphs and multiple properties") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")

    // WHEN
    execute("GRANT MERGE {prop1, prop2} ON GRAPH foo, bar NODES * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(merge).role("custom").graph("foo").property("prop1").node("*").map,
      granted(merge).role("custom").graph("bar").property("prop1").node("*").map,
      granted(merge).role("custom").graph("foo").property("prop2").node("*").map,
      granted(merge).role("custom").graph("bar").property("prop2").node("*").map
    ))
  }

  test("should grant merge privilege to custom role for specific graph and all labels using parameter") {
    // GIVEN
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MERGE {*} ON GRAPH $graph NODES * TO custom", Map("graph" -> DEFAULT_DATABASE_NAME))

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(merge).graph(DEFAULT_DATABASE_NAME).role("custom").node("*").map,
    ))
  }

  test("should grant merge privilege to multiple roles in a single grant") {
    // GIVEN
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")
    execute("CREATE DATABASE foo")

    // WHEN
    execute("GRANT MERGE { prop } ON GRAPH foo RELATIONSHIPS * TO role1, role2, role3")

    // THEN
    val expected: Seq[PrivilegeMapBuilder] = Seq(
      granted(merge).graph("foo").property("prop").relationship("*")
    )

    execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
    execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
    execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
  }

  // Tests for revoke grant and revoke deny write privileges

  test("should revoke correct grant merge privilege different graphs") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT MERGE {*} ON GRAPH * NODES * TO custom")
    execute("GRANT MERGE {*} ON GRAPH foo NODES * TO custom")
    execute("GRANT MERGE {*} ON GRAPH bar NODES * TO custom")

    // WHEN
    execute("REVOKE GRANT MERGE {*} ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(merge).role("custom").node("*").map,
      granted(merge).role("custom").graph("bar").node("*").map,
    ))

    // WHEN
    execute("REVOKE GRANT MERGE {*} ON GRAPH * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(merge).role("custom").graph("bar").node("*").map,
    ))
  }

  test("should be able to revoke merge if only having grant") {
    // GIVEN
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT MERGE { prop } ON GRAPH * NODES * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(merge).role("custom").property("prop").node("*").map,
    ))

    // WHEN
    execute("REVOKE MERGE { prop } ON GRAPH * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)

  }

  test("should do nothing when revoking grant merge privilege from non-existent role") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT MERGE {*} ON GRAPH * TO custom")

    // WHEN
    execute("REVOKE GRANT MERGE {*} ON GRAPH * FROM wrongRole")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(merge).role("custom").node("*").map,
      granted(merge).role("custom").relationship("*").map,
    ))
  }

  test("should do nothing when revoking grant merge privilege not granted to role") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE ROLE role")
    execute("GRANT MERGE { prop } ON GRAPH * TO custom")

    // WHEN
    execute("REVOKE GRANT MERGE { prop } ON GRAPH * FROM role")
    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("revoke should revoke grant") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE ROLE role")
    execute("GRANT MERGE { prop } ON GRAPH * TO custom")

    // WHEN
    execute("REVOKE MERGE { prop } ON GRAPH * FROM custom")
    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(empty)
  }

  test("revoke grant should revoke grant") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("GRANT MERGE { prop } ON GRAPH * TO custom")

    // WHEN
    execute("REVOKE GRANT MERGE { prop } ON GRAPH * FROM custom")
    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(empty)
  }

  test("revoke grant on default database should revoke grant") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("GRANT MERGE { prop } ON DEFAULT GRAPH TO custom")

    // WHEN
    execute("REVOKE GRANT MERGE { prop } ON DEFAULT GRAPH FROM custom")
    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(empty)
  }

  test("revoke something not granted on default database should do nothing") {
    // GIVEN
    execute("CREATE ROLE custom")

    // WHEN
    execute("REVOKE GRANT MERGE { prop } ON DEFAULT GRAPH FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(empty)
  }

  test("revoke something not granted should do nothing") {
    // GIVEN
    execute("CREATE ROLE custom")

    // WHEN
    execute("REVOKE GRANT MERGE { prop } ON GRAPH * FROM custom")
    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(empty)
  }

  test("deny merge should return a helpful error message") {
    // GIVEN
    execute("CREATE ROLE custom")

    // WHEN
    val exception = the [SyntaxException] thrownBy {
      execute("DENY MERGE { prop } ON GRAPH * TO custom")
    }
    exception.getMessage should startWith("`DENY MERGE` is not supported. Use `DENY SET PROPERTY` and `DENY CREATE` instead.")
    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(empty)
  }

  test("revoke deny merge should return a helpful error message") {
    // GIVEN
    execute("CREATE ROLE custom")

    // WHEN
    val exception = the [SyntaxException] thrownBy {
      execute("REVOKE DENY MERGE { prop } ON GRAPH * FROM custom")
    }
    exception.getMessage should startWith("`DENY MERGE` is not supported. Use `DENY SET PROPERTY` and `DENY CREATE` instead.")
    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(empty)
  }

  // Test the merge

  test("merge should allow a merge to update a property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { * } ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('prop')")
    execute("CREATE ()")

    // WHEN
    executeOnDefault("joe", "soap", s"MERGE (n) ON CREATE SET n.prop = 'created' on MATCH SET n.prop='matched'")

    // THEN
    execute("MATCH (n) RETURN n.prop").toSet shouldBe Set(Map("n.prop" -> "matched"))
  }

  test("merge should allow a merge to create a node") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { * } ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('prop')")

    // WHEN
    executeOnDefault("joe", "soap", s"MERGE (n) ON CREATE SET n.prop = 'created' on MATCH SET n.prop='matched'")

    // THEN
    execute("MATCH (n) RETURN n.prop").toSet shouldBe Set(Map("n.prop" -> "created"))
  }

  test("merge should not allow a merge to create a node with the wrong property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { prop } ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('prop')")
    execute("CALL db.createProperty('wrong')")

    // WHEN
    val exception = the [AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", s"MERGE (n) ON CREATE SET n.wrong = 'created' on MATCH SET n.wrong='matched'")
    }
    exception.getMessage should startWith("Set property for property 'wrong' is not allowed for user")

    // THEN
    execute("MATCH (n) RETURN n.prop").toSet shouldBe empty
  }

  test("merge should not allow a merge to create a node with the wrong label") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { prop } ON GRAPH * NODES Foo TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('prop')")
    execute("CALL db.createLabel('Bar')")

    // WHEN
    val exception = the [AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", s"MERGE (n:Bar) ON CREATE SET n.prop = 'created' on MATCH SET n.prop='matched'")
    }
    exception.getMessage should startWith("Create node with labels 'Bar' is not allowed for user")

    // THEN
    execute("MATCH (n) RETURN n.prop").toSet shouldBe empty
  }

  test("merge should not allow a merge to update a wrong property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { prop } ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('prop')")
    execute("CALL db.createProperty('wrong')")
    execute("CREATE ()")

    // WHEN
    val exception = the [AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", s"MERGE (n) ON CREATE SET n.wrong = 'created' on MATCH SET n.wrong='matched'")
    }
    exception.getMessage should startWith("Set property for property 'wrong' is not allowed for user")

    // THEN
    execute("MATCH (n) RETURN n.prop").toSet shouldBe Set(Map("n.prop" -> null))
  }

  test("merge should not allow a merge to update a node with the wrong label") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { * } ON GRAPH * NODES Foo TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('prop')")
    execute("CALL db.createLabel('Bar')")
    execute("CREATE (:Bar)")

    // WHEN
    val exception = the [AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", s"MERGE (n:Bar) ON CREATE SET n.prop = 'created' on MATCH SET n.prop='matched'")
    }
    exception.getMessage should startWith("Set property for property 'prop' is not allowed for user")

    // THEN
    execute("MATCH (n) RETURN n.prop").toSet shouldBe Set(Map("n.prop" -> null))
  }

  test("merge should allow create to create a node with property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { * } ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('prop')")
    execute("CALL db.createLabel('Foo')")

    // WHEN
    executeOnDefault("joe", "soap", s"CREATE (:Foo {prop:'value'})")

    // THEN
    execute("MATCH (n) RETURN n.prop").toSet shouldBe Set(Map("n.prop" -> "value"))
  }

  test("merge should allow set property on existing node") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { * } ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('prop')")
    execute("CALL db.createLabel('Foo')")
    execute("CREATE()")

    // WHEN
    executeOnDefault("joe", "soap", s"MATCH (n) SET n.prop='value'")

    // THEN
    execute("MATCH (n) RETURN n.prop").toSet shouldBe Set(Map("n.prop" -> "value"))
  }

  test("merge should not update nodes it cannot see") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { * } ON GRAPH * TO custom")
    execute("DENY TRAVERSE ON GRAPH * NODES Foo TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('prop')")
    execute("CREATE (:Foo)")

    // WHEN
    executeOnDefault("joe", "soap", s"MERGE (n:Foo) ON CREATE SET n.prop = 'created' on MATCH SET n.prop='matched'")

    // THEN
    execute("MATCH (n) RETURN n.prop").toSet shouldBe Set(Map("n.prop" -> null),Map("n.prop" -> "created"))
  }

  test("merge should match nodes with properties") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { * } ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('visible')")
    execute("CREATE (:Foo {visible:1})")

    // WHEN
    Range(0, 10).foreach { _ =>
      executeOnDefault("joe", "soap", "MERGE (n:Foo {visible:1}) ON MATCH SET n.visible = n.visible + 1")
    }

    // THEN
    val expected = Range(0, 5).map(_ => Map[String, Int]("n.visible" -> 2)) :+ Map("n.visible" -> 1)
    execute("MATCH (n:Foo) RETURN n.visible").toList shouldBe expected.toList
  }

  test("merge should not match nodes with properties it cannot see") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { * } ON GRAPH * TO custom")
    execute("DENY READ {invisible} ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('visible')")
    execute("CALL db.createProperty('invisible')")
    execute("CALL db.createLabel('Foo')")
    execute("CREATE (:Foo {invisible:1, visible:1})")

    // WHEN
    Range(0, 10).foreach { _ =>
      executeOnDefault("joe", "soap", "MERGE (n:Foo {invisible:1}) ON CREATE SET n.visible = 1 on MATCH SET n.visible = n.visible + 1")
    }

    // THEN
    val expected = Range(0, 11).map(_ => Map[String, Int]("n.visible" -> 1, "n.invisible" -> 1))
    execute("MATCH (n:Foo) RETURN n.visible, n.invisible").toList shouldBe expected.toList
  }

  test("merge should create a relationship") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { * } ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('R1')")
    execute("CREATE (:Foo)")
    execute("CREATE (:Bar)")

    // WHEN
    executeOnDefault("joe", "soap", s"MATCH (a:Foo), (b:Bar) MERGE (a)-[r:R1]->(b)")

    // THEN
    execute("MATCH (a)-[r:R1]->(b) RETURN r").toSet should have size 1
  }

  test("merge should create a relationship with property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { prop } ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('R1')")
    execute("CALL db.createProperty('prop')")
    execute("CREATE (:Foo)")
    execute("CREATE (:Bar)")

    // WHEN
    executeOnDefault("joe", "soap", s"MATCH (a:Foo), (b:Bar) MERGE (a)-[r:R1{prop:'value'}]->(b)")

    // THEN
    execute("MATCH (a)-[r:R1]->(b) RETURN r.prop").toSet should be(Set(Map("r.prop" -> "value")))
  }

  test("merge should not create wrong relationship") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { * } ON GRAPH * RELATIONSHIPS R1 TO custom")
    execute("GRANT MERGE { * } ON GRAPH * NODES Foo,Bar TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('R1')")
    execute("CALL db.createRelationshipType('R2')")
    execute("CREATE (:Foo)")
    execute("CREATE (:Bar)")

    // WHEN
    val exception = the [AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", s"MATCH (a:Foo), (b:Bar) MERGE (a)-[r:R2]->(b)")
    }
    exception.getMessage should startWith("Create relationship with type 'R2' is not allowed for user")

    // THEN
    execute("MATCH (a)-[r:R2]->(b) RETURN r").toSet should have size 0
  }

  test("merge should not create a relationship with wrong property") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { prop } ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('R1')")
    execute("CALL db.createProperty('prop')")
    execute("CALL db.createProperty('wrong')")
    execute("CREATE (:Foo)")
    execute("CREATE (:Bar)")

    // WHEN
    val exception = the [AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", s"MATCH (a:Foo), (b:Bar) MERGE (a)-[r:R1{wrong:'value'}]->(b)")
    }
    exception.getMessage should startWith("Set property for property 'wrong' is not allowed for user")

    // THEN
    execute("MATCH (a)-[r:R1]->(b) RETURN r.prop").toSet should be(empty)
  }

  test("merge should allow set property on existing relationship") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MERGE { * } ON GRAPH * NODES Foo, Bar TO custom")
    execute("GRANT MERGE { prop } ON GRAPH * RELATIONSHIPS R1 TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createProperty('prop')")
    execute("CREATE (:Foo)-[:R1]->(:Bar)")

    // WHEN
    executeOnDefault("joe", "soap", s"MATCH (a:Foo)-[r:R1]->(b:Bar) SET r.prop='value' ")

    // THEN
    execute("MATCH (a)-[r:R1]->(b) RETURN r.prop").toSet should be(Set(Map("r.prop" -> "value")))
  }

}
