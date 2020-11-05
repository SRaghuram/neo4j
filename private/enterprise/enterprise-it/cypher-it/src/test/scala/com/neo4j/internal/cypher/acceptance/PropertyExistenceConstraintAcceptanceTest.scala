/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import com.neo4j.cypher.EnterpriseGraphDatabaseTestSupport
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.internal.compiler.helpers.ListSupport
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.graphdb.ConstraintViolationException
import org.neo4j.kernel.api.exceptions.Status

class PropertyExistenceConstraintAcceptanceTest
  extends ExecutionEngineFunSuite
  with QueryStatisticsTestSupport
  with ListSupport
  with EnterpriseGraphDatabaseTestSupport {

  test("node: should enforce constraints on creation") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is not null")

    // WHEN
    val e = intercept[ConstraintViolationException](execute("create (node:Label1)"))

    // THEN
    e.getMessage should endWith(" with label `Label1` must have the property `key1`")
  }

  test("relationship: should enforce constraints on creation") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert rel.since is not null")

    // WHEN
    val e = intercept[ConstraintViolationException](execute("create (p1:Person)-[:KNOWS]->(p2:Person)"))

    // THEN
    e.getMessage should endWith("with type `KNOWS` must have the property `since`")
  }

  test("node: should enforce on removing property") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is not null")

    // WHEN
    execute("create (node1:Label1 {key1:'value1'})")

    // THEN
    intercept[ConstraintViolationException](execute("match (node:Label1) remove node.key1"))
  }

  test("relationship: should enforce on removing property") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert rel.since is not null")

    // WHEN
    execute("create (p1:Person)-[:KNOWS {since: 'yesterday'}]->(p2:Person)")

    // THEN
    intercept[ConstraintViolationException](execute("match (p1:Person)-[r:KNOWS]->(p2:Person) remove r.since"))
  }

  test("node: should enforce on setting property to null") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is not null")

    // WHEN
    execute("create ( node1:Label1 {key1:'value1' } )")

    //THEN
    intercept[ConstraintViolationException](execute("match (node:Label1) set node.key1 = null"))
  }

  test("relationship: should enforce on setting property to null") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert rel.since is not null")

    // WHEN
    execute("create (p1:Person)-[:KNOWS {since: 'yesterday'}]->(p2:Person)")

    //THEN
    intercept[ConstraintViolationException](execute("match (p1:Person)-[r:KNOWS]->(p2:Person) set r.since = null"))
  }

  test("node: should allow to break constraint within statement") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is not null")

    // WHEN
    val res = execute("create (node:Label1) set node.key1 = 'foo' return node")

    //THEN
    res.toList should have size 1
  }

  test("relationship: should allow to break constraint within statement") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert rel.since is not null")

    // WHEN
    val res = execute("create (p1:Person)-[r:KNOWS]->(p2:Person) set r.since = 'yesterday' return r")

    //THEN
    res.toList should have size 1
  }

  test("node: should allow creation of non-conflicting data") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is not null")

    // WHEN
    execute("create (node {key1:'value1'} )")
    execute("create (node:Label2)")
    execute("create (node:Label1 { key1:'value1'})")
    execute("create (node:Label1 { key1:'value1'})")

    // THEN
    numberOfNodes shouldBe 4
  }

  test("relationship: should allow creation of non-conflicting data") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert rel.since is not null")

    // WHEN
    execute("create (p1:Person {name: 'foo'})-[r:KNOWS {since: 'today'}]->(p2:Person {name: 'bar'})")
    execute("create (p1:Person)-[:FOLLOWS]->(p2:Person)")
    execute("create (p:Person {name: 'Bob'})<-[:KNOWS {since: '2010'}]-(a:Animal {name: 'gädda'})")

    // THEN
    numberOfRelationships shouldBe 3
  }

  test("node: should fail to create constraint when existing data violates it") {
    // GIVEN
    execute("create (node:Label1)")

    // WHEN
    val e = intercept[CypherExecutionException](execute("create constraint on (node:Label1) assert node.key1 is not null"))

    //THEN
    e.status should equal(Status.Schema.ConstraintCreationFailed)
  }

  test("relationship: should fail to create constraint when existing data violates it") {
    // GIVEN
    execute("create (p1:Person)-[:KNOWS]->(p2:Person)")

    // WHEN
    val e = intercept[CypherExecutionException](execute("create constraint on ()-[rel:KNOWS]-() assert rel.since is not null"))

    //THEN
    e.status should equal(Status.Schema.ConstraintCreationFailed)
  }

  test("node: should drop constraint") {
    // GIVEN
    execute("create constraint my_constraint on (node:Label1) assert node.key1 is not null")

    intercept[ConstraintViolationException](execute("create (node:Label1)"))
    numberOfNodes shouldBe 0

    // WHEN
    execute("drop constraint my_constraint")
    execute("create (node:Label1)")

    // THEN
    numberOfNodes shouldBe 1
  }

  test("relationship: should drop constraint") {
    // GIVEN
    execute("create constraint my_constraint on ()-[rel:KNOWS]-() assert rel.since is not null")

    intercept[ConstraintViolationException](execute("create (p1:Person)-[:KNOWS]->(p2:Person)"))
    numberOfRelationships shouldBe 0

    // WHEN
    execute("drop constraint my_constraint")
    execute("create (p1:Person)-[:KNOWS]->(p2:Person)")

    // THEN
    numberOfRelationships shouldBe 1
  }

  test("should not use countStore short cut when no constraint exist") {
    val result = execute("MATCH (n:X) RETURN count(n.foo)")

    result.executionPlanDescription() shouldNot includeSomewhere.aPlan("NodeCountFromCountStore")
  }

  test("should use countStore short cut when constraint exist") {
    execute("CREATE CONSTRAINT ON (n:X) ASSERT n.foo IS NOT NULL")

    val result = execute("MATCH (n:X) RETURN count(n.foo)")

    result.executionPlanDescription() should includeSomewhere.aPlan("NodeCountFromCountStore")
  }

  private def numberOfNodes = executeScalar[Long]("match (n) return count(n)")

  private def numberOfRelationships = executeScalar[Long]("match ()-[r]->() return count(r)")
}
