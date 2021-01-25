/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.procedure.GlobalProcedures

import scala.collection.JavaConverters.asScalaIteratorConverter

class ProceduresAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig()
    .updated(procedure_unrestricted,
      util.List.of(
        "org.neo4j.findById",
        "org.neo4j.findByIdInTx",
        "org.neo4j.findByIdInDatabase",
        "org.neo4j.findPropertyInDatabase",
        "org.neo4j.findRelationshipByIdInDatabase",
        "org.neo4j.findRelationshipById"
      ))

  test("should return result") {
    registerTestProcedures()

    val result = executeWith(Configs.ProcedureCallRead,
      "CALL org.neo4j.stream123() YIELD count, name RETURN count, name ORDER BY count")

    result.toList should equal(List(
      Map("count" -> 1, "name" -> "count1" ),
      Map("count" -> 2, "name" -> "count2" ),
      Map("count" -> 3, "name" -> "count3" )
    ))
  }

  test("should call cypher from procedure") {
    registerTestProcedures()

    executeSingle("UNWIND [1,2,3] AS i CREATE (a:Cat)")

    val result = executeWith(Configs.ProcedureCallRead,
      "CALL org.neo4j.aNodeWithLabel( 'Cat' ) YIELD node RETURN node")

    result.size should equal(1)
  }

  test("should recursively call cypher and procedure") {
    registerTestProcedures()

    executeSingle("UNWIND [1,2,3] AS i CREATE (a:Cat)")

    val result = executeWith(Configs.ProcedureCallRead,
      "CALL org.neo4j.recurseN( 3 ) YIELD node RETURN node")

    result.size should equal(1)
  }

  test("should call Core API") {
    registerTestProcedures()

    executeSingle("UNWIND [1,2,3] AS i CREATE (a:Cat)")
    executeSingle("UNWIND [1,2] AS i CREATE (a:Mouse)")

    val result = executeWith(Configs.ProcedureCallRead,
      "CALL org.neo4j.findNodesWithLabel( 'Cat' ) YIELD node RETURN node")

    result.size should equal(3)
  }

  test("should call expand using Core API") {
    registerTestProcedures()

    executeSingle("CREATE (c:Cat) WITH c UNWIND [1,2,3] AS i CREATE (c)-[:HUNTS]->(m:Mouse)")

    val result = executeWith(Configs.ProcedureCallRead,
      "MATCH (c:Cat) CALL org.neo4j.expandNode( id( c ) ) YIELD node AS n RETURN n")

    result.size should equal(3)
  }

  test("should create node with loop using Core API") {
    registerTestProcedures()

    executeWith(Configs.ProcedureCallWrite, "CALL org.neo4j.createNodeWithLoop( 'Node', 'Rel' ) YIELD node RETURN count(node)")

    val result = executeSingle("MATCH (n)-->(n) RETURN n")
    result.size should equal(1)
  }

  test("should find shortest path using Graph Algos Dijkstra") {
    registerTestProcedures()

    executeSingle(
      """
        |CREATE (s:Start)
        |CREATE (e:End)
        |CREATE (n1)
        |CREATE (n2)
        |CREATE (n3)
        |CREATE (n4)
        |CREATE (n5)
        |CREATE (s)-[:Rel {weight:5}]->(n1)
        |CREATE (s)-[:Rel {weight:7}]->(n2)
        |CREATE (s)-[:Rel {weight:1}]->(n3)
        |CREATE (n1)-[:Rel {weight:2}]->(n2)
        |CREATE (n1)-[:Rel {weight:6}]->(n4)
        |CREATE (n3)-[:Rel {weight:1}]->(n4)
        |CREATE (n4)-[:Rel {weight:1}]->(n5)
        |CREATE (n5)-[:Rel {weight:1}]->(e)
        |CREATE (n2)-[:Rel {weight:2}]->(e)
        |""".stripMargin)

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (s:Start),(e:End) CALL org.neo4j.graphAlgosDijkstra( s, e, 'Rel', 'weight' ) YIELD node RETURN node")

    result.size should equal(5) // s -> n3 -> n4 -> n5 -> e
  }

  test("should use traversal API") {
    registerTestProcedures()

    // Given
    executeSingle(TestGraph.movies)
    executeSingle("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western")

    // When
    val result = executeWith(Configs.ProcedureCallRead,
      """MATCH (k:Person {name:'Keanu Reeves'})
        |CALL org.neo4j.movieTraversal(k) YIELD path RETURN last(nodes(path)).name AS name""".stripMargin)

    // Then
    result.toList should equal(List(Map("name" -> "Clint Eastwood")))
  }

  test("should use correct temporal types") {
    registerTestProcedures()

    val result = executeSingle(
      "CALL org.neo4j.time(localtime.statement())")

    result.toList should be(empty) // and not crash
  }

  test("should call procedure with query parameters overriding default values") {
    registerTestProcedures()

    executeSingle("UNWIND [1,2,3] AS i CREATE (a:Cat)")

    val result = executeWith(Configs.ProcedureCallRead,
      "CALL org.neo4j.aNodeWithLabel", params = Map("label" -> "Cat"))

    result.size should equal(1)
  }

  test("should call procedure with internal types") {
    registerTestProcedures()

    executeWith(Configs.ProcedureCallRead,
      "CALL org.neo4j.internalTypes()").toList should equal(List(Map("textValue" -> "Dog", "mapValue" -> Map("key" -> 1337))))
    executeWith(Configs.ProcedureCallRead,
      "CALL org.neo4j.internalTypes('Cat')").toList should equal(List(Map("textValue" -> "Cat", "mapValue" -> Map("key" -> 1337))))
    executeWith(Configs.ProcedureCallRead,
      "CALL org.neo4j.internalTypes('Cat', {key: 42})").toList should equal(List(Map("textValue" -> "Cat", "mapValue" -> Map("key" -> 42))))
  }

  test( "should fail to modify entities from closed transaction") {
    registerTestProcedures()

    val (n, r) = createNodeAndRelationship()

    failWithError(Configs.ProcedureCallWrite, "CALL org.neo4j.setProperty($node, 'prop', 'glass')", params = Map("node" -> n), message = "NotInTransaction")
    failWithError(Configs.ProcedureCallWrite, "CALL org.neo4j.setProperty($rel, 'prop', 'glass')", params = Map("rel" -> r), message = "NotInTransaction")
  }

  test( "should allow to modify entities in other transaction") {
    registerTestProcedures()

    val tx1 = graph.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)
    try {
      val n = tx1.createNode()
      val r = n.createRelationshipTo(n, RelationshipType.withName("R"))

      executeWith(Configs.ProcedureCallWrite, "CALL org.neo4j.setProperty($node, 'prop', 'glass')", params = Map("node" -> n))
      executeWith(Configs.ProcedureCallWrite, "CALL org.neo4j.setProperty($rel, 'prop', 'glace')", params = Map("rel" -> r))

      n.getProperty("prop") shouldBe "glass"
      r.getProperty("prop") shouldBe "glace"
      tx1.rollback()
    } finally {
      tx1.close()
    }
  }

  test("should fail to modify entities created in nested tx") {
    registerTestProcedures()

    createNodeAndRelationship()

    failWithError(Configs.ProcedureCallWrite,
      """CALL org.neo4j.matchNodeAndRelationship() YIELD node AS n
        |CALL org.neo4j.setProperty(n, 'prop', 'glass') YIELD node
        |RETURN node.prop
        |""".stripMargin,
      "NotInTransactionException")

    failWithError(Configs.ProcedureCallWrite,
      """CALL org.neo4j.matchNodeAndRelationship() YIELD relationship AS r
        |CALL org.neo4j.setProperty(r, 'prop', 'glace') YIELD relationship
        |RETURN relationship.prop
        |""".stripMargin,
      "NotInTransactionException")
  }

  test("should fail to modify entities created in nested UDF tx") {
    registerTestProcedures()

    val (n, _) = createNodeAndRelationship()

    failWithError(Configs.ProcedureCallWrite,
      """WITH org.neo4j.findByIdInTx($nodeId) AS n
        |CALL org.neo4j.setProperty(n, 'prop', 'n1') YIELD node
        |RETURN node.prop
        |""".stripMargin,
      params = Map("nodeId" -> n.getId),
      message = "NotInTransactionException")
  }

  test("should be able to pass entities returned from another transaction, if transaction is open") {
    // given
    registerTestProcedures()
    val (n, _) = createNodeAndRelationship()
    val tx = graphOps.beginTx()

    // when
    // findById uses an open injected transaction
    val result = tx.execute("WITH org.neo4j.findById($nodeId) AS n RETURN n", util.Map.of("nodeId", n.getId.asInstanceOf[Object]))
      .asScala
      .toList

    //then
    result shouldEqual(List(util.Map.of("n", n)))

    tx.commit()
  }

  test("should not be able to pass entities returned from another transaction, if transaction is closed") {
    // given
    registerTestProcedures()
    val (n, _) = createNodeAndRelationship()
    val tx = graphOps.beginTx()

    // then
    // findByIdInTx closes the transaction that is used to find the node
    assertThrows[QueryExecutionException](tx.execute("WITH org.neo4j.findByIdInTx($nodeId) AS n RETURN n", util.Map.of("nodeId", n.getId.asInstanceOf[Object])).asScala.toList)

    tx.commit()
  }

  test("should be able to use properties of entities returned from a different database") {
    registerTestProcedures()
    managementService.createDatabase("test123")
    val dbOther = managementService.database("test123")
    val tx1 = dbOther.beginTx()
    val node = tx1.createNode()
    node.setProperty("prop", "123prop")
    tx1.commit()

    executeWith(Configs.ProcedureCallRead,
      """WITH org.neo4j.findPropertyInDatabase($nodeId, $dbOther, $property) AS nProp
        | RETURN nProp
        |""".stripMargin,
      params = Map("nodeId" -> node.getId, "dbOther" -> "test123", "property" -> "prop")).toList shouldEqual (List(Map("nProp" -> "123prop")))
  }

  test("should not be able to pass entities returned from a different database") {
    registerTestProcedures()
    managementService.createDatabase("test123")
    val dbOther = managementService.database("test123")
    val tx1 = dbOther.beginTx()
    val node = tx1.createNode().getId
    tx1.commit()
    val tx2 = dbOther.beginTx()

    failWithError(Configs.ProcedureCallRead,
      """WITH org.neo4j.findByIdInDatabase($nodeId, $dbOther, false) AS n
        | RETURN n
        |""".stripMargin,
      params = Map("nodeId" -> node, "dbOther" -> "test123"),
      errorType = Seq("CypherExecutionException"))

    tx2.close()
  }

  test("should be able to pass relationship entity returned from another transaction, if transaction is open") {
    // given
    registerTestProcedures()
    val (_, r) = createNodeAndRelationship()
    val tx = graphOps.beginTx()

    // when
    // findRelationshipById uses an open injected transaction
    val result = tx.execute("WITH org.neo4j.findRelationshipById($relId) AS r RETURN r", util.Map.of("relId", r.getId.asInstanceOf[Object]))
      .asScala
      .toList

    //then
    result shouldEqual(List(util.Map.of("r", r)))

    tx.commit()
  }

  test("should not be able to pass relationship entity returned from a different database") {
    registerTestProcedures()
    managementService.createDatabase("test123")
    val dbOther = managementService.database("test123")
    val tx1 = dbOther.beginTx()
    val relationship = tx1.execute("CREATE (a)-[r: Rel]->(b) return r").next().get("r").asInstanceOf[Relationship]
    tx1.commit()
    val tx2 = dbOther.beginTx()

    failWithError(Configs.ProcedureCallRead,
      """WITH org.neo4j.findRelationshipByIdInDatabase($relId, $dbOther, false) AS n
        | RETURN n
        |""".stripMargin,
      params = Map("relId" -> relationship.getId, "dbOther" -> "test123"),
      errorType = Seq("CypherExecutionException"))

    tx2.close()
  }

  private def registerTestProcedures(): Unit = {
    val procedures = graph.getDependencyResolver.resolveDependency(classOf[GlobalProcedures])
    procedures.registerProcedure(classOf[TestProcedure])
    procedures.registerFunction(classOf[TestProcedure])
  }

  // create a node and relationship in their own transaction
  private def createNodeAndRelationship() = {
    withTx { tx =>
      val n = tx.createNode()
      val r = n.createRelationshipTo(n, RelationshipType.withName("R"))
      (n, r)
    }
  }
}
