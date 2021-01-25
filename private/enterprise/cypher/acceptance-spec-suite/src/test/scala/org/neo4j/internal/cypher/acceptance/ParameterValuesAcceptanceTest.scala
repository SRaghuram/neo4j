/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.graphdb.Label
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.impl.coreapi.InternalTransaction

import scala.Array.emptyBooleanArray
import scala.Array.emptyByteArray
import scala.Array.emptyDoubleArray
import scala.Array.emptyFloatArray
import scala.Array.emptyIntArray
import scala.Array.emptyLongArray
import scala.Array.emptyShortArray

class ParameterValuesAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport
                                    with QueryStatisticsTestSupport {

  test("should be able to send in an array of nodes via parameter") {
    // given
    val node = createLabeledNode("Person")
    val result = executeWith(Configs.All, "WITH $param as p RETURN p", params = Map("param" -> Array(node)))
    val outputP = result.head("p")
    outputP should equal(Array(node))
  }

  // Not TCK material below; sending graph types or characters as parameters is not supported

  test("ANY should be able to use variables from the horizon") {

    val query =
      """ WITH 1 AS node, [] AS nodes1
        | RETURN ANY(n IN collect(distinct node) WHERE n IN nodes1) as exists """.stripMargin

    val r = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    r.toList should equal(List(Map("exists" -> false)))
  }

  test("should not erase the type of an empty array sent as parameter") {

    Seq(emptyLongArray, emptyShortArray, emptyByteArray, emptyIntArray,
      emptyDoubleArray, emptyFloatArray,
      emptyBooleanArray, Array[String]()).foreach { array =>

      val q = "CREATE (n) SET n.prop = $param RETURN n.prop AS p"
      val r = executeWith(Configs.InterpretedAndSlottedAndPipelined, q, params = Map("param" -> array))

      assertStats(r, nodesCreated = 1, propertiesWritten = 1)
      val returned = r.columnAs[Array[_]]("p").next()
      returned should equal(array)
      returned.getClass.getComponentType should equal(array.getClass.getComponentType)
    }
  }

  test("should not erase the type of nonempty arrays sent as parameter") {
    Seq(Array[Long](1l), Array[Short](2), Array[Byte](3), Array[Int](4),
      Array[Double](3.14), Array[Float](5.56f),
      Array[Boolean](false, true), Array[String]("", " ")).foreach { array =>

      val q = "CREATE (n) SET n.prop = $param RETURN n.prop AS p"
      val r = executeWith(Configs.InterpretedAndSlottedAndPipelined, q, params = Map("param" -> array))

      assertStats(r, nodesCreated = 1, propertiesWritten = 1)
      val returned = r.columnAs[Array[_]]("p").next()
      returned should equal(array)
      returned.getClass.getComponentType should equal(array.getClass.getComponentType)
    }
  }

  test("should be able to send in node via parameter") {
    // given
    val node = createLabeledNode("Person")

    val result = executeWith(Configs.All, "MATCH (b) WHERE b = $param RETURN b", params = Map("param" -> node))
    result.toList should equal(List(Map("b" -> node)))
  }

  test("should be able to pass in parameter from same transaction") {
    // given
    val tx = graph.getGraphDatabaseService.beginTx()
    val node = tx.createNode()

    // when
    val result = tx.execute("MATCH (b) WHERE b = $param RETURN b", java.util.Map.of("param", node)).next()

    // then
    result should equal(java.util.Map.of("b", node))

    tx.close()
  }

  test("should be able to pass in parameter from another open transaction state if same database") {
    // given
    val tx1 = graph.getGraphDatabaseService.beginTx()
    val tx2 = graph.getGraphDatabaseService.beginTx()
    tx1.createNode()
    val node2 = tx2.createNode()

    // when node2 does not exist yet, since tx2 has not been commited
    val result = tx1.execute("MATCH (b) WHERE b = $param RETURN b", java.util.Map.of("param", node2))

    // then
    result.hasNext shouldBe false

    tx1.close()
    tx2.close()
  }

  test("should be able to pass in parameter from another tx if same database, but cannot access node created in tx") {
    // given
    val tx1 = graph.getGraphDatabaseService.beginTx()

    //create and commit node
    val tx2 = graph.getGraphDatabaseService.beginTx()
    tx2.createNode()
    tx2.commit()

    // new transaction
    val tx3 = graph.getGraphDatabaseService.beginTx()
    val node3 = tx3.getAllNodes().iterator().next()

    // when
    val result = tx1.execute("MATCH (b) WHERE b = $param RETURN b", java.util.Map.of("param", node3))

    // then
    result.hasNext shouldBe true

    tx1.close()
  }

  test("should not be able to pass in parameter from another transaction state with same database if transaction is closed") {
    // given
    val tx1 = graph.getGraphDatabaseService.beginTx()
    val tx2 = graph.getGraphDatabaseService.beginTx()
    tx1.createNode()
    val node2 = tx2.createNode()
    tx2.commit()

    // then
    failWithErrorOnTx(Configs.All, tx1, "MATCH (b) WHERE b = $param RETURN labels(b)",
      s"The transaction of entity ${node2.getId} has been closed.", params = Map("param" -> node2))

    tx1.close()
  }

  test("should not be able to pass in parameter from another transaction state with different database") {
    // given
    val tx1 = graph.getGraphDatabaseService.beginTx()
    val dbOtherName = "other"
    managementService.createDatabase(dbOtherName)
    val dbOther = managementService.database(dbOtherName)
    val tx2 = dbOther.beginTx()
    tx1.createNode(Label.label("FOO"))
    val node2 = tx2.createNode(Label.label("BOO"))

    // then
    failWithErrorOnTx(Configs.All, tx1, "MATCH (b) WHERE b = $param RETURN labels(b)",
      s"Can not use an entity from another database. Entity id: ${node2.getId}, entity database: $dbOtherName, " +
        s"expected database: ${tx1.asInstanceOf[InternalTransaction].getDatabaseName}.", params = Map("param" -> node2))

    tx1.close()
    tx2.close()
  }

  test("should be able to send in relationship via parameter") {
    // given
    val rel = relate(createLabeledNode("Person"), createLabeledNode("Person"))

    val result = executeWith(Configs.All, "MATCH (:Person)-[r]->(:Person) WHERE r = $param RETURN r", params = Map("param" -> rel))
    result.toList should equal(List(Map("r" -> rel)))
  }

  test("should treat chars as strings in equality") {
    executeScalar[Boolean]("RETURN 'a' = $param", "param" -> 'a') shouldBe true
    executeScalar[Boolean]("RETURN $param = 'a'", "param" -> 'a') shouldBe true
  }

  test("removing property when not sure if it is a node or relationship should still work - NODE") {
    val n = createNode("name" -> "Anders")

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "WITH $p as p SET p.lastname = p.name REMOVE p.name", params = Map("p" -> n))

    graph.withTx( tx => {
      val node = tx.getNodeById(n.getId)
      node.getProperty("lastname") should equal("Anders")
      node.hasProperty("name") should equal(false)
    } )
  }

  test("removing property when not sure if it is a node or relationship should still work - REL") {
    val r = relate(createNode(), createNode(), "name" -> "Anders")

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "WITH $p as p SET p.lastname = p.name REMOVE p.name", params = Map("p" -> r))

    graph.withTx( tx => {
      val relationship = tx.getRelationshipById(r.getId)
      relationship.getProperty("lastname") should equal("Anders")
      relationship.hasProperty("name") should equal(false)
    } )
  }

  test("match with missing parameter should return error for empty db") {
    failWithError(Configs.All, "MATCH (n:Person {name:$name}) RETURN n", "Expected parameter(s): name")
  }

  test("match with missing parameter should return error for non-empty db") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "CREATE (n:Person) WITH n MATCH (n:Person {name:$name}) RETURN n", "Expected parameter(s): name")
  }

  test("match with multiple missing parameters should return error for empty db") {
    failWithError(Configs.All, "MATCH (n:Person {name:$name, age:$age}) RETURN n", "Expected parameter(s): name, age")
  }

  test("match with multiple missing parameters should return error for non-empty db") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "CREATE (n:Person) WITH n MATCH (n:Person {name:$name, age:$age}) RETURN n", "Expected parameter(s): name, age")
  }

  test("match with misspelled parameter should return error for empty db") {
    failWithError(Configs.All, "MATCH (n:Person {name:$name}) RETURN n", "Expected parameter(s): name", params = Map("nam" -> "Neo"))
  }

  test("match with misspelled parameter should return error for non-empty db") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "CREATE (n:Person) WITH n MATCH (n:Person {name:$name}) RETURN n", "Expected parameter(s): name", params = Map("nam" -> "Neo"))
  }

  test("name of missing parameters should only be returned once") {
    failWithError(Configs.All, "RETURN $p + $p + $p", "Expected parameter(s): p")
  }

  test("explain with missing parameter should NOT return error for empty db") {
    executeWith(Configs.All, "EXPLAIN MATCH (n:Person {name:$name}) RETURN n")
  }

  test("explain with missing parameter should NOT return error for non-empty db") {
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "EXPLAIN CREATE (n:Person) WITH n MATCH (n:Person {name:$name}) RETURN n")
  }

  test("merge and update using nested parameters list") {

    graph.createUniqueConstraint("Person", "name")
    createLabeledNode(Map("name" -> "Agneta"), "Person")

    val config = Configs.InterpretedAndSlotted
    val result = executeWith(config, """FOREACH (nameItem IN $nameItems |
                                       |   MERGE (p:Person {name:nameItem[0]})
                                       |   SET p.item = nameItem[1] )""".stripMargin,
      params = Map("nameItems" -> List(List("Agneta", "saw"), List("Arne", "hammer"))))

    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 3)
  }

  test("floating point parameters should be coerced to int for procedures") {
    // In Javascript every number is a floating point value unless specifically cast otherwise. Thus we need to coerce.
    // Yes, even floats that are not ints like 0.5
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "CALL db.awaitIndexes($param)", params = Map("param" -> 300.0f))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "CALL db.awaitIndexes($param)", params = Map("param" -> 300.5f))
  }

  test("floating point parameters should be coerced to int for functions") {
    // In Javascript every number is a floating point value unless specifically cast otherwise. Thus we need to coerce.
    // Yes, even floats that are not ints like 0.5
    executeWith(Configs.All, "UNWIND range(1, $param) AS n RETURN n", params = Map("param" -> 3.0f))
    executeWith(Configs.All, "UNWIND range(1, $param) AS n RETURN n", params = Map("param" -> 3.5f))
    executeWith(Configs.All, "UNWIND range($param1, $param2) AS n RETURN n", params = Map("param1" -> 1.0f, "param2" -> 3.0f))
    executeWith(Configs.All, "UNWIND range($param1, $param2) AS n RETURN n", params = Map("param1" -> 3.5f, "param2" -> 3.5f))
    executeWith(Configs.All, "UNWIND range($param1, $param2) AS n RETURN n", params = Map("param1" -> 3.0f, "param2" -> 5))
    executeWith(Configs.All, "UNWIND range($param1, $param2) AS n RETURN n", params = Map("param1" -> 3.5f, "param2" -> 5))
  }
}
