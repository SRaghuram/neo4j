/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.api.procedure.GlobalProcedures

class ProceduresAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

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

  private def registerTestProcedures(): Unit = {
    graph.getDependencyResolver.resolveDependency(classOf[GlobalProcedures]).registerProcedure(classOf[TestProcedure])
  }
}
