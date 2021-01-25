/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.helpers.NodeKeyConstraintCreator
import org.neo4j.cypher.internal.helpers.UniquenessConstraintCreator
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class UniqueIndexAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  Seq(UniquenessConstraintCreator, NodeKeyConstraintCreator).foreach { constraintCreator =>

    test(s"$constraintCreator: should be able to use unique index hints on IN expressions") {
      //GIVEN
      val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
      val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
      relate(andres, createNode())
      relate(jake, createNode())

      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob'] RETURN n")
      //THEN
      result.toList should equal(List(Map("n" -> jake)))
    }

    test(s"$constraintCreator: should be able to use unique index on IN collections with duplicates") {
      //GIVEN
      val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
      val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
      relate(andres, createNode())
      relate(jake, createNode())

      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob','Jacob'] RETURN n")

      //THEN
      result.toList should equal(List(Map("n" -> jake)))
    }

    test(s"$constraintCreator: should be able to use unique index on IN a null value") {
      //GIVEN
      val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
      val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
      relate(andres, createNode())
      relate(jake, createNode())

      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN null RETURN n")

      //THEN
      result.toList should equal(List())
    }

    test(s"$constraintCreator: should be able to use index unique index on IN a collection parameter") {
      //GIVEN
      val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
      val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
      relate(andres, createNode())
      relate(jake, createNode())

      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN $coll RETURN n", params = Map("coll" -> List("Jacob")))

      //THEN
      result.toList should equal(List(Map("n" -> jake)))
    }

    test(s"$constraintCreator: should not use locking index for read only query") {
      //GIVEN
      val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
      val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
      relate(andres, createNode())
      relate(jake, createNode())

      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN $coll RETURN n",
        planComparisonStrategy = ComparePlansWithAssertion(plan => {
          //THEN
          plan should includeSomewhere.aPlan("NodeUniqueIndexSeek")
          plan shouldNot includeSomewhere.aPlan("NodeUniqueIndexSeek(Locking)")
        }),
        params = Map("coll" -> List("Jacob")))
    }

    test(s"$constraintCreator: should use locking unique index for merge node queries") {
      //GIVEN
      createLabeledNode(Map("name" -> "Andres"), "Person")
      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      executeWith(Configs.InterpretedAndSlotted, "MERGE (n:Person {name: 'Andres'}) RETURN n.name",
        planComparisonStrategy = ComparePlansWithAssertion(plan => {
          //THEN
          plan shouldNot includeSomewhere.aPlan("NodeIndexSeek")
          plan should includeSomewhere.aPlan("NodeUniqueIndexSeek(Locking)")
        }))
    }

    test(s"$constraintCreator: should use locking unique index for merge relationship queries") {
      //GIVEN
      createLabeledNode(Map("name" -> "Andres"), "Person")
      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      executeWith(Configs.InterpretedAndSlotted,
        "PROFILE MATCH (n:Person {name: 'Andres'}) MERGE (n)-[:KNOWS]->(m:Person {name: 'Maria'}) RETURN n.name",
        planComparisonStrategy = ComparePlansWithAssertion(plan => {
          // THEN
          plan shouldNot includeSomewhere.aPlan("NodeIndexSeek")
          plan shouldNot includeSomewhere.aPlan("NodeByLabelScan")
          plan should includeSomewhere.aPlan("NodeUniqueIndexSeek(Locking)")
        }))
    }

    test(s"$constraintCreator: should use locking unique index for mixed read write queries") {
      //GIVEN
      createLabeledNode(Map("name" -> "Andres"), "Person")
      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      val query = "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN $coll SET n:Foo RETURN n.name"
      //WHEN
      executeWith(Configs.InterpretedAndSlotted, query, params = Map("coll" -> List("Jacob")),
        planComparisonStrategy = ComparePlansWithAssertion(plan => {
          //THEN
          plan shouldNot includeSomewhere.aPlan("NodeIndexSeek")
          plan should includeSomewhere.aPlan("NodeUniqueIndexSeek(Locking)")
        }))
    }
  }

  test("should handle null with locking unique index seeks") {
    //GIVEN
    createLabeledNode("Person")
    UniquenessConstraintCreator.createConstraint(graph, "Person", "name")
    graph should not(haveConstraints(s"${UniquenessConstraintCreator.other.typeName}:Person(name)"))
    graph should haveConstraints(s"${UniquenessConstraintCreator.typeName}:Person(name)")

    val query = "MATCH (n:Person) WHERE n.name = null SET n:FOO"
    //WHEN
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy = ComparePlansWithAssertion(plan => {
      //THEN
      plan shouldNot includeSomewhere.aPlan("NodeIndexSeek")
      plan shouldNot includeSomewhere.aPlan("NodeByLabelScan")
      plan should includeSomewhere.aPlan("NodeUniqueIndexSeek(Locking)")
    }))
  }
}
