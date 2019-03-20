/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, SyntaxException}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class ExistsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  private def dogSetup(): Unit = {
    val query =
      """
        |CREATE (:Person {name:'Alice', id: 0}),
        |       (:Person {name:'Bob', lastname: 'Bobson', id: 1})-[:HAS_DOG]->(:Dog {name:'Bob'}),
        |       (:Person {name:'Chris', id:2})-[:HAS_DOG]->(:Dog {name:'Charlie'})
      """.stripMargin

    executeSingle(query)
  }

  private def simpleSetup(): Unit = {
    val node0 = createNode(Map("prop" -> 0))
    val node1 = createNode(Map("prop" -> 1))
    val node2 = createNode(Map("prop" -> 2))

    relate(node0, node1)
    relate(node1, node2)
  }

  test("Omit match syntax in exist query") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))
  }


  test("Unrelated inner pattern") {

    dogSetup()

    val query =
      """
        |PROFILE MATCH (alice:Person {name:'Alice'})
        |WHERE EXISTS {
        |  (person:Person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN alice.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    //TODO check that Expand(ALL) produces only 1 Row (see Rows)

    result.toList should equal(List(Map("alice.name" -> "Alice")))
  }

  test("exists subquery with predicate") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog :Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))

  }

  test("exists subquery with predicate 2") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog :Dog)
        |  WHERE NOT person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Chris")))
  }

  test("exists subquery with predicate 3") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_HOUSE]->(:House)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List())
  }

  test("check if we leak variables to the outside") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog :Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name, dog.name
      """.stripMargin

    try {
      executeSingle(query)
    }
    catch {
      case syn: SyntaxException if syn.getMessage.startsWith("Variable `dog` not defined") => // this is expected
      case _ => fail()  // we are leaking
    }
  }

  test("exists subquery with multiple predicates") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog)
        |  WHERE person.name = dog.name AND dog.name = "Bob"
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))

  }

  test("predicates on outer and subquery 1") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person{name:'Bob'})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))

  }

  test("predicates on outer and subquery 2") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person{name:'Bob'})-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))

  }

  test("predicates on outer and subquery 3") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person{name:'Bob'})
        |WHERE EXISTS {
        |  MATCH (person{lastname:'Bobson'})-[:HAS_DOG]->(dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))

  }

  test("predicates on outer and subquery 4") {
    // like 1 but predicate is unrelated to equality here

    dogSetup()

    val query =
      """
        |MATCH (person:Person{lastname:'Bobson'})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))

  }

  test("predicates on outer and subquery 5") {
    // like 2 but predicate is unrelated to equality here

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person{lastname:'Bobson'})-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))

  }

  test("complexer predicates 1") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name AND person.lastname ='Bobson' AND person.id = 1
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))

  }

  test("complexer predicates 2") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person{id:1})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name AND person.lastname ='Bobson'
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))

  }

  test("complexer predicates 3") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE NOT person.name = dog.name OR person.lastname ='Bobson'
        |} AND person.id = 1
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))

  }

  test("transitive closure inside exists should still work its magic") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name AND person.name = 'Bob' and dog.lastname = person.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    val description = result.executionPlanDescription().find("Filter")
    result.toList should equal(List())
    description(0).toString should include("person.name = $`  AUTOSTRING0`")
    description(1).toString should include("dog:Dog; dog.lastname = $`  AUTOSTRING0`; dog.name = $`  AUTOSTRING0`")
  }

  test("simple node match in exists 1") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)
        |  WHERE person.name = 'Chris'
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("simple node match in exists 2") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person{id:2})
        |  WHERE person.name = 'Chris'
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("simple node match in exists that will return false") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person{id:3})
        |  WHERE person.name = 'Chris'
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map()))

  }

  test("Nesting 1") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE EXISTS {
        |    MATCH (dog)
        |    WHERE dog.name = 'Charlie'
        |  }
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("Nesting 2") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE EXISTS {
        |    MATCH (dog)<-[]-()
        |    WHERE dog.name = 'Charlie'
        |  }
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("NOT EXISTS should work") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE NOT EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Alice")))

  }

  test("NOT EXISTS with single node should work") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE NOT EXISTS {
        |  MATCH (person)
        |  WHERE person.name = 'Alice'
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    result.toList should equal(List(Map("person.name" -> "Bosse"), Map("person.name" -> "Chris")))

  }

  test("Nesting with NOT") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE NOT EXISTS {
        |    MATCH (dog)
        |    WHERE dog.name = 'Ozzy'
        |  }
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    val plan = result.executionPlanDescription()
    result.toList should equal(List(Map("person.name" -> "Bosse")))
  }

  test("new syntax without where clause in exists") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE exists{
        | MATCH(person)-[:HAS_DOG]->(:Dog)
        | }
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())
    result.toList should equal(List(Map("person.name" -> "Bob"), Map("person.name" -> "Chris")))
  }

  test("new syntax without where clause in exists but with predicate on outer match") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person{name:'Bob'})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeSingle(query)

    println(result.executionPlanDescription())

    result.toList should equal(List(Map("person.name" -> "Bob")))

  }
}
