/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class ExistsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  private def dogSetup(): Unit = {
    val query =
    """
        |CREATE (:Person {name:'Alice', id: 0}),
        |       (:Person {name:'Bosse', lastname: 'Bobson', id: 1})-[:HAS_DOG {since: 2016}]->(:Dog {name:'Bosse'}),
        |       (:Dog {name:'Fido'})<-[:HAS_DOG {since: 2010}]-(:Person {name:'Chris', id:2})-[:HAS_DOG {since: 2018}]->(:Dog {name:'Ozzy'})
      """.stripMargin

    executeSingle(query)
  }

  // EXISTS without inner WHERE clause

  test("simple exists without where clause") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE exists{
        | MATCH(person)-[:HAS_DOG]->(:Dog)
        | }
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Bosse"), Map("person.name" -> "Chris")))
  }

  test("exists without where clause but with predicate on outer match") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person{name:'Bosse'})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists subquery with not findable inner pattern") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_HOUSE]->(:House)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List())
  }

  // EXISTS with inner MATCH WHERE

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

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists subquery with negative predicate") {

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

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Chris")))
  }

  test("exists subquery with multiple predicates") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog)
        |  WHERE person.name = dog.name AND dog.name = "Bosse"
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists subquery with multiple predicates 2") {

    dogSetup()

    val query =
      """
        |MATCH (dog:Dog)
        |WHERE EXISTS {
        |  MATCH (person{name:'Chris'})-[:HAS_DOG]->(dog)
        |  WHERE dog.name < 'Karo'
        |}
        |RETURN dog.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("dog.name" -> "Fido")))

  }

  test("exists subquery with multiple predicates 3") {

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

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists subquery with predicates on both outer and inner query") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person{name:'Bosse'})
        |WHERE EXISTS {
        |  MATCH (person{lastname:'Bobson'})-[:HAS_DOG]->(dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("complexer predicates 1") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name AND person.lastname ='Bobson' AND person.id < 2
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

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

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

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

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("Unrelated inner pattern") {

    dogSetup()

    val query =
      """
        |MATCH (alice:Person {name:'Alice'})
        |WHERE EXISTS {
        |  (person:Person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN alice.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("alice.name" -> "Alice")))
  }

  // Relationship predicates

  test("should handle relationship predicate") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |    MATCH (person)-[h:HAS_DOG]->(dog:Dog)
        |    WHERE h.since < 2016
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("should handle relationship predicate linking inner and outer relationship") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)-[r]->()
        |WHERE EXISTS {
        |    MATCH ()-[h:HAS_DOG]->(dog :Dog {name:'Bosse'})
        |    WHERE h.since = r.since
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("should handle more complex relationship predicate") {

    dogSetup()

    val query =
      """
        |MATCH (adog:Dog {name:'Ozzy'})
        |WITH adog
        |MATCH ()-[r]->()
        |WHERE EXISTS {
        |    MATCH (person)-[h:HAS_DOG]->(adog)
        |    WHERE id(h) = id(r)
        |}
        |RETURN adog.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("adog.name" -> "Ozzy")))

  }

  // Omitting the MATCH keyword

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

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))
  }

  // Other tests

  test("ensure we don't leak variables to the outside") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name, dog.name
      """.stripMargin

    failWithError(Configs.All, query, errorType = Seq("SyntaxException"), message = Seq("Variable `dog` not defined"))
  }

  test("should support variable length pattern") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[*]->(dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse"), Map("person.name" -> "Chris")))

  }

  test("transitive closure inside exists should still work its magic") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name AND person.name = 'Bosse' and dog.lastname = person.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)


    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    plan should includeSomewhere.aPlan("Filter").containingArgument("person.name = $`  AUTOSTRING0`")
    plan should includeSomewhere.aPlan("Filter").containingArgument("dog:Dog", "dog.lastname = $`  AUTOSTRING0`", "dog.name = $`  AUTOSTRING0`")
  }

  test("Should handle scoping and dependencies properly when subclause is in horizon") {

    dogSetup()

    val query =
      """
        |MATCH (adog:Dog {name:'Ozzy'})
        |WITH adog
        |MATCH (person:Person)
        |WHERE EXISTS {
        |    MATCH (person)-[:HAS_DOG]->(adog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  // EXISTS with simple node pattern in the MATCH

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

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
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

    val result = executeWith(Configs.InterpretedAndSlotted, query)

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

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List())

  }

  // EXISTS with nested subclauses

  test("Nesting 1") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE EXISTS {
        |    MATCH (dog)
        |    WHERE dog.name = 'Ozzy'
        |  }
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
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
        |    WHERE dog.name = 'Ozzy'
        |  }
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("should handle several levels of nesting") {

    dogSetup()

    val query =
      """
        |MATCH (person:Person)-[]->()
        |WHERE EXISTS {
        |  MATCH (person)
        |  WHERE person.id > 0 AND EXISTS {
        |    MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |    WHERE EXISTS {
        |     MATCH (dog)
        |     WHERE dog.name = 'Ozzy'
        |    }
        |  }
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris"), Map("person.name" -> "Chris")))

  }

  // NOT EXISTS

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

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("AntiSemiApply")

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

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("AntiSemiApply")

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
        |    WHERE dog.name = 'Bosse'
        |  }
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    plan should includeSomewhere.aPlan("AntiSemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }
}
