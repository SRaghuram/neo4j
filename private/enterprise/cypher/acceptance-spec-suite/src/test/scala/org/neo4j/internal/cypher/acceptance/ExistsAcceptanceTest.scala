/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class ExistsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  protected override def initTest(): Unit = {
    super.initTest()
    executeSingle(
      """
        |CREATE (:Person {name:'Alice', id: 0}),
        |       (:Person {name:'Bosse', lastname: 'Bobson', id: 1})-[:HAS_DOG {since: 2016}]->(:Dog {name:'Bosse'}),
        |       (:Dog {name:'Fido'})<-[:HAS_DOG {since: 2010}]-(:Person {name:'Chris', id:2})-[:HAS_DOG {since: 2018}]->(:Dog {name:'Ozzy'})
      """.stripMargin
    )
  }

  // EXISTS without inner WHERE clause

  test("simple exists without where clause") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        | MATCH (person)-[:HAS_DOG]->(:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Bosse"), Map("person.name" -> "Chris")))
  }

  test("exists without where clause but with predicate on outer match") {

    val query =
      """
        |MATCH (person:Person {name:'Bosse'})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists subquery with not findable inner pattern") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_HOUSE]->(:House)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List())
  }

  test("exists after optional match without where clause") {

    val query =
      """
        |OPTIONAL MATCH (person:Person {name:'Bosse'})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists after empty optional match without where clause") {

    val query =
      """
        |OPTIONAL MATCH (person:Person {name:'Charlie'}) // Charlie does not exist
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> null)))

  }

  test("(unfulfilled) exists after optional match without where clause") {

    val query =
      """
        |OPTIONAL MATCH (person:Person {name:'Alice'})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog) // Alice doesn't have any dogs
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> null)))

  }

  test("exists after simple WITH without where clause") {

    val query =
      """
        |MATCH (person:Person {name:'Bosse'})
        |WITH person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists after selective WITH without where clause") {

    val query =
      """
        |MATCH (person:Person {name:'Bosse'}), (p:Person)
        |WITH person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse"), Map("person.name" -> "Bosse"), Map("person.name" -> "Bosse")))

  }

  test("exists after renaming WITH without where clause") {

    val query =
      """
        |MATCH (p:Person {name:'Bosse'})
        |WITH p AS person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists after additive WITH without where clause") {

    val query =
      """
        |MATCH (person:Person {name:'Bosse'})
        |WITH person, 1 AS ignore
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists after WITH DISTINCT without where clause") {

    val query =
      """
        |MATCH (person:Person {name:'Bosse'}), (p:Person)
        |WITH DISTINCT person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists in MATCH in horizon without where clause") {

    val query =
      """
        |MATCH (dog:Dog)
        |WITH 1 AS ignore
        |MATCH (person:Person {name:'Bosse'})
        |WITH person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse"), Map("person.name" -> "Bosse"), Map("person.name" -> "Bosse")))

  }

  test("double exists without where clause") {

    val query =
      """
        |MATCH (person:Person {name:'Bosse'})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |AND
        |EXISTS {
        |  MATCH (dog:Dog {name: 'Ozzy'})
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.nTimes(2, aPlan("SemiApply"))

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("double exists after WITH without where clause") {

    val query =
      """
        |MATCH (p:Person {name:'Bosse'})
        |WITH p AS person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |AND
        |EXISTS {
        |  MATCH (dog:Dog {name: 'Ozzy'})
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.nTimes(2, aPlan("SemiApply"))

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("double exists (no result) without where clause") {

    val query =
      """
        |MATCH (person:Person {name:'Bosse'})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |AND
        |EXISTS {
        |  MATCH (dog:Dog {name: 'Jacob'}) // Jacob does not exist
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.nTimes(2, aPlan("SemiApply"))

    result.toList should equal(List.empty)

  }

  test("double exists after WITH (no result) without where clause") {

    val query =
      """
        |MATCH (p:Person {name:'Bosse'})
        |WITH p AS person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |}
        |AND
        |EXISTS {
        |  MATCH (dog:Dog {name: 'Jacob'}) // Jacob does not exist
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.nTimes(2, aPlan("SemiApply"))

    result.toList should equal(List.empty)

  }

  // EXISTS with inner MATCH WHERE

  test("exists subquery with predicate") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog :Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists subquery with negative predicate") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog :Dog)
        |  WHERE NOT person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Chris")))
  }

  test("exists subquery with multiple predicates") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog)
        |  WHERE person.name = dog.name AND dog.name = "Bosse"
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists subquery with multiple predicates 2") {

    val query =
      """
        |MATCH (dog:Dog)
        |WHERE EXISTS {
        |  MATCH (person {name:'Chris'})-[:HAS_DOG]->(dog)
        |  WHERE dog.name < 'Karo'
        |}
        |RETURN dog.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("dog.name" -> "Fido")))

  }

  test("exists subquery with multiple predicates 3") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person {lastname:'Bobson'})-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists subquery with predicates on both outer and inner query") {

    val query =
      """
        |MATCH (person:Person {name:'Bosse'})
        |WHERE EXISTS {
        |  MATCH (person {lastname:'Bobson'})-[:HAS_DOG]->(dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("complexer predicates 1") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name AND person.lastname = 'Bobson' AND person.id < 2
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("complexer predicates 2") {

    val query =
      """
        |MATCH (person:Person {id:1})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name AND person.lastname = 'Bobson'
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("complexer predicates 3") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE NOT person.name = dog.name OR person.lastname = 'Bobson'
        |} AND person.id = 1
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("OR between EXISTS and other predicate") {

    val query =
      """
        |    MATCH (a:Person), (b:Dog { name:'Ozzy' })
        |    WHERE a.id = 0
        |    OR EXISTS {
        |      MATCH (a)-[:HAS_DOG]->(b)
        |    }
        |    RETURN a.name as name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SelectOrSemiApply")

    result.toSet should equal(Set(Map("name" -> "Alice"), Map("name" -> "Chris")))
  }

  test("OR between NOT EXISTS and other predicate") {

    val query =
      """
        |    MATCH (a:Person), (b:Dog { name:'Ozzy' })
        |    WHERE a.id = 0
        |    OR NOT EXISTS {
        |      MATCH (a)-[:HAS_DOG]->(b)
        |    }
        |    RETURN a.name as name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SelectOrAntiSemiApply")

    result.toSet should equal(Set(Map("name" -> "Alice"), Map("name" -> "Bosse")))
  }

  test("Unrelated inner pattern") {

    val query =
      """
        |MATCH (alice:Person {name:'Alice'})
        |WHERE EXISTS {
        |  (person:Person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN alice.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("alice.name" -> "Alice")))
  }

  test("exists after optional match with simple predicate") {

    val query =
      """
        |OPTIONAL MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists after empty optional match with simple predicate") {

    val query =
      """
        |OPTIONAL MATCH (person:Person {name:'Charlie'}) // Charlie does not exist
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> null)))

  }

  test("(unfulfilled) exists after optional match with simple predicate") {

    val query =
      """
        |OPTIONAL MATCH (person:Person {name:'Chris'})
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name // neither of Chris dogs are named Chris
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> null)))

  }

  test("exists after simple WITH with simple predicate") {

    val query =
      """
        |MATCH (person:Person)
        |WITH person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists after selective WITH with simple predicate") {

    val query =
      """
        |MATCH (person:Person), (p:Person)
        |WITH person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse"), Map("person.name" -> "Bosse"), Map("person.name" -> "Bosse")))

  }

  test("exists after rename WITH with simple predicate") {

    val query =
      """
        |MATCH (p:Person)
        |WITH p AS person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists after additive WITH with simple predicate") {

    val query =
      """
        |MATCH (person:Person)
        |WITH person, 1 AS ignore
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("exists after WITH DISTINCT with simple predicate") {

    val query =
      """
        |MATCH (person:Person),(p:Person)
        |WITH DISTINCT person
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  // Relationship predicates

  test("should handle relationship predicate") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |    MATCH (person)-[h:HAS_DOG]->(dog:Dog)
        |    WHERE h.since < 2016
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("should handle relationship predicate linking inner and outer relationship") {

    val query =
      """
        |MATCH (person:Person)-[r]->()
        |WHERE EXISTS {
        |    MATCH ()-[h:HAS_DOG]->(dog :Dog {name:'Bosse'})
        |    WHERE h.since = r.since
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Bosse")))

  }

  test("should handle more complex relationship predicate") {

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

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("adog.name" -> "Ozzy")))

  }

  // Omitting the MATCH keyword

  test("Omit match syntax in exist query") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse")))
  }

  // Other tests

  test("ensure we don't leak variables to the outside") {

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

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[*]->(dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse"), Map("person.name" -> "Chris")))

  }

  test("transitive closure inside exists should still work its magic") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE person.name = dog.name AND person.name = 'Bosse' and dog.lastname = person.name
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)


    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    plan should includeSomewhere.aPlan("Filter").containingArgument("cache[person.name] = $autostring_0")
    plan should includeSomewhere.aPlan("Filter").containingArgument("dog.name = $autostring_0 AND dog.lastname = $autostring_0 AND dog:Dog")
  }

  test("Should handle scoping and dependencies properly when subclause is in horizon") {

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

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("reuse of variable after exists should not affect result") {
    val query =
      """
        |MATCH (dog:Dog {name:'Bosse'})
        |
        |// Since Bosse's owner doesn't have other dogs, person should not be null
        |OPTIONAL MATCH (person:Person)-[:HAS_DOG]->(dog)
        |WHERE NOT EXISTS {
        |   MATCH (person)-[:HAS_DOG]->(d:Dog)
        |    WHERE NOT d = dog
        |}
        |
        |// since person isn't NULL the result should be 2
        |WITH CASE WHEN person IS NULL THEN 1 ELSE 2 END AS person
        |RETURN person
        |""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("AntiSemiApply")
    result.toList should be(List(Map("person" -> 2)))
  }

  // EXISTS with simple node pattern in the MATCH

  test("simple node match in exists 1") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)
        |  WHERE person.name = 'Chris'
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("simple node match in exists 2") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person {id:2})
        |  WHERE person.name = 'Chris'
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("simple node match in exists that will return false") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person {id:3})
        |  WHERE person.name = 'Chris'
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List())

  }

  // EXISTS with nested subclauses

  test("Nesting 1") {

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

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("Nesting 2") {

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

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  test("should handle several levels of nesting") {

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

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris"), Map("person.name" -> "Chris")))

  }

  // NOT EXISTS

  test("NOT EXISTS should work") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE NOT EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("AntiSemiApply")

    result.toList should equal(List(Map("person.name" -> "Alice")))

  }

  test("WHERE NOT EXISTS with horizons") {
    val q =
      """WITH 1 AS x
        |WHERE NOT EXISTS {
        |  MATCH (:Badger)
        |}
        |RETURN x""".stripMargin

    val res = executeSingle(q)
    res.toList shouldBe List(Map("x" -> 1))
  }

  test("WHERE NOT EXISTS with horizons 2") {
    val q =
      """WITH 1 AS x
        |WITH x, 2 as y
        |WHERE NOT EXISTS {
        |  MATCH (:Badger)
        |}
        |RETURN x""".stripMargin

    val res = executeSingle(q)
    res.toList shouldBe List(Map("x" -> 1))
  }

  test("WHERE NOT EXISTS with horizons 3") {
    val q =
      """WITH 1 AS x
        |WHERE NOT NOT EXISTS {
        |  MATCH ()
        |}
        |RETURN x""".stripMargin

    val res = executeSingle(q)
    res.toList shouldBe List(Map("x" -> 1))
  }

  test("WHERE NOT EXISTS WITH OR and horizon") {
    // test
    val q =
      """WITH 1 AS x
         WHERE x = 1 OR NOT EXISTS {
         MATCH (:Badger)
         }
         RETURN x""".stripMargin
    val res = executeSingle(q)
    res.toList shouldBe List(Map("x" -> 1))
  }

  test("WHERE NOT pattern predicate exists WITH OR and horizon") {
    // test
    val q =
      """MATCH (n)
         WITH 1 AS x, n AS n
         WHERE x = 1 OR NOT exists((n)-->(n))
         RETURN x
         LIMIT 1""".stripMargin

    val res = executeSingle(q)
    res.toList shouldBe List(Map("x" -> 1))
  }

  test("WHERE NOT property predicate exists WITH OR and horizon") {
    // test
    val q =
      """MATCH (n)
         WITH 1 AS x, n AS n
         WHERE x = 1 OR NOT exists(n.prop)
         RETURN x
         LIMIT 1""".stripMargin

    val res = executeSingle(q)
    res.toList shouldBe List(Map("x" -> 1))
  }

  test("WHERE EXISTS WITH OR and horizon") {
    // test
    val q =
      """WITH 1 AS x
         WHERE x = 1 OR EXISTS {
         MATCH (:Badger)
         }
         RETURN x""".stripMargin
    val res = executeSingle(q)
    res.toList shouldBe List(Map("x" -> 1))
  }

  test("WHERE EXISTS or WHERE NOT EXISTS WITH OR and horizon") {
    // test
    val q =
      """WITH 1 AS x
         WHERE x = 1 OR EXISTS {
         MATCH (:Badger)
         } OR NOT EXISTS {
          MATCH (:Snake)
         }
         RETURN x""".stripMargin
    val res = executeSingle(q)
    res.toList shouldBe List(Map("x" -> 1))
  }

  test("WHERE NOT EXISTS WITH AND and horizon") {
    val q =
      """WITH 1 AS x
         WHERE x = 1 AND NOT EXISTS {
         MATCH (:Badger)
         }
         RETURN x""".stripMargin
    val res = executeSingle(q)
    res.toList shouldBe List(Map("x" -> 1))
  }

  test("WHERE NOT EXISTS WITH OR, AND and horizon") {
    // test
    val q =
      """WITH 1 AS x
         WHERE x = 1 OR NOT EXISTS {
         MATCH (:Badger)
         } AND x < 64
         RETURN x""".stripMargin
    val res = executeSingle(q)
    res.toList shouldBe List(Map("x" -> 1))
  }

  test("WHERE NOT EXISTS WITH OR and multiple horizons") {
    // test
    val q =
      """WITH 1 AS x
         WITH 2 AS y, x
         WHERE x = 1 OR NOT EXISTS {
         MATCH (:Badger)
         } OR y < 64
         RETURN x""".stripMargin
    val res = executeSingle(q)
    res.toList shouldBe List(Map("x" -> 1))
  }

  test("NOT EXISTS with single node should work") {

    val query =
      """
        |MATCH (person:Person)
        |WHERE NOT EXISTS {
        |  MATCH (person)
        |  WHERE person.name = 'Alice'
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("AntiSemiApply")

    result.toList should equal(List(Map("person.name" -> "Bosse"), Map("person.name" -> "Chris")))

  }

  test("Nesting with NOT") {

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

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    plan should includeSomewhere.aPlan("AntiSemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris")))

  }

  // Multiple patterns in the inner MATCH

  test("multiple patterns in outer MATCH should be supported") {
    val query =
      """
        |MATCH (person:Person), (dog:Dog)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog)
        |  WHERE NOT EXISTS {
        |    MATCH (dog)
        |    WHERE dog.name = 'Bosse'
        |  }
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("SemiApply")
    plan should includeSomewhere.aPlan("AntiSemiApply")
    result.toList should equal(List(Map("person.name" -> "Chris"), Map("person.name" -> "Chris")))

  }

  test("multiple patterns in inner MATCH should work") {
    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        | MATCH (person), (car:Car)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should equal(List())
  }

  test("multiple patterns in inner MATCH with WHERE clause should be supported") {
    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        | MATCH (person), (person)-[:HAS_DOG]->(dog:Dog)
        | WHERE dog.name = "Bosse"
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should equal(List(Map("person.name" -> "Bosse")))
  }

  test("multiple patterns in inner MATCH without external variables should be supported") {
    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        | MATCH (anything), (allOther)
        |}
        |RETURN person.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should equal(List(Map("person.name" -> "Alice"), Map("person.name" -> "Bosse"), Map("person.name" -> "Chris")))
  }

  test("multiple patterns in inner MATCH should be supported") {
    val query =
      """
        |MATCH (dog:Dog)
        |WHERE EXISTS {
        | MATCH (person)-[:HAS_DOG]->(dog:Dog), (person)-[:HAS_DOG]->(dog2:Dog)
        | WHERE dog.name <> dog2.name
        |}
        |RETURN dog.name
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should equal(List(Map("dog.name" -> "Fido"), Map("dog.name" -> "Ozzy")))
  }

  // Unsupported EXISTS subqueries

  test("RETURN in inner MATCH should fail with syntax error at parsing") {
    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        | MATCH (person)-[:HAS_DOG]->(dog:Dog)
        | RETURN dog.name
        |}
        |RETURN person.name
      """.stripMargin

    failWithError(Configs.All, query, errorType = Seq("SyntaxException"))
  }

  test("inner query with MATCH -> WHERE -> WITH -> WHERE should fail with syntax error at parsing") {
    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        | MATCH (person)-[:HAS_DOG]->(dog:Dog)
        | WHERE person.name = 'Chris'
        | WITH dog
        | WHERE dog.name = 'Ozzy'
        |}
        |RETURN person.name
      """.stripMargin

    failWithError(Configs.All, query, errorType = Seq("SyntaxException"))
  }

  test("inner query with horizon should fail with syntax error at parsing") {
    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        | MATCH (person)-[:HAS_DOG]->(dog:Dog)
        | WITH dog
        | MATCH (dog {name: 'Ozzy'})
        |}
        |RETURN person.name
      """.stripMargin

    failWithError(Configs.All, query, errorType = Seq("SyntaxException"))
  }

  test("inner query with UNWIND should fail with syntax error at parsing") {
    val query =
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        | UNWIND $dogNames AS name
        |   MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |   WHERE dog.name = name
        |}
        |RETURN person.name
      """.stripMargin

    failWithError(Configs.All, query, params = Map("dogNames" -> Seq("Fido", "Bosse")), errorType = Seq("SyntaxException"))
  }

  test("aggregation with exists in horizon of tail should plan") {
    val query =
      """
        |MATCH (p:Person)-[:HAS_DOG]->(d:Dog)
        |WITH p, collect(d.name) as names
        |WITH p.name as walker
        |WHERE EXISTS { MATCH (n) }
        |RETURN walker
        |""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should equal(List(Map("walker" -> "Bosse"), Map("walker" -> "Chris")))
  }

  test("exists in WITH should return a syntax error at parsing") {

    val query =
      """
        |MATCH (person:Person)
        |WITH EXISTS {
        | MATCH (person)-[:HAS_DOG]->(:Dog)
        |} as foo
        |RETURN foo
      """.stripMargin

    failWithError(Configs.All, query, Seq("The EXISTS subclause is not valid inside a WITH or RETURN clause."), Seq("SyntaxException"))
  }

  test("exists in RETURN should return a syntax error at parsing") {

    val query =
      """
        |MATCH (person:Person)
        |RETURN EXISTS {
        | MATCH (person)-[:HAS_DOG]->(:Dog)
        |}
      """.stripMargin

    failWithError(Configs.All, query, Seq("The EXISTS subclause is not valid inside a WITH or RETURN clause."), Seq("SyntaxException"))
  }

  test("exists deep in RETURN should return a syntax error at parsing") {

    val query =
      """
        |MATCH (person:Person)
        |RETURN person.name, false OR person.boolean = EXISTS {
        | MATCH (person)-[:HAS_DOG]->(:Dog)
        |}
      """.stripMargin

    failWithError(Configs.All, query, Seq("EXISTS is only valid in a WHERE clause as a standalone predicate or as part of a boolean expression (AND / OR / NOT)"), Seq("SyntaxException"))
  }

  test("exists is not valid as part of an equality check") {
    val query =
      """
        |MATCH (person:Person)
        |WHERE person.age = EXISTS {
        | MATCH (person)-[:HAS_DOG]->(:Dog)
        |}
        |RETURN person
        |""".stripMargin

    failWithError(Configs.All, query, Seq("EXISTS is only valid in a WHERE clause as a standalone predicate or as part of a boolean expression (AND / OR / NOT)"), Seq("SyntaxException"))
  }

  test("not exists is not valid as part of an equality check") {
    val query =
      """
        |MATCH (person:Person)
        |WHERE person.age = (NOT EXISTS {
        | MATCH (person)-[:HAS_DOG]->(:Dog)
        |})
        |RETURN person
        |""".stripMargin

    failWithError(Configs.All, query, Seq("EXISTS is only valid in a WHERE clause as a standalone predicate or as part of a boolean expression (AND / OR / NOT)"), Seq("SyntaxException"))
  }

  test("Cannot use exists subclause as a function parameter") {
    val query =
      """
        |MATCH (person:Person)
        |WHERE TOSTRING(EXISTS {
        | MATCH (person)-[:HAS_DOG]->(:Dog)
        |}) = "true"
        |RETURN person
        |""".stripMargin

    failWithError(Configs.All, query, Seq("EXISTS is only valid in a WHERE clause as a standalone predicate or as part of a boolean expression (AND / OR / NOT)"), Seq("SyntaxException"))
  }

  test("Cannot set a property to the value of an exists subclause") {
    val query =
      """
        |MATCH (person:Person)
        |SET person.age = EXISTS {
        | MATCH (person)-[:HAS_DOG]->(:Dog)
        |}
        |RETURN person
        |""".stripMargin

    failWithError(Configs.All, query, Seq("The EXISTS subclause is not valid inside a SET clause."), Seq("SyntaxException"))
  }

  test("Cannot use exists as part of a create") {
    val query =
      """
        |CREATE (:Badger{isAlive: EXISTS {
        | MATCH (person)-[:HAS_DOG]->(:Dog)
        |}})
        |RETURN person
        |""".stripMargin

    failWithError(Configs.All, query, Seq("EXISTS is only valid in a WHERE clause as a standalone predicate or as part of a boolean expression (AND / OR / NOT)"), Seq("SyntaxException"))
  }


}
