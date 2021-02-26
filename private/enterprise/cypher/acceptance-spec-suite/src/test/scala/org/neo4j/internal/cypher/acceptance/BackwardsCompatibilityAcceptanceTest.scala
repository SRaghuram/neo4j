/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions.SyntaxException
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class BackwardsCompatibilityAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle switch between Cypher versions") {
    // run query against latest version
    executeSingle("MATCH (n) RETURN n")

    // toInt should work if compatibility mode is set to 3.5
    executeSingle("CYPHER 3.5 RETURN toInt('1') AS one")

    // toInt should fail in latest version
    val exception = the[SyntaxException] thrownBy {
      executeSingle("RETURN toInt('1') AS one")
    }
    exception.getMessage should include("The function toInt() is no longer supported. Please use toInteger() instead")
  }

  // Removals earlier in 4.x

  test("query without removed syntax should work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN reverse('emil') as backwards")
    result.toList should be(List(Map("backwards" -> "lime")))
  }

  test("toInt should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN toInt('1') AS one")
    result.toList should be(List(Map("one" -> 1)))
  }

  test("upper should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN upper('foo') AS upper")
    result.toList should be(List(Map("upper" -> "FOO")))
  }

  test("lower should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN lower('BAR') AS lower")
    result.toList should be(List(Map("lower" -> "bar")))
  }

  test("rels should still work with CYPHER 3.5") {

    // GIVEN
    val a = createNode(Map("name" -> "Alice"))
    val b = createNode(Map("name" -> "Bob"))
    val c = createNode(Map("name" -> "Charlie"))

    relate(a, b, "prop" -> "ab")
    relate(b, c, "prop" -> "bc")
    relate(a, c, "prop" -> "ac")

    // WHEN
    val query =
      """
        |CYPHER 3.5
        |MATCH p = ({name:'Alice'})-->()
        |UNWIND [r IN rels(p) | r.prop] AS prop
        |RETURN prop ORDER BY prop
      """.stripMargin

    val result = executeSingle(query)

    // THEN
    result.toList should be(List(Map("prop" -> "ab"), Map("prop" -> "ac")))
  }

  test("filter should still work with CYPHER 3.5 regardless of casing") {
    for (filter <- List("filter", "FILTER", "filTeR")) {
      val result = executeSingle(s"CYPHER 3.5 WITH [1,2,3] AS list RETURN $filter(x IN list WHERE x % 2 = 1) AS odds")
      result.toList should be(List(Map("odds" -> List(1, 3))))
    }
  }

  test("extract should still work with CYPHER 3.5 regardless of casing") {
    for (extract <- List("extract", "EXTRACT", "exTraCt")) {
      val result = executeSingle(s"CYPHER 3.5 WITH [1,2,3] AS list RETURN $extract(x IN list | x * 10) AS tens")
      result.toList should be(List(Map("tens" -> List(10, 20, 30))))
    }
  }

  test("old parameter syntax should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN {param} AS answer", params = Map("param" -> 42))
    result.toList should be(List(Map("answer" -> 42)))
  }

  test("length of string should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN length('a string') as len")
    result.toList should be(List(Map("len" -> 8)))
  }

  test("length of collection should still work with CYPHER 3.5") {
    val result = executeSingle("CYPHER 3.5 RETURN length([1, 2, 3]) as len")
    result.toList should be(List(Map("len" -> 3)))
  }

  test("length of pattern expression should still work with CYPHER 3.5") {

    // GIVEN
    val a = createNode(Map("name" -> "Alice"))
    val b = createNode(Map("name" -> "Bob"))
    val c = createNode(Map("name" -> "Charlie"))
    val d = createNode(Map("name" -> "David"))

    // a -> b -> c -> d
    relate(a, b)
    relate(b, c)
    relate(c, d)

    // a -> c -> b
    relate(a, c)
    relate(c, b)

    // WHEN
    val result = executeSingle("CYPHER 3.5 MATCH (a) WHERE a.name='Alice' RETURN length((a)-->()-->()) as len")

    // THEN
    result.toList should be(List(Map("len" -> 3))) // a -> b -> c, a -> c -> b, a -> c -> d
  }

  test("MATCH with legacy type separator with CYPHER 3.5") {
    // GIVEN
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b, "B", Map("foo" -> "bar"))
    relate(a, b, "C", Map("foo" -> "bar"))
    relate(a, c, "D")

    val queryVariable = "MATCH (n)-[x:A|:B|:C]->() RETURN n" // variable binding
    val queryProperty = "MATCH (n)-[:A|:B|:C {foo: 'bar'}]->() RETURN n" // inlined property predicate
    val queryLength = "MATCH (n)-[:A|:B|:C*]->() RETURN n" // variable length

    val queryShortestPath = "MATCH p = shortestPath((n)-[:A|:B|:C|:D*]->(m)) WHERE n <> m RETURN p"
    val queryWhere1 = "MATCH (n) WHERE (n)-[:A|:B|:C {foo: 'bar'}]->() RETURN n"
    val queryWhere2 = "MATCH (n) WHERE exists((n)-[:A|:B|:C*]->()) RETURN n"
    val queryWith1 = "MATCH (n) WITH size((n)-[:A|:B|:C {foo: 'bar'}]->()) AS size RETURN size"
    val queryWith2 = "MATCH (n) WITH exists((n)-[:A|:B|:C {foo: 'bar'}]->()) AS exists RETURN exists"

    Seq(
      (queryVariable, 2),
      (queryProperty, 2),
      (queryLength, 2),
      (queryShortestPath, 2),
      (queryWhere1, 1),
      (queryWhere2, 1),
      (queryWith1, 3),
      (queryWith2, 3)
    ).foreach { case (query, numResults) =>
      withClue(query) {
        // WHEN
        val res = executeSingle(s"CYPHER 3.5 $query")

        // THEN
        res.toList.size should be(numResults)
      }
    }
  }

  // Additions earlier in 4.x

  test("The USE clause should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 USE neo4j RETURN 1 + 1")
    }
    exception.getMessage should include("The USE clause is not supported in this Cypher version.")
  }

  test("community administration commands should not work with CYPHER 3.5") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 SHOW DATABASES")
    }
    exception.getMessage should include("Commands towards system database are not supported in this Cypher version.")
  }

  test("enterprise administration commands should not work with CYPHER 3.5") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 DROP ROLE reader")
    }
    exception.getMessage should include("Commands towards system database are not supported in this Cypher version.")
  }

  test("procedures towards system should not work with CYPHER 3.5") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CALL dbms.security.createUser('Alice', '1234', true)")
    }
    exception.getMessage should include("Commands towards system database are not supported in this Cypher version.")
  }

  test("new create index syntax should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CREATE INDEX my_index FOR (n:Label) ON (n.prop)")
    }
    exception.getMessage should include("Creating index using this syntax is not supported in this Cypher version.")

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")).isEmpty should be(true)
  }

  test("new drop index syntax should not work with CYPHER 3.5") {
    // GIVEN
    graph.createIndexWithName("my_index", "Label", "prop")

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 DROP INDEX my_index")
    }
    exception.getMessage should include("Dropping index by name is not supported in this Cypher version.")

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")).isDefined should be(true)
  }

  test("create named node key constraint should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    }
    exception.getMessage should include("Creating named node key constraint is not supported in this Cypher version.")

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)
  }

  test("create named uniqueness constraint should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    }
    exception.getMessage should include("Creating named uniqueness constraint is not supported in this Cypher version.")

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)
  }

  test("create named node existence constraint should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT EXISTS(n.prop)")
    }
    exception.getMessage should include("Creating named node existence constraint is not supported in this Cypher version.")

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)
  }

  test("create named relationship existence constraint should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CREATE CONSTRAINT my_constraint ON ()-[r:Label]-() ASSERT EXISTS(r.prop)")
    }
    exception.getMessage should include("Creating named relationship existence constraint is not supported in this Cypher version.")

    // THEN
    graph.getMaybeRelationshipConstraint("Label", "prop").isEmpty should be(true)
  }

  test("new drop constraint syntax should not work with CYPHER 3.5") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("my_constraint", "Label", "prop")

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 DROP CONSTRAINT my_constraint")
    }
    exception.getMessage should include("Dropping constraint by name is not supported in this Cypher version.")

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isDefined should be(true)
  }

  test("create node key constraint with IF NOT EXISTS should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    }
    exception.getMessage should include("Creating node key constraint using `IF NOT EXISTS` is not supported in this Cypher version.")

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)
  }

  test("create uniqueness constraint with IF NOT EXISTS should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    }
    exception.getMessage should include("Creating uniqueness constraint using `IF NOT EXISTS` is not supported in this Cypher version.")

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)
  }

  test("create node existence constraint with IF NOT EXISTS should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT EXISTS(n.prop)")
    }
    exception.getMessage should include("Creating node existence constraint using `IF NOT EXISTS` is not supported in this Cypher version.")

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)
  }

  test("create relationship existence constraint with IF NOT EXISTS should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:Label]-() ASSERT EXISTS(r.prop)")
    }
    exception.getMessage should include("Creating relationship existence constraint using `IF NOT EXISTS` is not supported in this Cypher version.")

    // THEN
    graph.getMaybeRelationshipConstraint("Label", "prop").isEmpty should be(true)
  }

  test("existential subquery should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 MATCH (n) WHERE EXISTS { (n)-->() } RETURN n")
    }
    // THEN
    exception.getMessage should include("Existential subquery is not supported in this Cypher version.")
  }

  test("create node key constraint with options should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY OPTIONS {irrelevantValue: 'CantBeEmptyMap'}")
    }

    // THEN
    exception.getMessage should include("Creating node key constraint with options is not supported in this Cypher version.")
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)
  }

  test("create uniqueness constraint with options should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE OPTIONS {irrelevantValue: 'CantBeEmptyMap'}")
    }

    // THEN
    exception.getMessage should include("Creating uniqueness constraint with options is not supported in this Cypher version.")
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)
  }

  test("SHOW INDEXES should not work with Cypher 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 SHOW INDEXES")
    }

    // THEN
    exception.getMessage should include("SHOW INDEXES is not supported in this Cypher version.")
  }

  test("SHOW CONSTRAINTS should not work with Cypher 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 SHOW CONSTRAINTS")
    }

    // THEN
    exception.getMessage should include("SHOW CONSTRAINTS is not supported in this Cypher version.")
  }

  // Additions 4.3

  test("Node existence constraint using IS NOT NULL should not work with CYPHER 3.5 and 4.2") {
    Seq("CYPHER 3.5", "CYPHER 4.2").foreach(version => {
      withClue(version) {
        // WHEN
        val exception = the[SyntaxException] thrownBy {
          executeSingle(s"$version CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")
        }

        // THEN
        exception.getMessage should include("Creating node existence constraint using `IS NOT NULL` is not supported in this Cypher version.")
      }
    })

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)
  }

  test("Relationship existence constraint using IS NOT NULL should not work with CYPHER 3.5 and 4.2") {
    Seq("CYPHER 3.5", "CYPHER 4.2").foreach(version => {
      withClue(version) {
        // WHEN
        val exception = the[SyntaxException] thrownBy {
          executeSingle(s"$version CREATE CONSTRAINT ON ()-[r:Label]-() ASSERT r.prop IS NOT NULL")
        }

        // THEN
        exception.getMessage should include("Creating relationship existence constraint using `IS NOT NULL` is not supported in this Cypher version.")
      }
    })

    // THEN
    graph.getMaybeRelationshipConstraint("Label", "prop").isEmpty should be(true)
  }

  test("Procedure call with YIELD * should not work with CYPHER 3.5 and 4.2") {
    Seq("CYPHER 3.5", "CYPHER 4.2").foreach(version => {
      withClue(version) {
        // WHEN
        val exception = the[SyntaxException] thrownBy {
          executeSingle(s"$version CALL db.labels() YIELD *")
        }

        // THEN
        exception.getMessage should include("Procedure call using `YIELD *` is not supported in this Cypher version.")
      }
    })
  }

  test("IF EXISTS for ALTER USER should not work with CYPHER 3.5 and 4.2") {
    Seq("CYPHER 3.5", "CYPHER 4.2").foreach(version => {
      withClue(version) {
        // WHEN
        val exception = the[SyntaxException] thrownBy {
          executeSingle(s"$version ALTER USER foo IF EXISTS SET STATUS ACTIVE")
        }

        // THEN
        exception.getMessage should include("Updating a user with `IF EXISTS` is not supported in this Cypher version.")
      }
    })
  }

  test("SHOW INDEXES should not support WHERE, YIELD and RETURN with Cypher 4.2") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 4.2 SHOW INDEXES YIELD name WHERE name='foo' RETURN name")
    }

    // THEN
    exception.getMessage should include("Using YIELD or WHERE to list indexes is not supported in this Cypher version.")
  }

  test("SHOW INDEXES should not support WHERE with Cypher 4.2") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 4.2 SHOW INDEX WHERE name='foo'")
    }

    // THEN
    exception.getMessage should include("Using YIELD or WHERE to list indexes is not supported in this Cypher version.")
  }
}
