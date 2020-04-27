/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions.SyntaxException
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

class BackwardsCompatibilityAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test( "should handle switch between Cypher versions" ) {
    // run query against latest version
    executeSingle("MATCH (n) RETURN n")

    // toInt should work if compatibility mode is set to 3.5
    executeSingle("CYPHER 3.5 RETURN toInt('1') AS one")

    // toInt should fail in latest version
    val exception = the [SyntaxException] thrownBy {
      executeSingle("RETURN toInt('1') AS one")
    }
    exception.getMessage should include("The function toInt() is no longer supported. Please use toInteger() instead")
  }

  // Removals in 4.0

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

  // Additions in 4.0

  test("administration commands should not work with CYPHER 3.5") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 SHOW DATABASES")
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

  test("existential subquery should not work with CYPHER 3.5") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 MATCH (n) WHERE EXISTS { (n)-->() } RETURN n")
    }
    // THEN
    exception.getMessage should include("Existential subquery is not supported in this Cypher version.")
  }

  // Additions in 4.1

  test("DROP DATABASE KEEP DATA is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE DATABASE foo")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 DROP DATABASE foo KEEP DATA")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 4.0 DROP DATABASE foo DUMP DATA")
    }
    exception_40.getMessage should include("Dumping data when dropping databases is not supported in this Cypher version.")

    // THEN
    executeSingle("DROP DATABASE foo DUMP DATA")
  }

  test("grant DEFAULT DATABASE is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 GRANT ACCESS ON DEFAULT DATABASE TO role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 4.0 GRANT ACCESS ON DEFAULT DATABASE TO role")
    }
    exception_40.getMessage should include("DEFAULT DATABASE is not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  test("deny DEFAULT DATABASE is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 DENY INDEX ON DEFAULT DATABASE TO role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 4.0 DENY INDEX ON DEFAULT DATABASE TO role")
    }
    exception_40.getMessage should include("DEFAULT DATABASE is not supported in this Cypher version.")

    // WHEN 4.1
    val exception_41 = the[InvalidArgumentsException] thrownBy {
      executeSingle("DENY INDEX ON DEFAULT DATABASE TO role")
    }
    exception_41.getMessage should include("Failed to deny index privilege to role 'role': Role does not exist.")
  }

  test("revoke DEFAULT DATABASE is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")
    executeSingle("GRANT START ON DEFAULT DATABASE TO role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 REVOKE START ON DEFAULT DATABASE FROM role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 4.0 REVOKE START ON DEFAULT DATABASE FROM role")
    }
    exception_40.getMessage should include("DEFAULT DATABASE is not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should not be List.empty
  }

  // Role management privilege commands should work in 4.0
  Seq(
    "ROLE MANAGEMENT",
    "CREATE ROLE",
    "DROP ROLE",
    "SHOW ROLE",
    "ASSIGN ROLE",
    "REMOVE ROLE"
  ).foreach {
    command =>
      test(s"GRANT $command is not supported in 3.5") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        executeSingle("CREATE ROLE role")

        // WHEN 3.5
        val exception_35 = the[SyntaxException] thrownBy {
          executeSingle(s"CYPHER 3.5 GRANT $command ON DBMS TO role")
        }
        exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

        // THEN
        executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)

        // WHEN 4.0
        executeSingle(s"CYPHER 4.0 GRANT $command ON DBMS TO role")

        // THEN
        executeSingle("SHOW ROLE role PRIVILEGES").toList should not be List.empty
      }

      test(s"DENY $command is not supported in 3.5") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        executeSingle("CREATE ROLE role")

        // WHEN 3.5
        val exception_35 = the[SyntaxException] thrownBy {
          executeSingle(s"CYPHER 3.5 DENY $command ON DBMS TO role")
        }
        exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

        // THEN
        executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)

        // WHEN 4.0
        executeSingle(s"CYPHER 4.0 DENY $command ON DBMS TO role")

        // THEN
        executeSingle("SHOW ROLE role PRIVILEGES").toList should not be List.empty
      }

      test(s"REVOKE $command is not supported in 3.5") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        executeSingle("CREATE ROLE role")
        executeSingle(s"GRANT $command ON DBMS TO role")

        // WHEN 3.5
        val exception_35 = the[SyntaxException] thrownBy {
          executeSingle(s"CYPHER 3.5 REVOKE $command ON DBMS FROM role")
        }
        exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

        // THEN
        executeSingle("SHOW ROLE role PRIVILEGES").toList should not be List.empty

        // WHEN 4.0
        executeSingle(s"CYPHER 4.0 REVOKE $command ON DBMS FROM role")

        // THEN
        executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
      }
  }

  // Other dbms privilege commands than role management should not work
  Seq(
    "USER MANAGEMENT",
    "CREATE USER",
    "DROP USER",
    "SHOW USER",
    "ALTER USER",
    "DATABASE MANAGEMENT",
    "CREATE DATABASE",
    "DROP DATABASE",
    "PRIVILEGE MANAGEMENT",
    "SHOW PRIVILEGE",
    "ASSIGN PRIVILEGE",
    "REMOVE PRIVILEGE",
    "ALL DBMS PRIVILEGES"
  ).foreach {
    command =>
      test(s"GRANT $command is not supported in 3.5 or 4.0") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        executeSingle("CREATE ROLE role")

        // WHEN 3.5
        val exception_35 = the[SyntaxException] thrownBy {
          executeSingle(s"CYPHER 3.5 GRANT $command ON DBMS TO role")
        }
        exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

        // WHEN 4.0
        val exception_40 = the[SyntaxException] thrownBy {
          executeSingle(s"CYPHER 4.0 GRANT $command ON DBMS TO role")
        }
        exception_40.getMessage should include(s"$command privilege is not supported in this Cypher version.")

        // THEN
        executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
      }

      test(s"DENY $command is not supported in 3.5 or 4.0") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        executeSingle("CREATE ROLE role")

        // WHEN 3.5
        val exception_35 = the[SyntaxException] thrownBy {
          executeSingle(s"CYPHER 3.5 DENY $command ON DBMS TO role")
        }
        exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

        // WHEN 4.0
        val exception_40 = the[SyntaxException] thrownBy {
          executeSingle(s"CYPHER 4.0 DENY $command ON DBMS TO role")
        }
        exception_40.getMessage should include(s"$command privilege is not supported in this Cypher version.")

        // THEN
        executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
      }

      test(s"REVOKE $command is not supported in 3.5 or 4.0") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        executeSingle("CREATE ROLE role")
        executeSingle(s"GRANT $command ON DBMS TO role")

        // WHEN 3.5
        val exception_35 = the[SyntaxException] thrownBy {
          executeSingle(s"CYPHER 3.5 REVOKE $command ON DBMS FROM role")
        }
        exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

        // WHEN 4.0
        val exception_40 = the[SyntaxException] thrownBy {
          executeSingle(s"CYPHER 4.0 REVOKE $command ON DBMS FROM role")
        }
        exception_40.getMessage should include(s"$command privilege is not supported in this Cypher version.")

        // THEN
        executeSingle("SHOW ROLE role PRIVILEGES").toList should not be List.empty
      }
  }

  test("grant transaction management is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 GRANT TRANSACTION MANAGEMENT ON DATABASE * TO role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 4.0 GRANT TRANSACTION MANAGEMENT ON DATABASE * TO role")
    }
    exception_40.getMessage should include("Transaction administration privileges are not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  test("deny transaction management is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 DENY SHOW TRANSACTION ON DATABASE * TO role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 4.0 DENY SHOW TRANSACTION ON DATABASE * TO role")
    }
    exception_40.getMessage should include("Transaction administration privileges are not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  test("revoke transaction management is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")
    executeSingle("GRANT TERMINATE TRANSACTION ON DATABASE * TO role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 3.5 REVOKE TERMINATE TRANSACTION ON DATABASE * FROM role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle("CYPHER 4.0 REVOKE TERMINATE TRANSACTION ON DATABASE * FROM role")
    }
    exception_40.getMessage should include("Transaction administration privileges are not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should not be List.empty
  }

  // Fined-grained write should not work in 4.0

  test("grant fine-grained write is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 GRANT SET LABEL label ON GRAPH * TO role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 4.0 GRANT SET LABEL label ON GRAPH * TO role")
    }
    exception_40.getMessage should include("Fine-grained writes are not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  test("deny fine-grained write is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 DENY REMOVE LABEL label ON GRAPH * TO role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 4.0 DENY REMOVE LABEL label ON GRAPH * TO role")
    }
    exception_40.getMessage should include("Fine-grained writes are not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  test("revoke fine-grained write is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 REVOKE CREATE ON GRAPH * FROM role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 4.0 REVOKE CREATE ON GRAPH * FROM role")
    }
    exception_40.getMessage should include("Fine-grained writes are not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  test("revoke grant fine-grained write is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 REVOKE GRANT DELETE ON GRAPH * NODES A FROM role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 4.0 REVOKE GRANT DELETE ON GRAPH * NODES A FROM role")
    }
    exception_40.getMessage should include("Fine-grained writes are not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  test("revoke deny fine-grained write is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 REVOKE DENY SET PROPERTY {prop} ON GRAPH * FROM role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 4.0 REVOKE DENY SET PROPERTY {prop} ON GRAPH * FROM role")
    }
    exception_40.getMessage should include("Fine-grained writes are not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  // ALL GRAPH PRIVILEGES should not be supported in 4.0

   test("grant all graph privileges is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 4.0 GRANT ALL GRAPH PRIVILEGES ON GRAPH * TO role")
    }
    exception_40.getMessage should include("ALL GRAPH PRIVILEGES is not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  test("deny all graph privileges is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 DENY ALL GRAPH PRIVILEGES ON GRAPH * TO role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 4.0 DENY ALL GRAPH PRIVILEGES ON GRAPH * TO role")
    }
    exception_40.getMessage should include("ALL GRAPH PRIVILEGES is not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  // ALL MERGE should not be supported in 4.0

  test("grant merge is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 GRANT MERGE {*} ON GRAPH * TO role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 4.0 GRANT MERGE {*} ON GRAPH * TO role")
    }
    exception_40.getMessage should include("Fine-grained writes are not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  test("deny merge is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 DENY MERGE {*} ON GRAPH * TO role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 4.0 DENY MERGE {*} ON GRAPH * TO role")
    }
    exception_40.getMessage should include("Fine-grained writes are not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  test("revoke merge is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 REVOKE MERGE {*} ON GRAPH * FROM role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 4.0 REVOKE MERGE {*} ON GRAPH * FROM role")
    }
    exception_40.getMessage should include("Fine-grained writes are not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  test("revoke all graph privileges is not supported in 3.5 or 4.0") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 REVOKE ALL GRAPH PRIVILEGES ON GRAPH * FROM role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")

    // WHEN 4.0
    val exception_40 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 4.0 REVOKE ALL GRAPH PRIVILEGES ON GRAPH * FROM role")
    }
    exception_40.getMessage should include("ALL GRAPH PRIVILEGES is not supported in this Cypher version.")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should be(List.empty)
  }

  // Write should work in 4.0
  test("GRANT WRITE is not supported in 3.5") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    executeSingle("CREATE ROLE role")

    // WHEN 3.5
    val exception_35 = the[SyntaxException] thrownBy {
      executeSingle(s"CYPHER 3.5 GRANT WRITE ON GRAPH * TO role")
    }
    exception_35.getMessage should include("Commands towards system database are not supported in this Cypher version.")


    // WHEN 4.0
    executeSingle(s"CYPHER 4.0 GRANT WRITE ON GRAPH * TO role")

    // THEN
    executeSingle("SHOW ROLE role PRIVILEGES").toList should not be (List.empty)
  }
}

