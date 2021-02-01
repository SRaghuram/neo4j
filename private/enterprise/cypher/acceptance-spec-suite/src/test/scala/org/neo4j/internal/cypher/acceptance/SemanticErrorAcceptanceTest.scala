/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.InterpretedRuntimeName
import org.neo4j.cypher.internal.PipelinedRuntimeName
import org.neo4j.cypher.internal.SlottedRuntimeName
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString
import org.neo4j.graphdb.QueryExecutionException

import java.util
import scala.collection.JavaConverters.asScalaIteratorConverter

class SemanticErrorAcceptanceTest extends ExecutionEngineFunSuite {

  test("return node that's not there") {
    executeAndEnsureError(
      "match (n) where id(n) = 0 return bar",
      "Variable `bar` not defined (line 1, column 34 (offset: 33))"
    )
  }

  test("don't allow a string after IN") {
    executeAndEnsureError(
      "MATCH (n) where id(n) IN '' return 1",
      "Type mismatch: expected List<T> but was String (line 1, column 26 (offset: 25))"
    )
  }

  test("don't allow a integer after IN") {
    executeAndEnsureError(
      "MATCH (n) WHERE id(n) IN 1 RETURN 1",
      "Type mismatch: expected List<T> but was Integer (line 1, column 26 (offset: 25))"
    )
  }

  test("don't allow a float after IN") {
    executeAndEnsureError(
      "MATCH (n) WHERE id(n) IN 1.0 RETURN 1",
      "Type mismatch: expected List<T> but was Float (line 1, column 26 (offset: 25))"
    )
  }

  test("don't allow a boolean after IN") {
    executeAndEnsureError(
      "MATCH (n) WHERE id(n) IN true RETURN 1",
      "Type mismatch: expected List<T> but was Boolean (line 1, column 26 (offset: 25))"
    )
  }

  test("define node and treat it as a relationship") {
    executeAndEnsureError(
      "match (r) where id(r) = 0 match (a)-[r]-(b) return r",
      "Type mismatch: r defined with conflicting type Node (expected Relationship) (line 1, column 38 (offset: 37))"
    )
  }

  test("redefine symbol in match") {
    executeAndEnsureError(
      "match (a)-[r]-(r) return r", "Type mismatch: r defined with conflicting type Relationship (expected Node) (line 1, column 16 (offset: 15))"
    )
  }

  test("cant use type() on nodes") {
    executeAndEnsureError(
      "MATCH (r) RETURN type(r)",
      "Type mismatch: expected Relationship but was Node (line 1, column 23 (offset: 22))"
    )
  }

  test("cant use labels() on relationships") {
    executeAndEnsureError(
      "MATCH ()-[r]-() RETURN labels(r)",
      "Type mismatch: expected Node but was Relationship (line 1, column 31 (offset: 30))"
    )
  }

  test("cant use toInteger() on booleans") {
    executeAndEnsureError(
      "RETURN toInteger(true)",
      "Type mismatch: expected Float, Integer, Number or String but was Boolean (line 1, column 18 (offset: 17))"
    )
  }

  test("cant use toFloat() on booleans") {
    executeAndEnsureError(
      "RETURN toFloat(false)",
      "Type mismatch: expected Float, Integer, Number or String but was Boolean (line 1, column 16 (offset: 15))"
    )
  }

  test("cant use toString() on nodes") {
    executeAndEnsureError(
      "MATCH (n) RETURN toString(n)",
      "Type mismatch: expected Boolean, Float, Integer, Point, String, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was Node (line 1, column 27 (offset: 26))"
    )
  }

  test("cant use LENGTH on nodes") {
    executeAndEnsureError(
      "match (n) where id(n) = 0 return length(n)",
      "Type mismatch: expected Path but was Node (line 1, column 41 (offset: 40))"
    )
  }

  test("cant use LENGTH on collections") {
    executeAndEnsureError(
      "return length([1, 2, 3])",
      "Type mismatch: expected Path but was List<Integer> (line 1, column 15 (offset: 14))"
    )
  }

  test("cant use LENGTH on strings") {
    executeAndEnsureError(
      "return length('a string')",
      "Type mismatch: expected Path but was String (line 1, column 15 (offset: 14))"
    )
  }

  test("cant use LENGTH on pattern expression") {
    executeAndEnsureError(
      " match (a) where a.name='Alice' return length((a)-->()-->())",
      "Type mismatch: expected Path but was List<Path> (line 1, column 47 (offset: 46))"
    )
  }

  test ("cant use SIZE on paths") {
    executeAndEnsureError(
      " MATCH p =(a)-->(b)-->(c) WHERE a.name = 'Alice' RETURN size(p)",
      "Type mismatch: expected String or List<T> but was Path (line 1, column 62 (offset: 61))"
    )
  }

  // Not TCK material; shortestPath, regex, constraints, hints, etc

  test("should know not to compare strings and numbers") {
    executeAndEnsureError(
      "match (a) where a.age =~ 13 return a",
      "Type mismatch: expected String but was Integer (line 1, column 26 (offset: 25))"
    )
  }

  test("should complain if shortest path has no relationship") {
    executeAndEnsureError(
      "match p=shortestPath((n)) return p",
      "shortestPath(...) requires a pattern containing a single relationship (line 1, column 9 (offset: 8))"
    )

    executeAndEnsureError(
      "match p=allShortestPaths((n)) return p",
      "allShortestPaths(...) requires a pattern containing a single relationship (line 1, column 9 (offset: 8))"
    )
  }

  test("should complain if shortest path has multiple relationships") {
    executeAndEnsureError(
      "match p=shortestPath((a)--()--(b)) where id(a) = 0 and id(b) = 1 return p",
      "shortestPath(...) requires a pattern containing a single relationship (line 1, column 9 (offset: 8))"
    )
    executeAndEnsureError(
      "match p=allShortestPaths((a)--()--(b)) where id(a) = 0 and id(b) = 1 return p",
      "allShortestPaths(...) requires a pattern containing a single relationship (line 1, column 9 (offset: 8))"
    )
  }

  test("should complain if shortest path has a minimal length different from 0 or 1") {
    executeAndEnsureError(
      "match p=shortestPath((a)-[*2..3]->(b)) where id(a) = 0 and id(b) = 1 return p",
      "shortestPath(...) does not support a minimal length different from 0 or 1 (line 1, column 9 (offset: 8))"
    )
    executeAndEnsureError(
      "match p=allShortestPaths((a)-[*2..3]->(b)) where id(a) = 0 and id(b) = 1 return p",
      "allShortestPaths(...) does not support a minimal length different from 0 or 1 (line 1, column 9 (offset: 8))"
    )
  }

  test("should be semantically incorrect to refer to unknown variable in create constraint") {
    executeAndEnsureError(
      "create constraint on (foo:Foo) assert bar.name is unique",
      "Variable `bar` not defined (line 1, column 39 (offset: 38))"
    )
  }

  test("should be semantically incorrect to refer to unknown variable in drop constraint") {
    executeAndEnsureError(
      "drop constraint on (foo:Foo) assert bar.name is unique",
      "Variable `bar` not defined (line 1, column 37 (offset: 36))"
    )
  }

  test("should fail when trying to create shortest paths") {
    executeAndEnsureError(
      "match (a), (b) create shortestPath((a)-[:T]->(b))",
      "shortestPath(...) cannot be used to CREATE (line 1, column 23 (offset: 22))"
    )
    executeAndEnsureError(
      "match (a), (b) create allShortestPaths((a)-[:T]->(b))",
      "allShortestPaths(...) cannot be used to CREATE (line 1, column 23 (offset: 22))"
    )
  }

  test("should fail when trying to merge shortest paths") {
    executeAndEnsureError(
      "match (a), (b) MERGE shortestPath((a)-[:T]->(b))",
      "shortestPath(...) cannot be used to MERGE (line 1, column 22 (offset: 21))"
    )
    executeAndEnsureError(
      "match (a), (b) MERGE allShortestPaths((a)-[:T]->(b))",
      "allShortestPaths(...) cannot be used to MERGE (line 1, column 22 (offset: 21))"
    )
  }

  test("should fail when trying to uniquely create shortest paths") {
    executeAndEnsureError(
      "match (a), (b) MERGE shortestPath((a)-[:T]->(b))",
      "shortestPath(...) cannot be used to MERGE (line 1, column 22 (offset: 21))"
    )
    executeAndEnsureError(
      "match (a), (b) create allShortestPaths((a)-[:T]->(b))",
      "allShortestPaths(...) cannot be used to CREATE (line 1, column 23 (offset: 22))"
    )
  }

  test("should fail when reduce used with wrong separator") {
    executeAndEnsureError("""MATCH topRoute = (s)<-[:CONNECTED_TO*1..3]-(e)
                            |WHERE id(s) = 1 AND id(e) = 2
                            |RETURN reduce(weight=0, r in relationships(topRoute) : weight+r.cost) AS score
                            |ORDER BY score ASC LIMIT 1
                          """.stripMargin,
      List(
        "reduce(...) requires '| expression' (an accumulation expression) (line 3, column 8 (offset: 84))",
        """Encountered " ")" ")"" at line 3, column 69."""
      )
    )
  }

  test("should fail if old iterable separator") {
    executeAndEnsureError(
      "match (a) where id(a) = 0 return reduce(i = 0, x in a.List : i + x.prop)",
      List(
        "reduce(...) requires '| expression' (an accumulation expression) (line 1, column 34 (offset: 33))",
        """Encountered " ")" ")"" at line 1, column 72."""
      )
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return any(x in a.list : x.prop = 1)",
      "any(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return all(x in a.list : x.prop = 1)",
      "all(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return single(x in a.list : x.prop = 1)",
      "single(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return none(x in a.list : x.prop = 1)",
      "none(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )
  }

  test("should fail if using a hint with an unknown variable") {
    executeAndEnsureError(
      "match (n:Person)-->() using index m:Person(name) where n.name = \"kabam\" return n",
      "Variable `m` not defined (line 1, column 35 (offset: 34))"
    )
  }

  test("should fail if using a hint on a node with no label") {
    executeAndEnsureError(
      "MATCH (n) USING INDEX n:Person(name) where n.name = \"Johan\" return n",
      "Cannot use index hint in this context. Must use label on node that hint is referring to. " +
        "(line 1, column 11 (offset: 10))"
    )
  }

  test("should fail if using a hint on a node and not using the property") {
    executeAndEnsureError(
      "MATCH (n:Person) USING INDEX n:Person(name) where n.lastname = \"Teleman\" return n",
      "Cannot use index hint in this context. Index hints are only supported for the following "+
        "predicates in WHERE (either directly or as part of a top-level AND or OR): equality comparison, " +
        "inequality (range) comparison, STARTS WITH, IN condition or checking property " +
        "existence. The comparison cannot be performed between two property values. Note that the " +
        "label and property comparison must be specified on a non-optional node (line 1, " +
        "column 18 (offset: 17))"
    )
  }

  test("should fail if no parens around node") {
    val expectedErrors =
      List(
        "Parentheses are required to identify nodes in patterns, i.e. (n) (line 1, column 7 (offset: 6))",
        """Invalid input 'n': expected "shortestPath", "allShortestPaths" or "(" (line 1, column 7 (offset: 6))"""
      )

    executeAndEnsureError(
      "match n:Person return n",
      expectedErrors
    )
    executeAndEnsureError(
      "match n {foo: 'bar'} return n",
      expectedErrors
    )
  }

  test("should fail if using non update clause inside foreach") {
    executeAndEnsureError(
      "FOREACH (n in [1] | WITH foo RETURN bar)",
      "Invalid use of WITH inside FOREACH (line 1, column 21 (offset: 20))"
    )
  }

  test("should return custom type error for reduce") {
    executeAndEnsureError(
      "RETURN reduce(x = 0, y IN [1,2,3] | x + y^2)",
      "Type mismatch: accumulator is Integer but expression has type Float (line 1, column 39 (offset: 38))"
    )
  }

  test("should reject properties on shortest path relationships") {
    executeAndEnsureError(
      "MATCH (a), (b), shortestPath( (a)-[r* {x: 1}]->(b) ) RETURN *",
      List(
        "shortestPath(...) contains properties MapExpression(List((PropertyKeyName(x),SignedDecimalIntegerLiteral(1)))). This is currently not supported. (line 1, column 17 (offset: 16))",
        "shortestPath(...) contains properties MapExpression(WrappedArray((PropertyKeyName(x),SignedDecimalIntegerLiteral(1)))). This is currently not supported. (line 1, column 17 (offset: 16))"
      )
    )
  }

  test("should reject properties on all shortest paths relationships") {
    executeAndEnsureError(
      "MATCH (a), (b), allShortestPaths( (a)-[r* {x: 1}]->(b) ) RETURN *",
      List(
        "allShortestPaths(...) contains properties MapExpression(List((PropertyKeyName(x),SignedDecimalIntegerLiteral(1)))). This is currently not supported. (line 1, column 17 (offset: 16))",
        "allShortestPaths(...) contains properties MapExpression(WrappedArray((PropertyKeyName(x),SignedDecimalIntegerLiteral(1)))). This is currently not supported. (line 1, column 17 (offset: 16))"
      )
    )
  }

  test("aggregation inside looping queries is not allowed") {

    val mess = "Can't use aggregating expressions inside of expressions executing over lists"
    executeAndEnsureError(
      "MATCH (n) RETURN ALL(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 45 (offset: 44))")

    executeAndEnsureError(
      "MATCH (n) RETURN ANY(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 45 (offset: 44))")

    executeAndEnsureError(
      "MATCH (n) RETURN NONE(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 46 (offset: 45))")

    executeAndEnsureError(
      "MATCH (n) RETURN SINGLE(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 48 (offset: 47))")

    executeAndEnsureError(
      "MATCH (n) RETURN [x in [1,2,3,4,5] | count(*) = 0]",
      s"$mess (line 1, column 38 (offset: 37))")

    executeAndEnsureError(
      "MATCH (n) RETURN [x in [1,2,3,4,5] WHERE count(*) = 0]",
      s"$mess (line 1, column 42 (offset: 41))")

    executeAndEnsureError(
      "MATCH (n) RETURN REDUCE(acc = 0, x in [1,2,3,4,5] | acc + count(*))",
      s"$mess (line 1, column 59 (offset: 58))")
  }

  test("error message should contain full query") {
    val query = "EXPLAIN MATCH (m), (n) RETURN m, n, o LIMIT 25"
    val error = intercept[QueryExecutionException](graph.withTx( tx => tx.execute(query)))

    val first :: second :: third :: Nil = error.getMessage.linesIterator.toList
    first should equal("Variable `o` not defined (line 1, column 37 (offset: 36))")
    second should equal(s""""$query"""")
    third should startWith(" "*37 + "^")
  }

  test("positions should not be cached") {
    executeAndEnsureError("EXPLAIN MATCH (m), (n) RETURN m, n, o LIMIT 25",
      "Variable `o` not defined (line 1, column 37 (offset: 36))")
    executeAndEnsureError("MATCH (m), (n) RETURN m, n, o LIMIT 25",
      "Variable `o` not defined (line 1, column 29 (offset: 28))")
  }

  test("give a nice error message when using unknown arguments in point") {
    executeAndEnsureError("RETURN point({xxx: 2.3, yyy: 4.5}) as point",
      "A map with keys 'xxx', 'yyy' is not describing a valid point, a point is described either by " +
        "using cartesian coordinates e.g. {x: 2.3, y: 4.5, crs: 'cartesian'} or using geographic " +
        "coordinates e.g. {latitude: 12.78, longitude: 56.7, crs: 'WGS-84'}. (line 1, column 14 (offset: 13))")
  }

  // Below follows tests on integer overflow errors. Not sure if integers have bounds from a language POV -- is this TCK material?
  test("should warn on over sized integer") {
    executeAndEnsureError(
      "RETURN 1766384027365849394756747201203756",
      "integer is too large (line 1, column 8 (offset: 7))"
    )
  }

  test("should warn when addition overflows") {
    executeAndEnsureError(
      s"RETURN ${Long.MaxValue} + 1",
      "result of 9223372036854775807 + 1 cannot be represented as an integer (line 1, column 28 (offset: 27))"
    )
  }

  test("should fail nicely when addition overflows in runtime") {
    executeAndEnsureErrorForAllRuntimes(
      s"RETURN $$t1 + $$t2",
      "long overflow", // "result of 9223372036854775807 + 1 cannot be represented as an integer",
      "t1" -> Long.MaxValue, "t2" -> 1
    )
  }

  test("should warn when subtraction underflows") {
    executeAndEnsureError(
      s"RETURN ${Long.MinValue} - 1",
      "result of -9223372036854775808 - 1 cannot be represented as an integer (line 1, column 29 (offset: 28))"
    )
  }

  test("should fail nicely when subtraction underflows in runtime") {
    executeAndEnsureErrorForAllRuntimes(
      s"RETURN $$t1 - $$t2",
      "long overflow", // "result of -9223372036854775808 - 1 cannot be represented as an integer",
      "t1" -> Long.MinValue, "t2" -> 1
    )
  }

  test("should warn when multiplication overflows") {
    executeAndEnsureError(
      s"RETURN ${Long.MaxValue} * 10",
      "result of 9223372036854775807 * 10 cannot be represented as an integer (line 1, column 28 (offset: 27))"
    )
  }

  test("should fail nicely when multiplication overflows in runtime") {
    executeAndEnsureErrorForAllRuntimes(
      s"RETURN $$t1 * $$t2",
      "long overflow", //"result of 9223372036854775807 * 10 cannot be represented as an integer",
      "t1" -> Long.MaxValue, "t2" -> 10
    )
  }

  test("should fail nicely when divide integer by zero in runtime") {
    executeAndEnsureErrorForAllRuntimes(
      s"RETURN $$t1 / $$t2",
      List("/ by zero", "divide by zero"),
      "t1" -> 1, "t2" -> 0
    )
  }

  test("should fail nicely when modulo integer by zero in runtime") {
    executeAndEnsureErrorForAllRuntimes(
      s"RETURN $$t1 % $$t2",
      List("/ by zero", "divide by zero"),
      "t1" -> 1, "t2" -> 0
    )
  }

  test("fail when parsing larger than 64 bit integers") {
    executeAndEnsureError(
      "RETURN toInteger('10508455564958384115')",
      "integer, 10508455564958384115, is too large")
  }

  test("should not be able to use more then Long.MaxValue for LIMIT in interpreted runtime") {
    val expectedErrorMessage = "Invalid input for LIMIT. Either the string does not have the appropriate format or the" +
      " provided number is bigger then 2^63-1 (line 1, column 55 (offset: 54))"
    val limit = "9223372036854775808" // this equals Long.MaxValue +1
    executeAndEnsureError(
      "CYPHER runtime = interpreted MATCH (n) RETURN n LIMIT " + limit,
      expectedErrorMessage)
  }

  test("should give an error message about aliasing importing WITH") {
    val msg = "Importing WITH should consist only of simple references to outside variables. Aliasing or expressions are not supported."

    executeAndEnsureError(
      "WITH 1 AS n CALL { WITH n AS x RETURN x } RETURN x",
      s"$msg (line 1, column 20 (offset: 19))"
    )
    executeAndEnsureError(
      "WITH 1 AS n CALL { WITH n, 2 AS m RETURN n as x } RETURN x",
      s"$msg (line 1, column 20 (offset: 19))"
    )
    executeAndEnsureError(
      "WITH 1 AS n CALL { WITH n+1 AS x RETURN x } RETURN x",
      s"$msg (line 1, column 20 (offset: 19))"
    )
    executeAndEnsureError(
      "WITH 1 AS n CALL { FROM g WITH n+1 AS x RETURN x } RETURN x",
      s"$msg (line 1, column 27 (offset: 26))"
    )
    executeAndEnsureError(
      "WITH 1 AS n CALL { USE g WITH n+1 AS x RETURN x } RETURN x",
      s"$msg (line 1, column 26 (offset: 25))"
    )
    executeAndEnsureError(
      "WITH 1 AS n CALL { WITH collect(n) AS ns RETURN ns } RETURN ns",
      s"$msg (line 1, column 20 (offset: 19))"
    )
    executeAndEnsureError(
      "WITH 1 AS n CALL { WITH n AS m RETURN m+1 AS x UNION WITH n RETURN n-1 AS x } RETURN x",
      s"$msg (line 1, column 20 (offset: 19))"
    )
    executeAndEnsureError(
      "WITH 1 AS n CALL { WITH n RETURN n+1 AS x UNION WITH n AS m RETURN m-1 AS x } RETURN x",
      s"$msg (line 1, column 49 (offset: 48))"
    )
    executeAndEnsureError(
      "WITH 1 AS n CALL { WITH *, 1 AS x RETURN x } RETURN x",
      s"$msg (line 1, column 20 (offset: 19))"
    )
  }

  test("should give an error message about bad importing WITH") {

    val badImportingWithClauses = Seq(
      "WITH n ORDER BY n.prop" -> "ORDER BY",
      "WITH n WHERE n.prop > 123" -> "WHERE",
      "WITH n LIMIT 10" -> "LIMIT",
      "WITH n SKIP 10" -> "SKIP",
      "WITH DISTINCT n" -> "DISTINCT"
    )

    for {
      (withClause, errorMsg) <- badImportingWithClauses
      query <- Seq(
        s"WITH 1 AS n CALL { $withClause RETURN x } RETURN x",
        s"WITH 1 AS n CALL { FROM g $withClause RETURN x } RETURN x",
        s"WITH 1 AS n CALL { USE g $withClause RETURN x } RETURN x",
        s"WITH 1 AS n CALL { WITH n RETURN n+1 AS x UNION $withClause RETURN n-1 AS x } RETURN x",
        s"WITH 1 AS n CALL { WITH n CALL { $withClause RETURN n+1 AS x } RETURN x } RETURN x",
      )
    } {
      executeAndEnsureError(
        query,
        s"Importing WITH should consist only of simple references to outside variables. $errorMsg is not allowed.")
   }
  }

  test("should give a custom error message when returning already declared variable from uncorrelated subquery") {
    val query =
      """WITH 1 AS x
        |CALL {
        |  RETURN 2 AS x // error here
        |}
        |RETURN x
        |""".stripMargin

    val expectedErrorLines = Seq(
      """Variable `x` already declared in outer scope (line 3, column 15 (offset: 33))""",
      """"  RETURN 2 AS x // error here"""",
      """               ^"""
    )

    executeAndMatchErrorMessageLines(query, expectedErrorLines)
  }

  test("should give a custom error message when returning already declared variable from nested uncorrelated subquery") {
    val query =
      """WITH 1 AS x
        |CALL {
        |  WITH 2 AS y
        |  CALL {
        |    RETURN 3 AS y // error here
        |  }
        |  RETURN y
        |}
        |RETURN x, y
        |""".stripMargin

    val expectedErrorLines = Seq(
      """Variable `y` already declared in outer scope (line 5, column 17 (offset: 58))""",
      """"    RETURN 3 AS y // error here"""",
      """                 ^"""
    )

    executeAndMatchErrorMessageLines(query, expectedErrorLines)
  }

  test("should give a custom error message when returning already declared variable from correlated subquery") {
    val query =
      """WITH 1 AS x
        |CALL {
        |  WITH x
        |  RETURN x // error here
        |}
        |RETURN x, y
        |""".stripMargin

    val expectedErrorLines = Seq(
      """Variable `x` already declared in outer scope (line 4, column 10 (offset: 37))""",
      """"  RETURN x // error here"""",
      """          ^"""
    )

    executeAndMatchErrorMessageLines(query, expectedErrorLines)
  }

  test("should give a custom error message when returning already declared variable from nested correlated subquery") {
    val query =
      """WITH 1 AS x
        |CALL {
        |  WITH x
        |  CALL {
        |    WITH x
        |    RETURN x // error here
        |  }
        |  RETURN x AS y
        |}
        |RETURN x, y
        |""".stripMargin

    val expectedErrorLines = Seq(
      """Variable `x` already declared in outer scope (line 6, column 12 (offset: 59))""",
      """"    RETURN x // error here"""",
      """            ^"""
    )

    executeAndMatchErrorMessageLines(query, expectedErrorLines)
  }

  test("should give a custom error message when returning already declared variable from uncorrelated union subquery") {
    val query =
      """WITH 1 AS x
        |CALL {
        |  RETURN 2 AS x
        |  UNION
        |  RETURN 3 AS x
        |  UNION
        |  RETURN 4 AS x // error here
        |}
        |RETURN x
        |""".stripMargin

    val expectedErrorLines = Seq(
      """Variable `x` already declared in outer scope (line 7, column 15 (offset: 81))""",
      """"  RETURN 4 AS x // error here"""",
      """               ^"""
    )

    executeAndMatchErrorMessageLines(query, expectedErrorLines)
  }

  test("should give a custom error message when returning already declared variable from correlated union subquery") {
    val query =
      """WITH 1 AS x
        |CALL {
        |  RETURN 2 AS x
        |  UNION
        |  WITH x RETURN x
        |  UNION
        |  WITH x RETURN x // error here
        |}
        |RETURN x
        |""".stripMargin

    val expectedErrorLines = Seq(
      """Variable `x` already declared in outer scope (line 7, column 17 (offset: 85))""",
      """"  WITH x RETURN x // error here"""",
      """                 ^"""
    )

    executeAndMatchErrorMessageLines(query, expectedErrorLines)
  }

  test("should give a custom error message when returning already declared variable from nested uncorrelated union subquery") {
    val query =
      """WITH 1 AS x
        |CALL {
        |  WITH 2 AS y
        |  CALL {
        |    RETURN 2 AS y
        |    UNION
        |    RETURN 3 AS y
        |    UNION
        |    RETURN 4 AS y // error here
        |  }
        |  RETURN y
        |}
        |RETURN x, y
        |""".stripMargin

    val expectedErrorLines = Seq(
      """Variable `y` already declared in outer scope (line 9, column 17 (offset: 114))""",
      """"    RETURN 4 AS y // error here"""",
      """                 ^"""
    )

    executeAndMatchErrorMessageLines(query, expectedErrorLines)
  }

  test("should give a custom error message when returning already declared variable from nested correlated union subquery") {
    val query =
      """WITH 1 AS x
        |CALL {
        |  WITH 2 AS y
        |  CALL {
        |    WITH y RETURN y
        |    UNION
        |    RETURN 3 AS y
        |    UNION
        |    WITH y RETURN y // error here
        |  }
        |  RETURN y
        |}
        |RETURN x, y
        |""".stripMargin

    val expectedErrorLines = Seq(
      """Variable `y` already declared in outer scope (line 9, column 19 (offset: 118))""",
      """"    WITH y RETURN y // error here"""",
      """                   ^"""
    )

    executeAndMatchErrorMessageLines(query, expectedErrorLines)
  }

  test("map projection on an undefined variable") {
    executeAndEnsureError("MATCH (n) RETURN x{.*}",
      "Variable `x` not defined (line 1, column 18 (offset: 17))")
  }

  test("Should disallow introducing variables in pattern expressions") {
    executeAndEnsureError("MATCH (x) WHERE (x)-[r]-(y) RETURN x",
      "PatternExpressions are not allowed to introduce new variables: 'r'. (line 1, column 22 (offset: 21))")
  }

  private def executeAndEnsureErrorForAllRuntimes(query: String, expected: String, params: (String,Any)*): Unit = {
    executeAndEnsureErrorForAllRuntimes(query, List(expected), params:_*)
  }

  private def executeAndEnsureErrorForAllRuntimes(query: String, expected: Seq[String], params: (String,Any)*): Unit = {
    val runtimes = List(PipelinedRuntimeName.name, SlottedRuntimeName.name, InterpretedRuntimeName.name) // Non-experimental runtimes that support arithmetics
    runtimes.foreach( r => executeAndEnsureError(s"CYPHER runtime=$r $query", expected, params: _*) )
  }

  private def executeAndEnsureError(query: String, expected: String, params: (String,Any)*): Unit = {
    executeAndEnsureError(query, List(expected), params: _*)
  }

  private def executeAndEnsureError(query: String, expected: Seq[String], params: (String,Any)*) {

    val expectedErrorString = expected.map(e => s"'$e'").mkString(" or ")
    graph.withTx( tx =>
      try {
        val jParams = new util.HashMap[String, Object]()
        params.foreach(kv => jParams.put(kv._1, kv._2.asInstanceOf[AnyRef]))

        tx.execute(query.fixNewLines, jParams).asScala.size
        fail(s"Did not get the expected error, expected: $expectedErrorString")
      } catch {
        case x: QueryExecutionException =>
          val actual = x.getMessage.linesIterator.next().trim
          if (!correctError(actual, expected)) {
            fail(s"Did not get the expected error, expected: $expectedErrorString actual: '$actual'")
          }
      }
    )
  }

  private def correctError(actualError: String, possibleErrors: Seq[String]): Boolean = {
    possibleErrors == Seq.empty || (actualError != null && possibleErrors.exists(s => actualError.replaceAll("\\r", "").contains(s.replaceAll("\\r", ""))))
  }

  private def executeAndMatchErrorMessageLines(query: String, expectedErrorMessageLines: Seq[String]): Unit = {
    val error = intercept[QueryExecutionException](graph.withTx(_.execute(query)))
    error.getMessage.linesIterator.toSeq shouldEqual expectedErrorMessageLines
  }
}
