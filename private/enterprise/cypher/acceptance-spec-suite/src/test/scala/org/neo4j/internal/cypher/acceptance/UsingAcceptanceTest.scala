/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.lang.Boolean.FALSE
import java.lang.Boolean.TRUE

import com.neo4j.cypher.RunWithConfigTestSupport
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.api.exceptions.Status

class UsingAcceptanceTest extends ExecutionEngineFunSuite with RunWithConfigTestSupport with CypherComparisonSupport {
  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(GraphDatabaseSettings.cypher_hints_error -> TRUE)

  test("should use index on literal value") {
    val node = createLabeledNode(Map("id" -> 123), "Foo")
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id=123
        | RETURN f
      """.stripMargin

    val result = executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("f"))))

    result.columnAs[Node]("f").toList should equal(List(node))
  }

  test("should use named index on literal value") {
    val node = createLabeledNode(Map("id" -> 123), "Foo")
    graph.createIndexWithName("my_index", "Foo", "id")
    val query =
      """
        |PROFILE
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id=123
        | RETURN f
      """.stripMargin

    val result = executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek")
        .containingVariables("f").containingArgumentForIndexPlan("f", "Foo", Seq("id")))))

    result.columnAs[Node]("f").toList should equal(List(node))
  }

  test("should use index on literal map expression") {
    val nodes = Range(0,125).map(i => createLabeledNode(Map("id" -> i), "Foo"))
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id={id: 123}.id
        | RETURN f
      """.stripMargin

    val result = executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("f"))))

    result.columnAs[Node]("f").toSet should equal(Set(nodes(123)))
  }

  test("should use index on variable defined from literal value") {
    val node = createLabeledNode(Map("id" -> 123), "Foo")
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | WITH 123 AS row
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id=row
        | RETURN f
      """.stripMargin

    val result = executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("f"))))

    result.columnAs[Node]("f").toList should equal(List(node))
  }

  test("fail if using a variable with label not used in match") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    failWithError(Configs.All, "match (n)-->() using index n:Person(name) where n.name = 'kabam' return n",
      "Cannot use index hint in this context. Must use label on node that hint is referring to.")
  }

  test("fail if using a variable with label not used in match (named index)") {
    // GIVEN
    graph.createIndexWithName("my_index", "Person", "name")

    // WHEN
    failWithError(Configs.All, "match (n)-->() using index n:Person(name) where n.name = 'kabam' return n",
      "Cannot use index hint in this context. Must use label on node that hint is referring to.")
  }

  test("fail if using an hint for a non existing index") {
    // GIVEN: NO INDEX

    // WHEN
    failWithError(Configs.All, "match (n:Person)-->() using index n:Person(name) where n.name = 'kabam' return n", "No such index")
  }

  test("fail if using hints with unusable equality predicate") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    failWithError(Configs.All, "match (n:Person)-->() using index n:Person(name) where n.name <> 'kabam' return n","Cannot use index hint in this context")
  }

  test("fail if using hints with unusable equality predicate (named index)") {
    // GIVEN
    graph.createIndexWithName("my_index", "Person", "name")

    // WHEN
    failWithError(Configs.All, "match (n:Person)-->() using index n:Person(name) where n.name <> 'kabam' return n", "Cannot use index hint in this context")
  }

  test("fail if joining index hints in equality predicates") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.createIndex("Food", "name")

    // WHEN
    failWithError(Configs.All,
      "match (n:Person)-->(m:Food) using index n:Person(name) using index m:Food(name) where n.name = m.name return n",
      "Failed to fulfil the hints of the query.")
  }

  test("scan hints are handled by ronja") {
    executeWith(Configs.All, "match (n:Person) using scan n:Person return n").toList
  }

  test("fail when equality checks are done with OR") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    failWithError(Configs.All, "match (n)-->() using index n:Person(name) where n.name = 'kabam' OR n.name = 'kaboom' return n",
      "Cannot use index hint in this context. Must use label on node that hint is referring to.")
  }

  test("correct status code when no index") {

    // GIVEN
    val query =
      """MATCH (n:Test)
        |USING INDEX n:Test(foo)
        |WHERE n.foo = $foo
        |RETURN n""".stripMargin

    // WHEN
    failWithError(Configs.All, query, "No such index", params = Map("foo" -> 42))
  }

  test("should succeed (i.e. no warnings or errors) if executing a query using a 'USING INDEX' which can be fulfilled") {
    runWithConfig() {
      db =>
        graph.withTx( tx => tx.execute("CREATE INDEX FOR (n:Person) ON (n.name)"))
        graph.withTx( tx => tx.execute("CALL db.awaitIndexes()"))
        db.withTx( tx => shouldHaveNoWarnings(
          tx.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n")
        )
        )
    }
  }

  test("should succeed (i.e. no warnings or errors) if executing a query using a 'USING INDEX' which can be fulfilled (named index)") {
    runWithConfig() {
      db =>
        graph.withTx( tx => tx.execute("CREATE INDEX my_index FOR (n:Person) ON (n.name)"))
        graph.withTx( tx => tx.execute("CALL db.awaitIndex('my_index')"))
        db.withTx( tx => shouldHaveNoWarnings(
          tx.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n")
        )
        )
    }
  }

  test("should generate a warning if executing a query using a 'USING INDEX' which cannot be fulfilled") {
    runWithConfig() {
      db =>
        db.withTx( tx =>
          shouldHaveWarning(tx.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n"), Status.Schema.IndexNotFound)
        )
    }
  }

  test("should generate a warning if executing a query using a 'USING INDEX' which cannot be fulfilled, and hint errors are turned off") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> FALSE) {
      db =>
        db.withTx( tx =>
          shouldHaveWarning(tx.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n"), Status.Schema.IndexNotFound)
        )
    }
  }

  test("should generate an error if executing a query using EXPLAIN and a 'USING INDEX' which cannot be fulfilled, and hint errors are turned on") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> TRUE) {
      db =>
        db.withTx( tx =>
          intercept[QueryExecutionException](
            tx.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n")
          ).getStatusCode should equal("Neo.ClientError.Schema.IndexNotFound"))
    }
  }

  test("should generate an error if executing a query using a 'USING INDEX' which cannot be fulfilled, and hint errors are turned on") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> TRUE) {
      db =>
        db.withTx( tx =>
          intercept[QueryExecutionException](
            tx.execute(s"MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n")
          ).getStatusCode should equal("Neo.ClientError.Schema.IndexNotFound")
        )
    }
  }

  test("should generate an error if executing a query using a 'USING INDEX' for an existing index but which cannot be fulfilled for the query, and hint errors are turned on") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> TRUE) {
      db =>
        db.withTx( tx => {
          tx.execute("CREATE INDEX FOR (n:Person) ON (n.email)")
          intercept[QueryExecutionException](
            tx.execute(s"MATCH (n:Person) USING INDEX n:Person(email) WHERE n.name = 'John' RETURN n")
          ).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        })
    }
  }

  test("should generate an error if executing a query using a 'USING INDEX' for an existing index but which cannot be fulfilled for the query, even when hint errors are not turned on") {
    runWithConfig() {
      db =>
        db.withTx( tx => {
          tx.execute("CREATE INDEX FOR (n:Person) ON (n.email)")
          intercept[QueryExecutionException](
            tx.execute(s"MATCH (n:Person) USING INDEX n:Person(email) WHERE n.name = 'John' RETURN n")
          ).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        })
    }
  }

  test("should be able to use index hints on IN expressions") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob'] RETURN n")

    //THEN
    result.toList should equal(List(Map("n" -> jake)))
  }

  test("should be able to use index hints on IN collections with duplicates") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob','Jacob'] RETURN n")

    //THEN
    result.toList should equal(List(Map("n" -> jake)))
  }

  test("should be able to use index hints on IN a null value") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN null RETURN n")

    //THEN
    result.toList should equal(List())
  }

  test("should be able to use index hints on IN a collection parameter") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN $coll RETURN n",
      params = Map("coll" -> List("Jacob")))

    //THEN
    result.toList should equal(List(Map("n" -> jake)))
  }

  test("does not accept multiple index hints for the same variable") {
    // GIVEN
    graph.createIndex("Entity", "source")
    graph.createIndex("Person", "first_name")
    createNode("source" -> "form1")
    createNode("first_name" -> "John")

    // WHEN THEN

    failWithError(Configs.All,
      "MATCH (n:Entity:Person) " +
        "USING INDEX n:Person(first_name) " +
        "USING INDEX n:Entity(source) " +
        "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
        "RETURN n;",
     "Multiple index hints for same variable are not supported")
  }

  test("does not accept multiple index hints for the same variable (named indexes)") {
    // GIVEN
    graph.createIndexWithName("index1", "Entity", "source")
    graph.createIndexWithName("index2", "Person", "first_name")
    createNode("source" -> "form1")
    createNode("first_name" -> "John")

    // WHEN THEN

    failWithError(Configs.All,
      "MATCH (n:Entity:Person) " +
        "USING INDEX n:Person(first_name) " +
        "USING INDEX n:Entity(source) " +
        "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
        "RETURN n;",
      "Multiple index hints for same variable are not supported")
  }

  test("does not accept multiple scan hints for the same variable") {
    failWithError(Configs.All,
      "MATCH (n:Entity:Person) " +
        "USING SCAN n:Person " +
        "USING SCAN n:Entity " +
        "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
        "RETURN n;",
      "Multiple index hints for same variable are not supported")

  }

  test("does not accept multiple mixed hints for the same variable") {
    failWithError(Configs.All,
      "MATCH (n:Entity:Person) " +
        "USING SCAN n:Person " +
        "USING INDEX n:Entity(first_name) " +
        "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
        "RETURN n;",
      "Multiple index hints for same variable are not supported")
  }

  test("does not accept multiple join hints for the same variables") {
    failWithError(Configs.All,
      """MATCH (liskov:Scientist { name:'Liskov' })-[:KNOWS]->(wing:Scientist { name:'Wing' })-[:RESEARCHED]->(cs:Science { name:'Computer Science' })<-[:RESEARCHED]-(liskov)
        |USING JOIN ON liskov, cs
        |USING JOIN ON liskov, cs
        |RETURN wing.born AS column""".stripMargin,
      "Multiple join hints for same variable are not supported")
  }

  test("scan hint must fail if using a variable not used in the query") {
    // GIVEN

    // WHEN
    failWithError(Configs.All, "MATCH (n:Person)-->() USING SCAN x:Person return n", "Variable `x` not defined")
  }

  test("scan hint must fail if using label not used in the query") {
    // GIVEN

    // WHEN
    failWithError(Configs.All, "MATCH (n)-->() USING SCAN n:Person return n",
      "Cannot use label scan hint in this context.")
  }

  test("should succeed (i.e. no warnings or errors) if executing a query using a 'USING SCAN'") {
    runWithConfig() {
      engine =>
        engine.withTx( tx =>
          shouldHaveNoWarnings(
            tx.execute(s"EXPLAIN MATCH (n:Person) USING SCAN n:Person WHERE n.name = 'John' RETURN n")
          )
        )
    }
  }

  test("should succeed if executing a query using both 'USING SCAN' and 'USING INDEX' if index exists") {
    runWithConfig() {
      engine =>
        engine.withTx( tx => tx.execute("CREATE INDEX FOR (n:Person) ON (n.name)"))
        engine.withTx( tx => tx.execute("CALL db.awaitIndexes()"))
        engine.withTx( tx => shouldHaveNoWarnings(
          tx.execute(s"EXPLAIN MATCH (n:Person), (c:Company) USING INDEX n:Person(name) USING SCAN c:Company WHERE n.name = 'John' RETURN n")
        ))
    }
  }

  test("should succeed if executing a query using both 'USING SCAN' and 'USING INDEX' if named index exists") {
    runWithConfig() {
      engine =>
        engine.withTx( tx => tx.execute("CREATE INDEX my_index FOR (n:Person) ON (n.name)"))
        engine.withTx( tx => tx.execute("CALL db.awaitIndex('my_index')"))
        engine.withTx( tx => shouldHaveNoWarnings(
          tx.execute(s"EXPLAIN MATCH (n:Person), (c:Company) USING INDEX n:Person(name) USING SCAN c:Company WHERE n.name = 'John' RETURN n")
        ))
    }
  }

  test("should fail outright if executing a query using a 'USING SCAN' and 'USING INDEX' on the same variable, even if index exists") {
    runWithConfig() {
      engine =>
        engine.withTx( tx => {
          tx.execute("CREATE INDEX FOR (n:Person) ON (n.name)")
          intercept[QueryExecutionException](
            tx.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) USING SCAN n:Person WHERE n.name = 'John' RETURN n")
          ).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        })
    }
  }

  test("should fail outright if executing a query using a 'USING SCAN' and 'USING INDEX' on the same variable, even if named index exists") {
    runWithConfig() {
      engine =>
        engine.withTx( tx => {
          tx.execute("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
          intercept[QueryExecutionException](
            tx.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) USING SCAN n:Person WHERE n.name = 'John' RETURN n")
          ).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        })
    }
  }

  test("should handle join hint on the start node of a single hop pattern") {
    val initQuery = "CREATE (a:A {prop: 'foo'})-[:R]->(b:B {prop: 'bar'})"
    graph.withTx( tx => tx.execute(initQuery))

    val query =
      s"""
         |MATCH (a:A)-->(b:B)
         |USING JOIN ON a
         |RETURN a.prop AS res""".stripMargin

    val result = executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("a"))))

    result.toList should equal (List(Map("res" -> "foo")))
  }

  test("should handle join hint on the end node of a single hop pattern") {
    val initQuery = "CREATE (a:A {prop: 'foo'})-[:R]->(b:B {prop: 'bar'})"
    graph.withTx( tx => tx.execute(initQuery))

    val query =
      s"""
         |MATCH (a:A)-->(b:B)
         |USING JOIN ON b
         |RETURN b.prop AS res""".stripMargin

    val result = executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("b"))))

    result.toList should equal (List(Map("res" -> "bar")))
  }

  test("should fail when join hint is applied to an undefined node") {
    failWithError(Configs.All,
      s"""
         |MATCH (a:A)-->(b:B)<--(c:C)
         |USING JOIN ON d
         |RETURN a.prop""".stripMargin,
      "Variable `d` not defined")
  }

  test("should fail when join hint is applied to a single node") {
    failWithError(Configs.All,
      s"""
         |MATCH (a:A)
         |USING JOIN ON a
         |RETURN a.prop""".stripMargin,
      "Cannot use join hint for single node pattern")
  }

  test("should fail when join hint is applied to a relationship") {
    failWithError(Configs.All,
      s"""
         |MATCH (a:A)-[r1]->(b:B)-[r2]->(c:C)
         |USING JOIN ON r1
         |RETURN a.prop""".stripMargin,
      "Type mismatch: expected Node but was Relationship")
  }

  test("should fail when join hint is applied to a path") {
    failWithError(Configs.All,
      s"""
         |MATCH p=(a:A)-->(b:B)-->(c:C)
         |USING JOIN ON p
         |RETURN a.prop""".stripMargin,
      "Type mismatch: expected Node but was Path")
  }

  test("should be able to use join hints for multiple hop pattern") {
    val a = createNode(("prop", "foo"))
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val e = createNode(("prop", "foo"))

    relate(a, b, "X")
    relate(b, c, "X")
    relate(c, d, "X")
    relate(d, e, "X")

    val result = executeWith(Configs.All,
      s"""
         |MATCH (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)-[:X]->(e)
         |USING JOIN ON c
         |WHERE a.prop = e.prop
         |RETURN c""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("c"))))

    result.toList should equal(List(Map("c" -> c)))
  }

  test("should be able to use join hints for queries with var length pattern") {
    val a = createLabeledNode(Map("prop" -> "foo"), "Foo")
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val e = createLabeledNode(Map("prop" -> "foo"), "Bar")

    relate(a, b, "X")
    relate(b, c, "X")
    relate(c, d, "X")
    relate(e, d, "Y")

    val result = executeWith(Configs.VarExpand,
      s"""
         |MATCH (a:Foo)-[:X*]->(b)<-[:Y]->(c:Bar)
         |USING JOIN ON b
         |WHERE a.prop = c.prop
         |RETURN c""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("b"))))

    result.toList should equal(List(Map("c" -> e)))
  }

  test("should be able to use multiple join hints") {
    val a = createNode(("prop", "foo"))
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val e = createNode(("prop", "foo"))

    relate(a, b, "X")
    relate(b, c, "X")
    relate(c, d, "X")
    relate(d, e, "X")

    executeWith(Configs.All,
      s"""
         |MATCH (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)-[:X]->(e)
         |USING JOIN ON b
         |USING JOIN ON c
         |USING JOIN ON d
         |WHERE a.prop = e.prop
         |RETURN b, d""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("b"))
        planDescription should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("c"))
        planDescription should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("d"))
      }))
  }

  test("should work when join hint is applied to x in (a)-->(x)<--(b)") {
    val a = createNode()
    val b = createNode()
    val x = createNode()

    relate(a, x)
    relate(b, x)

    val query =
      s"""
         |MATCH (a)-->(x)<--(b)
         |USING JOIN ON x
         |RETURN x""".stripMargin

    executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("x"))))
  }

  test("should work when join hint is applied to x in (a)-->(x)<--(b) where a and b can use an index") {
    graph.createIndex("Person", "name")

    val tom = createLabeledNode(Map("name" -> "Tom Hanks"), "Person")
    val meg = createLabeledNode(Map("name" -> "Meg Ryan"), "Person")

    val harrysally = createLabeledNode(Map("title" -> "When Harry Met Sally"), "Movie")

    relate(tom, harrysally, "ACTS_IN")
    relate(meg, harrysally, "ACTS_IN")

    1 until 10 foreach { i =>
      createLabeledNode(Map("name" -> s"Person $i"), "Person")
    }

    1 until 90 foreach { _ =>
      createLabeledNode("Person")
    }

    1 until 20 foreach { _ =>
      createLabeledNode("Movie")
    }

    val query =
      s"""
         |MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
         |USING JOIN ON x
         |RETURN x""".stripMargin

    executeWith(Configs.All, query)
  }

  test("should work when join hint is applied to x in (a)-->(x)<--(b) where a and b are labeled") {
    val tom = createLabeledNode(Map("name" -> "Tom Hanks"), "Person")
    val meg = createLabeledNode(Map("name" -> "Meg Ryan"), "Person")

    val harrysally = createLabeledNode(Map("title" -> "When Harry Met Sally"), "Movie")

    relate(tom, harrysally, "ACTS_IN")
    relate(meg, harrysally, "ACTS_IN")

    1 until 10 foreach { i =>
      createLabeledNode(Map("name" -> s"Person $i"), "Person")
    }

    1 until 90 foreach { _ =>
      createLabeledNode("Person")
    }

    1 until 20 foreach { _ =>
      createLabeledNode("Movie")
    }

    val query =
      s"""
         |MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
         |USING JOIN ON x
         |RETURN x""".stripMargin

    executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("x"))
        planDescription.toString should not include "AllNodesScan"
      }))
  }

  test("should work when join hint is applied to x in (a)-->(x)<--(b) where using index hints on a and b") {
    graph.createIndex("Person", "name")

    val tom = createLabeledNode(Map("name" -> "Tom Hanks"), "Person")
    val meg = createLabeledNode(Map("name" -> "Meg Ryan"), "Person")

    val harrysally = createLabeledNode(Map("title" -> "When Harry Met Sally"), "Movie")

    relate(tom, harrysally, "ACTS_IN")
    relate(meg, harrysally, "ACTS_IN")

    1 until 10 foreach { i =>
      createLabeledNode(Map("name" -> s"Person $i"), "Person")
    }

    1 until 90 foreach { _ =>
      createLabeledNode("Person")
    }

    1 until 20 foreach { _ =>
      createLabeledNode("Movie")
    }

    val query =
      s"""
         |MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
         |USING INDEX a:Person(name)
         |USING INDEX b:Person(name)
         |USING JOIN ON x
         |RETURN x""".stripMargin

    executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("x"))
        planDescription.toString should not include "AllNodesScan"
      }))
  }

  test("should work when join hint is applied to x in (a)-->(x)<--(b) where using index hints on a and b (named index)") {
    graph.createIndexWithName("my_index", "Person", "name")

    val tom = createLabeledNode(Map("name" -> "Tom Hanks"), "Person")
    val meg = createLabeledNode(Map("name" -> "Meg Ryan"), "Person")

    val harrysally = createLabeledNode(Map("title" -> "When Harry Met Sally"), "Movie")

    relate(tom, harrysally, "ACTS_IN")
    relate(meg, harrysally, "ACTS_IN")

    1 until 10 foreach { i =>
      createLabeledNode(Map("name" -> s"Person $i"), "Person")
    }

    1 until 90 foreach { _ =>
      createLabeledNode("Person")
    }

    1 until 20 foreach { _ =>
      createLabeledNode("Movie")
    }

    val query =
      s"""
         |MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
         |USING INDEX a:Person(name)
         |USING INDEX b:Person(name)
         |USING JOIN ON x
         |RETURN x""".stripMargin

    executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("x"))
        planDescription.toString should not include "AllNodesScan"
      }))
  }

  test("should work when join hint is applied to x in (a)-->(x)<--(b) where x can use an index") {
    graph.createIndex("Movie", "title")

    val tom = createLabeledNode(Map("name" -> "Tom Hanks"), "Person")
    val meg = createLabeledNode(Map("name" -> "Meg Ryan"), "Person")

    val harrysally = createLabeledNode(Map("title" -> "When Harry Met Sally"), "Movie")

    relate(tom, harrysally, "ACTS_IN")
    relate(meg, harrysally, "ACTS_IN")

    1 until 10 foreach { i =>
      createLabeledNode(Map("name" -> s"Person $i"), "Person")
    }

    1 until 90 foreach { _ =>
      createLabeledNode("Person")
    }

    1 until 20 foreach { i =>
      createLabeledNode(Map("title" -> s"Movie $i"), "Movie")
    }

    val query =
      s"""
         |MATCH (a:Person)-[:ACTS_IN]->(x:Movie {title: "When Harry Met Sally"})<-[:ACTS_IN]-(b:Person)
         |USING JOIN ON x
         |RETURN x""".stripMargin

    executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("x"))
        planDescription should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("x"))
      }))
  }

  test("should handle using index hint on both ends of pattern") {
    graph.createIndex("Person", "name")

    val tom = createLabeledNode(Map("name" -> "Tom Hanks"), "Person")
    val meg = createLabeledNode(Map("name" -> "Meg Ryan"), "Person")

    val harrysally = createLabeledNode(Map("title" -> "When Harry Met Sally"), "Movie")

    relate(tom, harrysally, "ACTS_IN")
    relate(meg, harrysally, "ACTS_IN")

    1 until 10 foreach { i =>
      createLabeledNode(Map("name" -> s"Person $i"), "Person")
    }

    1 until 90 foreach { _ =>
      createLabeledNode("Person")
    }

    1 until 20 foreach { _ =>
      createLabeledNode("Movie")
    }

    val query =
      """MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
        |USING INDEX a:Person(name)
        |USING INDEX b:Person(name)
        |RETURN x""".stripMargin

    executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.nTimes(1, aPlan("NodeHashJoin").containingArgument("x"))
        planDescription.toString should not include "AllNodesScan"
      }))
  }

  test("Using index hints with two indexes should produce cartesian product"){
    val startNode = createLabeledNode(Map("name" -> "Neo"), "Person")
    val endNode = createLabeledNode(Map("name" -> "Trinity"), "Person")
    relate(startNode, endNode, "knows")
    graph.createIndex("Person","name")

    val query =
      """MATCH (k:Person {name: 'Neo'}), (t:Person {name: 'Trinity'})
        |using index k:Person(name)
        |using index t:Person(name)
        |MATCH p=(k)-[:knows*0..5]-(t)
        |RETURN count(p)
        |""".stripMargin

    executeWith(Configs.VarExpand, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("k"))
          .and(includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("t")))
          .or(includeSomewhere.aPlan("MultiNodeIndexSeek").containingVariables("k", "t"))
        planDescription.toString should not include "AllNodesScan"
      }))
  }

  test("Using index hints with two named indexes should produce cartesian product"){
    val startNode = createLabeledNode(Map("name" -> "Neo"), "Person")
    val endNode = createLabeledNode(Map("name" -> "Trinity"), "Person")
    relate(startNode, endNode, "knows")
    graph.createIndexWithName("my_index", "Person","name")

    val query =
      """MATCH (k:Person {name: 'Neo'}), (t:Person {name: 'Trinity'})
        |using index k:Person(name)
        |using index t:Person(name)
        |MATCH p=(k)-[:knows*0..5]-(t)
        |RETURN count(p)
        |""".stripMargin

    executeWith(Configs.VarExpand, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should
          includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("k"))
            .and(includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("t")))
            .or(includeSomewhere.aPlan("MultiNodeIndexSeek").containingVariables("k", "t"))
        planDescription.toString should not include "AllNodesScan"
      }))
  }

  test("USING INDEX hint should not clash with used variables") {
    graph.createIndex("PERSON", "id")

    val result = executeWith(Configs.All,
      """MATCH (actor:PERSON {id: 1})
        |USING INDEX actor:PERSON(id)
        |WITH 14 as id
        |RETURN 13 as id""".stripMargin)

    result.toList should be(empty)
  }

  test("should accept two hints on a single relationship") {
    val startNode = createLabeledNode(Map("prop" -> 1), "PERSON")
    val endNode = createLabeledNode(Map("prop" -> 2), "PERSON")
    relate(startNode, endNode)
    graph.createIndex("PERSON", "prop")

    val query =
      """MATCH (a:PERSON {prop: 1})-->(b:PERSON {prop: 2})
        |USING INDEX a:PERSON(prop)
        |USING INDEX b:PERSON(prop)
        |RETURN a, b""".stripMargin
    val result = executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("a"))
          .and(includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("b")))
          .or(includeSomewhere.aPlan("MultiNodeIndexSeek").containingVariables("a", "b"))
      }))

    result.toList should equal(List(Map("a" -> startNode, "b" -> endNode)))
  }

  test("should handle multiple hints on longer pattern") {
    val initQuery = "CREATE (a:Node {prop: 'foo'})-[:R]->(b:Node {prop: 'bar'})-[:R]->(c:Node {prop: 'baz'})"
    graph.withTx( tx => tx.execute(initQuery))
    graph.createIndex("Node", "prop")

    val query =
      s"""MATCH (a:Node)-->(b:Node)-->(c:Node)
         |USING INDEX a:Node(prop)
         |USING INDEX b:Node(prop)
         |WHERE a.prop = 'foo' and b.prop = 'bar'
         |RETURN b.prop AS res""".stripMargin

    val result = executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should
          includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("a"))
          .or(includeSomewhere.aPlan("MultiNodeIndexSeek").containingVariables("a"))
        planDescription should
          includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("b"))
          .or(includeSomewhere.aPlan("MultiNodeIndexSeek").containingVariables("b"))
      }))

    result.toList should equal (List(Map("res" -> "bar")))
  }

  test("should accept hint on composite index") {
    val node = createLabeledNode(Map("bar" -> 5, "baz" -> 3), "Foo")
    graph.createIndex("Foo", "bar", "baz")

    val query =
      """ MATCH (f:Foo)
        | USING INDEX f:Foo(bar,baz)
        | WHERE f.bar=5 and f.baz=3
        | RETURN f
      """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("f"))
      }))

    result.columnAs[Node]("f").toList should equal(List(node))
  }

  test("should accept hint on named composite index") {
    val node = createLabeledNode(Map("bar" -> 5, "baz" -> 3), "Foo")
    graph.createIndexWithName("my_index", "Foo", "bar", "baz")

    val query =
      """ MATCH (f:Foo)
        | USING INDEX f:Foo(bar,baz)
        | WHERE f.bar=5 and f.baz=3
        | RETURN f
      """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek")
          .containingVariables("f").containingArgumentForIndexPlan("f", "Foo", Seq("bar", "baz")))
      }))

    result.columnAs[Node]("f").toList should equal(List(node))
  }

  test("should accept hint and use index on correct node") {
    // Given
    graph.createIndex("Object", "name")
    createLabeledNode(Map("name" -> "a"), "Object")

    val query =
      """
        |MATCH (o1:Object)
        |MATCH (o2:Object)
        |USING INDEX o2:Object(name)
        |WHERE o2.name = o1.name
        |RETURN o1.name
        |""".stripMargin

    // When
    val result = executeWith(Configs.All, query)

    // Then
    result.executionPlanDescription() should includeSomewhere.aPlan("NodeIndexSeek")
      .containingVariables("o2")
      .containingArgumentForIndexPlan("o2", "Object", Seq("name"))
    result.toComparableResult should be(Seq(Map("o1.name" -> "a")))
  }

  test("should handle join hint solved multiple times") {
    val initQuery = "CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node), (d:Node)-[:R]->(b)-[:R]->(e:Node)"
    graph.withTx( tx => tx.execute(initQuery))

    val query =
      s"""MATCH (a:Node)-->(b:Node),(b)-->(c:Node),(d:Node)-->(b),(b)-->(e:Node)
         |USING JOIN ON b
         |RETURN count(*) as c""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy = ComparePlansWithAssertion({ plan =>
      plan should includeSomewhere.nTimes(1, aPlan("NodeHashJoin"))
    }))

    result.toList should equal (List(Map("c" -> 4)))
  }
}
