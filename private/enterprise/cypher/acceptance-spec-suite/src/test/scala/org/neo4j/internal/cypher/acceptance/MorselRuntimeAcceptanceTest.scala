/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.{Node, Result}
import org.neo4j.internal.cypher.acceptance.MorselRuntimeAcceptanceTest.MORSEL_SIZE
import org.scalactic.{Equality, TolerantNumerics}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object MorselRuntimeAcceptanceTest {
  val MORSEL_SIZE = 4 // The morsel size to use in the config for testing
}

abstract class MorselRuntimeAcceptanceTest extends ExecutionEngineFunSuite {

  implicit val dblEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.0001)

  test("should not use morsel by default") {
    //Given
    val result = graph.execute("MATCH (n) RETURN n")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should not equal "MORSEL"
  }

  test("should be able to ask for morsel") {
    //Given
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should warn that morsels are experimental") {
    //Given
    import scala.collection.JavaConverters._

    val result = graph.execute("CYPHER runtime=morsel EXPLAIN MATCH (n) RETURN n")

    // When (exhaust result)
    val notifications = result.getNotifications.asScala.toSet

    //Then
    notifications.head.getDescription should equal("You are using an experimental feature (use the morsel runtime at " +
                                                     "your own peril, not recommended to be run on production systems)")

  }

  test("should support count with no grouping") {
    //Given
    createNode("prop" -> "foo")
    createNode()
    createNode()
    createNode("prop" -> "foo")
    createNode("prop" -> "foo")
    createNode("prop" -> "foo")

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN count(n.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(Map("count(n.prop)" -> 4)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support multiple counts with no grouping") {
    //Given
    relate(createNode("prop" -> "foo"),createNode())
    relate(createNode(), createNode("prop" -> "foo"))
    relate(createNode("prop" -> "foo"), createNode("prop" -> "foo"))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n)-->(m) RETURN count(n.prop), count(m.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(Map("count(n.prop)" -> 2, "count(m.prop)" -> 2)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support count with grouping") {
    //Given
    createNode("prop" -> "foo", "count" -> 1)
    createNode("prop" -> "foo", "count" -> 1)
    createNode("prop" -> "bar")
    createNode("prop" -> "bar", "count" -> 1)
    createNode()

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.prop, count(n.count)")

    //Then
    asScalaResult(result).toSet should equal(Set(
      Map("n.prop" -> "foo", "count(n.count)" -> 2),
      Map("n.prop" -> "bar", "count(n.count)" -> 1),
      Map("n.prop" -> null, "count(n.count)" -> 0)
      ))

    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support average") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN avg(n.prop)")

    //Then
    val resultSet = asScalaResult(result).toSet
    resultSet should have size 1
    val singleMap = resultSet.head
    assert(55.0000001 === singleMap("avg(n.prop)"))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support average with grouping") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i, "group" -> (if (i > 50) "FOO" else "BAR")))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.group, avg(n.prop)")

    //Then
    val setResult = asScalaResult(result).toSet
    setResult.foreach { map =>
      if (map("n.group") == "FOO") {
        assert(80.0000001 === map("avg(n.prop)"))
      } else if (map("n.group") == "BAR") {
        assert(30.0000001 === map("avg(n.prop)"))
      } else {
        fail(s"Unexpected grouping column: ${map("n.group")}")
      }
    }
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support max") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN max(n.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(Map("max(n.prop)" -> 100)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support max with grouping") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i, "group" -> (if (i > 50) "FOO" else "BAR")))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.group, max(n.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(
      Map("n.group" -> "FOO", "max(n.prop)" -> 100),
      Map("n.group" -> "BAR", "max(n.prop)" -> 50)
    ))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support min") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN min(n.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(Map("min(n.prop)" -> 10)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support min with grouping") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i, "group" -> (if (i > 50) "FOO" else "BAR")))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.group, min(n.prop)")

    //Then
    asScalaResult(result).toSet should equal(Set(
      Map("n.group" -> "FOO", "min(n.prop)" -> 60),
      Map("n.group" -> "BAR", "min(n.prop)" -> 10)
    ))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support collect") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN collect(n.prop)")

    //Then
    asScalaResult(result).toList.head("collect(n.prop)").asInstanceOf[Seq[_]] should contain theSameElementsAs List(10, 20, 30, 40, 50, 60,
                                                                                               70, 80, 90, 100)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support top n < morsel size") {
    //Given
    1 to 100 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.prop ORDER BY n.prop DESC LIMIT 2")

    //Then
    val scalaresult = asScalaResult(result).toList
    scalaresult should equal(List(Map("n.prop" -> 100), Map("n.prop" -> 99)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support top n > morsel size") {
    //Given
    1 to 100 foreach(i => createNode("prop" -> i))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.prop ORDER BY n.prop DESC LIMIT 6")

    //Then
    val scalaresult = asScalaResult(result).toList
    scalaresult should equal(List(Map("n.prop" -> 100), Map("n.prop" -> 99), Map("n.prop" -> 98),
      Map("n.prop" -> 97), Map("n.prop" -> 96), Map("n.prop" -> 95)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support collect with grouping") {
    //Given
    10 to 100 by 10 foreach(i => createNode("prop" -> i, "group" -> (if (i > 50) "FOO" else "BAR")))

    //When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n) RETURN n.group, collect(n.prop)")

    //Then
    val first :: second :: Nil = asScalaResult(result).toList.sortBy(_("n.group").asInstanceOf[String])
    first("n.group") should equal("BAR")
    first("collect(n.prop)").asInstanceOf[Seq[_]] should contain theSameElementsAs List(10, 20, 30, 40, 50)

    second("n.group") should equal("FOO")
    second("collect(n.prop)").asInstanceOf[Seq[_]] should contain theSameElementsAs List(60, 70, 80, 90, 100)

    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support index scans") {
    // Given
    graph.createIndex("Person", "name")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))
    val names = (1 to 91).map(i => s"Satia$i")
    names.foreach(name => createLabeledNode(Map("name" -> name), "Person"))

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) WHERE EXISTS(n.name) RETURN n.name ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet.map(map => map("n.name")) should equal(names.toSet)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support index seek") {
    // Given
    graph.createIndex("Person", "name")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))
    val names = (1 to 91).map(i => s"Satia$i")
    names.foreach(name => createLabeledNode(Map("name" -> name), "Person"))

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) WHERE n.name='Satia42' RETURN n.name ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet.map(map => map("n.name")) should equal(Set("Satia42"))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support contains index seek") {
    // Given
    graph.createIndex("Person", "name")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))
    val names = (1 to 91).map(i => s"Satia$i")
    names.foreach(name => createLabeledNode(Map("name" -> name), "Person"))

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) WHERE n.name CONTAINS'tia4' RETURN n.name ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet.map(map => map("n.name")) should equal(("Satia4" +: (0 to 9).map(i => s"Satia4$i")).toSet)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support composite indexes") {
    // Given
    graph.createIndex("Person", "name", "age")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))
    val names = (1 to 91).map(i => (i, s"Satia$i"))
    names.foreach {
      case (i,name) => createLabeledNode(Map("name" -> name, "age" -> i), "Person")
    }

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) WHERE n.name = 'Satia42' AND n.age = 42 RETURN n.name, n.age ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet should equal(Set(Map("n.name" -> "Satia42", "n.age" -> 42)))
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support range queries") {
    // Given
    graph.createIndex("Person", "age")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))
    val names = (1 to 91).map(i => (i, s"Satia$i"))
    names.foreach {
      case (i,name) => createLabeledNode(Map("name" -> name, "age" -> i), "Person")
    }

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) WHERE n.age < 42 RETURN n.name, n.age ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet.map(map => map("n.name")) should equal((1 to 41).map(i => s"Satia$i").toSet)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("should support label scans") {
    // Given
    val names = (1 to 91).map(i => s"Satia$i")
    names.foreach(name => createLabeledNode(Map("name" -> name), "Person"))

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (n: Person) RETURN n.name ")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet.map(map => map("n.name")) should equal(names.toSet)
    result.getExecutionPlanDescription.getArguments.get("runtime") should equal("MORSEL")
  }

  test("aggregation should not overflow morsel") {
    // Given
    graph.execute( """
                     |CREATE (zadie: AUTHOR {name: "Zadie Smith"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "White teeth"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "The Autograph Man"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "On Beauty"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "NW"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "Swing Time"})""".stripMargin)

    // When
    val result = graph.execute("CYPHER runtime=morsel MATCH (a)-[r]->(b) RETURN b.book as book, count(r), count(a)")

    // Then
    asScalaResult(result).toList should not be empty
  }

  test("should produce results non-concurrently") {
    // Given a big network
    for (i <- 1 to 10) {
      val n = createLabeledNode("N")
      for (j <- 1 to 10) {
        val m = createLabeledNode("M")
        relate(n, m, "R")
        for (k <- 1 to 10) {
          val o = createLabeledNode(Map("i" -> i, "j" -> j, "k" -> k), "O")
          relate(m, o, "P")
        }
      }
    }

    val switch = new AtomicBoolean(false)

    // When executing a query that has multiple ProduceResult tasks
    val result = graph.execute("CYPHER runtime=morsel MATCH (n:N)-[:R]->(m:M)-[:P]->(o:O) RETURN o.i, o.j, o.k")

    // Then these tasks should be executed non-concurrently
    result.accept(new ResultVisitor[Exception]() {
      override def visit(row: Result.ResultRow): Boolean = {
        if (!switch.compareAndSet(false, true)) {
          fail("Expected switch to be false: Concurrently doing ProduceResults.")
        }
        Thread.sleep(0)
        if (!switch.compareAndSet(true, false)) {
          fail("Expected switch to be true: Concurrently doing ProduceResults.")
        }
        true
      }
    })
  }

  test("should big expand query without crashing") {
    // Given a big network
    val SIZE = 30
    val ns = new ArrayBuffer[Node]()
    val ms = new ArrayBuffer[Node]()

    graph.inTx {
      for (_ <- 1 to SIZE) ns += createLabeledNode("N")
      for (_ <- 1 to SIZE) ms += createLabeledNode("M")
    }

    graph.inTx {
      for {n <- ns; m <- ms} {
        relate(n, m, "R")
      }
    }

    val result = graph.execute("CYPHER runtime=morsel MATCH (n1:N)--(m1)--(n2)--(m2) RETURN id(m2)")

    var count = 0L
    while (result.hasNext) {
      result.next()
      count += 1
    }
    count should be(756900)
  }

  test("should not duplicate results in queries with multiple eager pipelines") {
    // Given
    graph.execute( """
                     |CREATE (zadie: AUTHOR {name: "Zadie Smith"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "White teeth", rating: 5})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "The Autograph Man", rating: 3})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "On Beauty", rating: 4})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "NW"})
                     |CREATE (zadie)-[:WROTE]->(:BOOK {book: "Swing Time", rating: 5})""".stripMargin)

    // When
    val result = graph.execute("CYPHER runtime=morsel  MATCH (b:BOOK) RETURN b.book as book, count(b.rating) ORDER BY book")

    // Then
    asScalaResult(result).toList should have size 5
  }

  test("should support apply") {

    graph.createIndex("Person", "name")
    graph.inTx(graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES))

    for(i <- 0 until 100) {
      createLabeledNode(Map("name" -> "me", "secondName" -> s"me$i"), "Person")
      createLabeledNode(Map("name" -> s"me$i", "secondName" -> "you"), "Person")
    }

    val query =
      """MATCH (p:Person { name:'me' })
        |MATCH (q:Person { name: p.secondName })
        |RETURN p, q""".stripMargin

    // When
    val result = graph.execute(s"CYPHER runtime=morsel $query")

    // Then
    val resultSet = asScalaResult(result).toSet
    resultSet.size should be(100)
  }

  // Test with some interesting cases around the morsel size boundary
  Seq(MORSEL_SIZE - 1, MORSEL_SIZE, MORSEL_SIZE + 1).foreach(scanSize =>
    test(s"cartesian product scan size $scanSize") {
      //GIVEN
      for (i <- 1 to scanSize) {
        createLabeledNode(Map("prop" -> s"a$i"), "A")
        createLabeledNode(Map("prop" -> s"b$i"), "B")
        createLabeledNode(Map("prop" -> s"c$i"), "C")
      }
      val query =
        """
          |MATCH (node_a:A), (node_b:B), (node_c:C)
          |WITH node_a.prop as a, node_b.prop as b, node_c.prop as c
          |RETURN a, b, c""".stripMargin

      //WHEN
      val slottedResult = graph.execute(s"CYPHER runtime=slotted $query")
      val slottedResultList = sortedScalaListOfABTuples(slottedResult)

      val morselResult = graph.execute(s"CYPHER runtime=morsel $query")
      val morselResultList = sortedScalaListOfABTuples(morselResult)

      //THEN
      // The output order will differ between runtimes so we need to compare the sorted result
      morselResultList shouldEqual slottedResultList
    }
  )

  test(s"cartesian product lhs scan size 0") {
    //GIVEN
    for (i <- 1 to MORSEL_SIZE) {
      // Only create nodes queried on the RHS
      createLabeledNode(Map("prop" -> s"b$i"), "B")
    }

    val query =
      """MATCH (node_a:A), (node_b:B)
        |WITH node_a.prop as a, node_b.prop as b
        |RETURN a, b""".stripMargin

    //WHEN
    val morselResult = graph.execute(s"CYPHER runtime=morsel $query")
    val morselResultList = sortedScalaListOfABTuples(morselResult)

    //THEN
    morselResultList shouldBe empty
  }

  test(s"cartesian product on rhs of apply") {
    //GIVEN
    for (i <- 1 to MORSEL_SIZE) {
      // Only create nodes queried on the RHS
      createLabeledNode(Map("prop" -> s"a$i"), "A")
      createLabeledNode(Map("prop" -> s"b$i"), "B")
    }

    val query =
      """WITH 42 as i
        |MATCH (node_a:A), (node_b:B)
        |WITH node_a.prop as a, node_b.prop as b
        |RETURN a, b""".stripMargin

    //WHEN
    val slottedResult = graph.execute(s"CYPHER runtime=slotted $query")
    val slottedResultList = sortedScalaListOfABTuples(slottedResult)

    val morselResult = graph.execute(s"CYPHER runtime=morsel $query")
    val morselResultList = sortedScalaListOfABTuples(morselResult)

    //THEN
    // The output order will differ between runtimes so we need to compare the sorted result
    morselResultList shouldEqual slottedResultList
  }

  test("allnodesscan should find all nodes") {
    val ids = (1 to 10 map(_ => createNode().getId)).toSet

    val query = "CYPHER runtime=morsel MATCH (n) RETURN id(n)"
    graph.execute(query).asScala.map(_.get("id(n)").asInstanceOf[Long]).toSet should equal(ids)
  }

  test("don't stall for nested plan expressions") {
    // Given
    graph.execute( """CREATE (a:A)
                     |CREATE (a)-[:T]->(:B),
                     |       (a)-[:T]->(:C)""".stripMargin)


    // When
    val result =
      graph.execute( """ CYPHER runtime=morsel
                       | MATCH (n)
                       | RETURN CASE
                       |          WHEN id(n) >= 0 THEN (n)-->()
                       |          ELSE 42
                       |        END AS p""".stripMargin)

    // Then
    asScalaResult(result).toList should not be empty
  }

  private def sortedScalaListOfABTuples(result: Result): Seq[(String, String)] = {
    result.map {
      new java.util.function.Function[java.util.Map[String, AnyRef], (String, String)] {
        override def apply(m: java.util.Map[String, AnyRef]): (String, String) = {
          val a = m.get("a")
          val b = m.get("b")
          a.asInstanceOf[String] -> b.asInstanceOf[String]
        }
      }
    }.asScala.toList.sorted
  }
}

class ParallelSimpleSchedulerMorselRuntimeAcceptanceTest extends MorselRuntimeAcceptanceTest {
  //we use a ridiculously small morsel size in order to trigger as many morsel overflows as possible
  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.cypher_hints_error -> "true",
    GraphDatabaseSettings.cypher_morsel_size -> MORSEL_SIZE.toString,
    GraphDatabaseSettings.cypher_worker_count -> "0"
  )
}

class ParallelLockFreeSchedulerMorselRuntimeAcceptanceTest extends MorselRuntimeAcceptanceTest {
  //we use a ridiculously small morsel size in order to trigger as many morsel overflows as possible
  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.cypher_hints_error -> "true",
    GraphDatabaseSettings.cypher_morsel_size -> MORSEL_SIZE.toString,
    GraphDatabaseSettings.cypher_worker_count -> "0",
    GraphDatabaseSettings.cypher_morsel_runtime_scheduler -> "lock_free"
  )
}

class SequentialMorselRuntimeAcceptanceTest extends MorselRuntimeAcceptanceTest {
  //we use a ridiculously small morsel size in order to trigger as many morsel overflows as possible
  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.cypher_hints_error -> "true",
    GraphDatabaseSettings.cypher_morsel_size -> MORSEL_SIZE.toString,
    GraphDatabaseSettings.cypher_worker_count -> "1"
  )
}

/**
  * Use this configuration to test the edge case of morsel_size = 1.
  */
class MorselSizeOneAcceptanceTest extends MorselRuntimeAcceptanceTest {
  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.cypher_hints_error -> "false",
    GraphDatabaseSettings.cypher_morsel_size -> "1",
    GraphDatabaseSettings.cypher_worker_count -> "0"
  )
}

/**
  * These tests can only be run with cypher_hints_error = false
  * since they test with queries that are not supported.
  */
class MorselRuntimeNotSupportedAcceptanceTest extends ExecutionEngineFunSuite {
  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.cypher_hints_error -> "false",
    GraphDatabaseSettings.cypher_morsel_size -> MORSEL_SIZE.toString,
    GraphDatabaseSettings.cypher_worker_count -> "0"
  )

  test("should fallback if morsel doesn't support query") {
    //Given
    val result = graph.execute("CYPHER runtime=morsel MATCH (n)-[*]->(m) RETURN n SKIP 1")

    // When (exhaust result)
    result.resultAsString()

    //Then
    result.getExecutionPlanDescription.getArguments.get("runtime") should not equal "MORSEL"
  }
}
