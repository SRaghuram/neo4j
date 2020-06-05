/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.impl.coreapi.InternalTransaction

import scala.collection.immutable

class AggregationWithValuesAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport
                                          with CypherComparisonSupport with CreateTempFileTestSupport {

  override def beforeEach(): Unit = {
    super.beforeEach()
    graph.withTx( tx => createSomeNodes(tx))
    graph.createIndex("Awesome", "prop1")
    graph.createIndex("Awesome", "prop2")
    graph.createIndex("Awesome", "prop3")
  }

  // Invoked once before the Tx and once in the same Tx
  def createSomeNodes(tx: InternalTransaction): Unit = {
    tx.execute(
      """
      CREATE (:Awesome {prop1: 40, prop2: 4})-[:R]->()
      CREATE (:Awesome {prop1: 41, prop2: 1})-[:R]->()
      CREATE (:Awesome {prop1: 42, prop2: 4})-[:R]->()
      CREATE (:Awesome {prop3: 'abc'})-[:R]->()
      CREATE (:Awesome {prop3: 'cc'})-[:R]->()
      """)
  }

  for ((i, func, funcBody, property, supportsCompiled, supportsPipelined) <-
         List(
           (0, "min", "n.prop3", "n.prop3", false, true),
           (1, "max", "n.prop3", "n.prop3", false, true),
           (2, "sum", "n.prop2", "n.prop2", false, true),
           (3, "avg", "n.prop2", "n.prop2", false, true),
           (4, "stDev", "n.prop2", "n.prop2", false, true),
           (5, "stDevP", "n.prop2", "n.prop2", false, true),
           (6, "percentileDisc", "n.prop1, 0.25", "n.prop1", false, false),
           (7, "percentileCont", "n.prop1, 0.75", "n.prop1", false, false),
           (8, "count", "n.prop1", "n.prop1", true, true),
           (9, "count", "DISTINCT n.prop2", "n.prop2", true, true)
         )
       ) {
    // Simple aggregation functions implicitly gives exists

    test(s"$i-$func: should use index provided values") {
      val config =
        if (supportsPipelined) Configs.CachedProperty
        else Configs.InterpretedAndSlotted

      val query = s"MATCH (n:Awesome) RETURN $func($funcBody) as aggregation"
      val result = executeWith(config, query, executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentRegex(s".*cache\\[$property\\]".r)

      val expected: immutable.Seq[Any] = List("abc", "cc", 18, 3.0, 1.5491933384829668, 1.4142135623730951, 40, 41.75, 6, 2)

      result.toList should equal(List(Map("aggregation" -> expected(i))))
    }

    // Combination of aggregation and grouping expression does not implicitly give exist
    test(s"$i-$func: cannot use index provided values with grouping expression without exists") {
      val config =
        if (supportsCompiled && supportsPipelined) Configs.CachedProperty + Configs.Compiled
        else if (supportsCompiled) Configs.All - Configs.Pipelined
        else if (supportsPipelined) Configs.CachedProperty
        else Configs.InterpretedAndSlotted

      val query = s"MATCH (n:Awesome) RETURN $property as res1, $func($funcBody) as res2"
      val result = executeWith(config, query, executeBefore = createSomeNodes)

      result.executionPlanDescription() should includeSomewhere.aPlan("NodeByLabelScan")

      val expected: List[Set[Map[String, Any]]] = List(
        Set(Map("res1" -> "cc", "res2" -> "cc"), Map("res1" -> "abc", "res2" -> "abc"), Map("res1" -> null, "res2" -> null)),
        Set[Map[String, Any]](Map("res1" -> "cc", "res2" -> "cc"), Map("res1" -> "abc", "res2" -> "abc"), Map("res1" -> null, "res2" -> null)),
        Set(Map("res1" -> 1, "res2" -> 2), Map("res1" -> 4, "res2" -> 16), Map("res1" -> null, "res2" -> 0)),
        Set(Map("res1" -> 1, "res2" -> 1.0), Map("res1" -> 4, "res2" -> 4.0), Map("res1" -> null, "res2" -> null)),
        Set(Map("res1" -> 1, "res2" -> 0.0), Map("res1" -> 4, "res2" -> 0.0), Map("res1" -> null, "res2" -> 0.0)),
        Set(Map("res1" -> 1, "res2" -> 0.0), Map("res1" -> 4, "res2" -> 0.0), Map("res1" -> null, "res2" -> 0.0)),
        Set(Map("res1" -> 40, "res2" -> 40), Map("res1" -> 41, "res2" -> 41), Map("res1" -> 42, "res2" -> 42), Map("res1" -> null, "res2" -> null)),
        Set(Map("res1" -> 40, "res2" -> 40), Map("res1" -> 41, "res2" -> 41), Map("res1" -> 42, "res2" -> 42), Map("res1" -> null, "res2" -> null)),
        Set(Map("res1" -> 40, "res2" -> 2), Map("res1" -> 41, "res2" -> 2), Map("res1" -> 42, "res2" -> 2), Map("res1" -> null, "res2" -> 0)),
        Set(Map("res1" -> 1, "res2" -> 1), Map("res1" -> 4, "res2" -> 1), Map("res1" -> null, "res2" -> 0))
      )

      result.toSet should equal(expected(i))
    }

    test(s"$i-$func: should use index provided values with grouping expression with exists") {
      //Compiled does not support exists
      val config = if(supportsPipelined) Configs.CachedProperty else Configs.InterpretedAndSlotted
      val query = s"MATCH (n:Awesome) WHERE exists($property) RETURN $property as res1, $func($funcBody) as res2"
      val result = executeWith(config, query, executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentRegex(s".*cache\\[$property\\]".r)

      val expected: List[Set[Map[String, Any]]] = List(
        Set(Map("res1" -> "cc", "res2" -> "cc"), Map("res1" -> "abc", "res2" -> "abc")),
        Set(Map("res1" -> "cc", "res2" -> "cc"), Map("res1" -> "abc", "res2" -> "abc")),
        Set(Map("res1" -> 1, "res2" -> 2), Map("res1" -> 4, "res2" -> 16)),
        Set(Map("res1" -> 1, "res2" -> 1.0), Map("res1" -> 4, "res2" -> 4.0)),
        Set(Map("res1" -> 1, "res2" -> 0.0), Map("res1" -> 4, "res2" -> 0.0)),
        Set(Map("res1" -> 1, "res2" -> 0.0), Map("res1" -> 4, "res2" -> 0.0)),
        Set(Map("res1" -> 40, "res2" -> 40), Map("res1" -> 41, "res2" -> 41), Map("res1" -> 42, "res2" -> 42)),
        Set(Map("res1" -> 40, "res2" -> 40), Map("res1" -> 41, "res2" -> 41), Map("res1" -> 42, "res2" -> 42)),
        Set(Map("res1" -> 40, "res2" -> 2), Map("res1" -> 41, "res2" -> 2), Map("res1" -> 42, "res2" -> 2)),
        Set(Map("res1" -> 1, "res2" -> 1), Map("res1" -> 4, "res2" -> 1))
      )

      result.toSet should equal(expected(i))
    }
  }

  test("collect: should use index provided values") {
    // executeSingle since different versions of Neo4j can give back the collected list in different orders
    val result = executeSingle("MATCH (n:Awesome) RETURN collect(n.prop3) as res")

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop3")

    result.toList should equal(List(Map("res" -> List("abc", "cc"))))
  }

  test("should use index provided values with renamed property") {
    val query = "MATCH (n: Awesome) WITH n.prop1 AS property RETURN min(property)"
    val result = executeWith(Configs.CachedProperty, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1")

    result.toList should equal(List(Map("min(property)" -> 40)))
  }

  test("should use index provided values after renaming multiple properties") {
    val query = "MATCH (n: Awesome) WITH n.prop1 AS property, n.prop2 AS prop RETURN min(property)"
    val result = executeWith(Configs.CachedProperty, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1")

    result.toList should equal(List(Map("min(property)" -> 40)))
  }

  test("should use index provided values after multiple renamings of property") {
    val query =
      """
        | MATCH (n: Awesome)
        | WITH n.prop1 AS property
        | WITH property AS prop
        | RETURN min(prop)
      """.stripMargin

    val result = executeWith(Configs.CachedProperty, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1")

    result.toList should equal(List(Map("min(prop)" -> 40)))
  }

  test("should use index provided values with renamed variable") {
    val query = "MATCH (n:Awesome) WITH n as m RETURN min(m.prop1)"
    val result = executeWith(Configs.CachedProperty, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1")

    result.toList should equal(List(Map("min(m.prop1)" -> 40)))
  }

  test("should use index provided values with renamed variable and property") {
    val query = "MATCH (n:Awesome) WITH n as m WITH m.prop1 AS prop RETURN min(prop)"
    val result = executeWith(Configs.CachedProperty, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1")

    result.toList should equal(List(Map("min(prop)" -> 40)))
  }

  test("should use index provided values after multiple renamings with reused names") {
    val query =
      """
        | MATCH (n: Awesome)
        | WITH n.prop1 AS property
        | WITH property AS prop, 1 as property
        | RETURN min(prop)
      """.stripMargin

    val result = executeWith(Configs.CachedProperty, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1")

    result.toList should equal(List(Map("min(prop)" -> 40)))
  }

  test("should use index provided values after multiple renamings with reused names 2") {
    val query =
      """
        | MATCH (n: Awesome)
        | WITH n.prop1 AS prop, 1 AS property
        | WITH prop AS property
        | RETURN min(property)
      """.stripMargin

    val result = executeWith(Configs.CachedProperty, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1")

    result.toList should equal(List(Map("min(property)" -> 40)))
  }

  test("should handle aggregation with node projection without renaming") {
    val query = "MATCH (n: Awesome) WITH n LIMIT 1 RETURN count(n)"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan").withExactVariables("n")

    result.toList should equal(List(Map("count(n)" -> 1)))
  }

  test("should use index provided values for multiple aggregations on same property") {
    val query = "MATCH (n: Awesome) RETURN min(n.prop1) AS min, max(n.prop1) AS max, avg(n.prop1) AS avg"
    val result = executeWith(Configs.CachedProperty, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1")

    result.toList should equal(List(Map("min" -> 40, "max" -> 42, "avg" -> 41)))
  }

  test("cannot use composite index for aggregation") {
    graph.createIndex("Label", "prop1", "prop2")
    createLabeledNode(Map("prop1" -> 123, "prop2" -> "abc"), "Label")
    createLabeledNode(Map("prop1" -> 12), "Label")
    resampleIndexes()

    val query = "MATCH (n: Label) RETURN min(n.prop1)"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, executeBefore = createSomeNodes)

    /*
     * Even if range scans for composite indexes become supported,
     * we cannot utilize composite indexes for aggregations because of
     * the case when some nodes only have a subset of the properties in the index
     */
    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan").withExactVariables("n")

    result.toList should equal(List(Map("min(n.prop1)" -> 12)))
  }

  test("cannot use index provided values for multiple aggregations on different properties") {

    graph.createIndex("Awesome", "prop3", "prop4")
    createLabeledNode(Map("prop3" -> 123, "prop4" -> "abc"), "Awesome")
    resampleIndexes()

    val query = "MATCH (n: Awesome) RETURN count(n.prop3), count(n.prop4)"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, executeBefore = createSomeNodes)

    /*
     * Even if range scans for composite indexes become supported,
     * we cannot utilize composite indexes for aggregations because of
     * the case when some nodes only have a subset of the properties in the index
     */
    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan").withExactVariables("n")

    result.toSet should equal(Set(Map("count(n.prop3)" -> 5, "count(n.prop4)" -> 1)))
  }

  test("should use index provided values for single aggregation after cartesian product") {

    createLabeledNode(Map("prop1" -> 1, "prop2" -> 4), "Label")
    createLabeledNode(Map("prop1" -> 2), "Label")
    graph.createIndex("Label", "prop2")

    val query = "MATCH (n:Awesome), (m:Label) RETURN count(m.prop2) AS count"
    val result = executeWith(Configs.CartesianProduct - Configs.Compiled, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("CartesianProduct")
        .withChildren(
          aPlan("NodeIndexScan").withExactVariables("m").containingArgumentForCachedProperty("m", "prop2"),
          aPlan("NodeByLabelScan").withExactVariables("n"))

    //count = 10 because of two calls to createSomeNodes
    result.toList should equal(List(Map("count" -> 10)))
  }

  test("cannot use index provided values for multiple aggregations on different variables after cartesian product") {

    createLabeledNode(Map("prop1" -> 1, "prop2" -> 4), "Label")
    createLabeledNode(Map("prop1" -> 2), "Label")

    val query = "MATCH (n:Awesome), (m:Label) RETURN min(n.prop1) AS min, count(m.prop1) AS count"
    val result = executeWith(Configs.CartesianProduct - Configs.Compiled, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("CartesianProduct")
        .withChildren(
          aPlan("CacheProperties").onTopOf(aPlan("NodeByLabelScan").withExactVariables("m")),
          aPlan("CacheProperties").onTopOf(aPlan("NodeByLabelScan").withExactVariables("n")))

    //count = 20 because of two calls to createSomeNodes and cartesian product
    result.toList should equal(List(Map("min" -> 40, "count" -> 20)))
  }

  test("cannot use index provided values for multiple aggregations on different variables with index on both after cartesian product") {

    createLabeledNode(Map("prop1" -> 1, "prop2" -> 4), "Label")
    createLabeledNode(Map("prop1" -> 2), "Label")
    graph.createIndex("Label", "prop1")

    val query = "MATCH (n:Awesome), (m:Label) RETURN min(n.prop1) AS min, count(m.prop1) AS count"
    val result = executeWith(Configs.CartesianProduct - Configs.Compiled, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("CartesianProduct")
        .withChildren(
          aPlan("CacheProperties").onTopOf(aPlan("NodeByLabelScan").withExactVariables("m")),
          aPlan("CacheProperties").onTopOf(aPlan("NodeByLabelScan").withExactVariables("n")))

    //count = 20 because of two calls to createSomeNodes and cartesian product
    result.toList should equal(List(Map("min" -> 40, "count" -> 20)))
  }

  test("should use index provided values for single aggregation with multiple variables") {

    val n = createLabeledNode(Map("prop1" -> 1, "prop2" -> 5), "LabelN")
    val m1 = createLabeledNode(Map("prop1" -> 2, "prop2" -> 4), "Label")
    val m2 = createLabeledNode(Map("prop1" -> 3, "prop2" -> 3), "Label")
    val m3 = createLabeledNode(Map("prop1" -> 4, "prop2" -> 2), "Label")
    val m4 = createLabeledNode(Map("prop1" -> 5, "prop2" -> 1), "Label")
    (0 until 20).foreach(_ => createLabeledNode("Label"))
    relate(n, m1)
    relate(n, m2)
    relate(n, m3)
    relate(n, m4)
    graph.createIndex("LabelN", "prop2")

    val query = "MATCH (n:LabelN)-[]->(m:Label) RETURN count(n.prop2) AS count"
    val result = executeWith(Configs.CachedProperty, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop2")

    result.toList should equal(List(Map("count" -> 4)))
  }

  test("cannot use index provided values for multiple aggregations on different variables") {

    val n = createLabeledNode(Map("prop1" -> 1, "prop2" -> 5), "LabelN")
    val m1 = createLabeledNode(Map("prop1" -> 2, "prop2" -> 4), "Label")
    val m2 = createLabeledNode(Map("prop1" -> 3, "prop2" -> 3), "Label")
    val m3 = createLabeledNode(Map("prop1" -> 4, "prop2" -> 2), "Label")
    val m4 = createLabeledNode(Map("prop1" -> 5, "prop2" -> 1), "Label")
    relate(n, m1)
    relate(n, m2)
    relate(n, m3)
    relate(n, m4)
    graph.createIndex("LabelN", "prop2")

    val query = "MATCH (n:LabelN)-[]->(m:Label) RETURN count(n.prop2) AS count, avg(m.prop1) AS avg"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan").withExactVariables("n")

    result.toList should equal(List(Map("count" -> 4, "avg" -> 3.5)))
  }

  test("cannot use index provided values when CREATE before aggregation") {
    val query = "MATCH (n:Awesome) CREATE (:New {prop:n.prop1}) RETURN avg(n.prop1)"
    val result1 = executeWith(Configs.Create, query, executeBefore = createSomeNodes)

    // With NodeIndexScan only 6 new nodes will be created, but it should be 10 new nodes
    result1.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan")
    result1.toList should equal(List(Map("avg(n.prop1)" -> 41)))

    val result2 = executeWith(Configs.FromCountStore, "MATCH (n:New) RETURN count(n)")
    result2.toList should equal(List(Map("count(n)" -> 10)))
  }

  test("cannot use index provided values when CREATE between renaming and aggregation") {
    val query = "MATCH (n:Awesome) WITH n.prop1 AS prop CREATE (:New {prop:prop}) RETURN avg(prop)"
    val result1 = executeWith(Configs.Create, query, executeBefore = createSomeNodes)

    //With NodeIndexScan only 6 new nodes will be created, but it should be 10 new nodes
    result1.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan")
    result1.toList should equal(List(Map("avg(prop)" -> 41)))
    result1.toList should equal(List(Map("avg(prop)" -> 41)))

    val result2 = executeWith(Configs.FromCountStore, "MATCH (n:New) RETURN count(n)")
    result2.toList should equal(List(Map("count(n)" -> 10)))
  }

  test("cannot use index provided values when CREATE before renaming") {
    val query = "MATCH (n:Awesome) CREATE (:New {prop:n.prop1}) WITH n.prop1 AS prop RETURN avg(prop)"
    val result1 = executeWith(Configs.Create, query, executeBefore = createSomeNodes)

    //With NodeIndexScan only 6 new nodes will be created, but it should be 10 new nodes
    result1.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan")
    result1.toList should equal(List(Map("avg(prop)" -> 41)))

    val result2 = executeWith(Configs.FromCountStore, "MATCH (n:New) RETURN count(n)")
    result2.toList should equal(List(Map("count(n)" -> 10)))
  }

  test("should use index provided values when CREATE after aggregation") {
    val query = "MATCH (n:Awesome) WITH avg(n.prop1) AS avg CREATE (:New) RETURN avg"
    val result1 = executeWith(Configs.Create, query, executeBefore = createSomeNodes)

    result1.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n").containingArgumentForCachedProperty("n", "prop1")
    result1.toList should equal(List(Map("avg" -> 41)))

    val result2 = executeWith(Configs.FromCountStore, "MATCH (n:New) RETURN count(n)")
    result2.toList should equal(List(Map("count(n)" -> 1)))
  }

  test("should use index provided values for DISTINCT before aggregation") {
    val query = "MATCH (n: Awesome) WITH DISTINCT n.prop2 as prop RETURN count(prop)"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").containingArgumentForCachedProperty("n", "prop2")

    result.toList should equal(List(Map("count(prop)" -> 2)))
  }

  test("should use index provided values when UNWIND before aggregation") {
    val query = "UNWIND [1,2,3] AS i MATCH (n: Awesome) RETURN count(n.prop1)"
    val result = executeWith(Configs.CachedProperty, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").containingArgumentForCachedProperty("n", "prop1")

    // 3 times 6 nodes
    result.toList should equal(List(Map("count(n.prop1)" -> 18)))
  }

  test("should use index provided values when complex UNWIND before aggregation") {
    val query = "MATCH (n: Awesome) UNWIND labels(n) + [n.prop1, n.prop2, 1, 'abc'] AS i RETURN count(n.prop1)"
    val result = executeWith(Configs.CachedProperty, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").containingArgumentForCachedProperty("n", "prop1")

    // 5 times 6 nodes
    result.toList should equal(List(Map("count(n.prop1)" -> 30)))
  }

  test("cannot use index provided values when aggregating over variable from UNWIND") {
    val query = "MATCH (n: Awesome) UNWIND [n.prop1] AS i RETURN count(i)"
    val result = executeWith(Configs.All, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan")

    result.toList should equal(List(Map("count(i)" -> 6)))
  }

  test("should use index provided values when LOAD CSV before aggregation") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("Value")
        writer.println("5")
        writer.println("34")
        writer.println("1337")
    })

    val query = s"LOAD CSV WITH HEADERS FROM '$url' AS row MATCH (n:Awesome) WHERE toInteger(row.Value) > 20 RETURN count(n.prop1)"
    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").containingArgumentForCachedProperty("n", "prop1")

    // 2 times 6 nodes
    result.toList should equal(List(Map("count(n.prop1)" -> 12)))
  }

  test("should use index provided values when procedure call before aggregation") {
    val query = "MATCH (n:Awesome) CALL db.labels() YIELD label RETURN count(n.prop1)"
    val result = executeWith(Configs.ProcedureCallRead, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").containingArgumentForCachedProperty("n", "prop1")

    result.toList should equal(List(Map("count(n.prop1)" -> 6)))
  }

  test("should handle aggregation with label not in the semantic table") {
    val query = "MATCH (n: NotExistingLabel) RETURN min(n.prop1)"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan").withExactVariables("n")

    result.toList should equal(List(Map("min(n.prop1)" -> null)))
  }
}
