/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class AggregationWithValuesAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  override def beforeEach(): Unit = {
    super.beforeEach()
    createSomeNodes()
    graph.createIndex("Awesome", "prop1")
    graph.createIndex("Awesome", "prop2")
    graph.createIndex("Awesome", "prop1", "prop2")
    graph.createIndex("Awesome", "prop3")
  }

  // Invoked once before the Tx and once in the same Tx
  def createSomeNodes(): Unit = {
    graph.execute(
      """
      CREATE (:Awesome {prop1: 40, prop2: 4})-[:R]->()
      CREATE (:Awesome {prop1: 41, prop2: 1})-[:R]->()
      CREATE (:Awesome {prop1: 42, prop2: 4})-[:R]->()
      CREATE (:Awesome {prop3: 'abc'})-[:R]->()
      CREATE (:Awesome {prop3: 'cc'})-[:R]->()
      """)
  }

  for ((i, func, funcBody, property, supportsCompiled) <-
         List(
           (0, "min", "n.prop3", "n.prop3", false),
           (1, "max", "n.prop3", "n.prop3", false),
           (2, "sum", "n.prop2", "n.prop2", false),
           (3, "avg", "n.prop2", "n.prop2", false),
           (4, "stDev", "n.prop2", "n.prop2", false),
           (5, "stDevP", "n.prop2", "n.prop2", false),
           (6, "percentileDisc", "n.prop1, 0.25", "n.prop1", false),
           (7, "percentileCont", "n.prop1, 0.75", "n.prop1", false),
           (8, "count", "n.prop1", "n.prop1", true),
           (9, "count", "DISTINCT n.prop2", "n.prop2", true)
         )
  ) {
    // Simple aggregation functions implicitly gives exists

    test(s"$i-$func: should use index provided values") {
      //Compiled does not support NodeIndexScan any more
      val config = if (supportsCompiled) Configs.InterpretedAndSlotted + Configs.Version3_4 else Configs.InterpretedAndSlotted
      val query = s"MATCH (n:Awesome) RETURN $func($funcBody) as aggregation"
      val result = executeWith(config, query, executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n", s"cached[$property]")

      val expected = List("abc", "cc", 18, 3.0, 1.5491933384829668, 1.4142135623730951, 40, 41.75, 6, 2)

      result.toList should equal(List(Map("aggregation" -> expected(i))))
    }

    // Combination of aggregation and grouping expression does not implicitly give exist
    test(s"$i-$func: cannot use index provided values with grouping expression without exists") {
      val config = if (supportsCompiled) Configs.All else Configs.InterpretedAndSlotted
      val query = s"MATCH (n:Awesome) RETURN $property as res1, $func($funcBody) as res2"
      val result = executeWith(config, query, executeBefore = createSomeNodes)

      result.executionPlanDescription() should includeSomewhere.aPlan("NodeByLabelScan")

      val expected = List(
        Set(Map("res1" -> "cc", "res2" -> "cc"), Map("res1" -> "abc", "res2" -> "abc"), Map("res1" -> null, "res2" -> null)),
        Set(Map("res1" -> "cc", "res2" -> "cc"), Map("res1" -> "abc", "res2" -> "abc"), Map("res1" -> null, "res2" -> null)),
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
      val config = Configs.InterpretedAndSlotted
      val query = s"MATCH (n:Awesome) WHERE exists($property) RETURN $property as res1, $func($funcBody) as res2"
      val result = executeWith(config, query, executeBefore = createSomeNodes)

      result.executionPlanDescription() should
        includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n", s"cached[$property]")

      val expected = List(
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
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n", s"cached[n.prop3]")

    result.toList should equal(List(Map("res" -> List("abc", "cc"))))
  }

  test("should use index provided values with renamed property") {
    val query = "MATCH (n: Awesome) WITH n.prop1 AS property RETURN min(property)"
    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n", "cached[n.prop1]")

    result.toList should equal(List(Map("min(property)" -> 40)))
  }

  test("should use index provided values after renaming multiple properties") {
    val query = "MATCH (n: Awesome) WITH n.prop1 AS property, n.prop2 AS prop RETURN min(property)"
    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n", "cached[n.prop1]")

    result.toList should equal(List(Map("min(property)" -> 40)))
  }

  test("should use index provided values after multiple renamings") {
    val query =
      """
        | MATCH (n: Awesome)
        | WITH n.prop1 AS property
        | WITH property AS prop
        | RETURN min(prop)
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n", "cached[n.prop1]")

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

    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n", "cached[n.prop1]")

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

    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n", "cached[n.prop1]")

    result.toList should equal(List(Map("min(property)" -> 40)))
  }

  test("should handle aggregation with node projection without renaming") {
    val query = "MATCH (n: Awesome) WITH n LIMIT 1 RETURN count(n)"
    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan").withExactVariables("n")

    result.toList should equal(List(Map("count(n)" -> 1)))
  }

  test("should use index provided values for multiple aggregations on same property") {
    val query = "MATCH (n: Awesome) RETURN min(n.prop1) AS min, max(n.prop1) AS max, avg(n.prop1) AS avg"
    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n", s"cached[n.prop1]")

    result.toList should equal(List(Map("min" -> 40, "max" -> 42, "avg" -> 41)))
  }

  test("cannot use index provided values for multiple aggregations on different properties") {
    val query = "MATCH (n: Awesome) RETURN max(n.prop2) AS max, avg(n.prop1) AS avg"
    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    /*
     * TODO: this could be supported when we have composite index range scans
     * result.executionPlanDescription() should
     *    includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n", "cached[n.prop1, n.prop2]") ?
     */

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan").withExactVariables("n")

    result.toList should equal(List(Map("max" -> 4, "avg" -> 41)))
  }

  test("should use index provided values for single aggregation after cartesian product") {

    createLabeledNode(Map("prop1" -> 1, "prop2" -> 4), "Label")
    createLabeledNode(Map("prop1" -> 2), "Label")
    graph.createIndex("Label", "prop2")

    val query = "MATCH (n:Awesome), (m:Label) RETURN count(m.prop2) AS count"
    val result = executeWith(Configs.InterpretedAndSlotted + Configs.Version3_4, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("CartesianProduct")
        .withLHS(aPlan("NodeIndexScan").withExactVariables("m", s"cached[m.prop2]"))
        .withRHS(aPlan("NodeByLabelScan").withExactVariables("n"))

    //count = 10 because of two calls to createSomeNodes
    result.toList should equal(List(Map("count" -> 10)))
  }

  test("cannot use index provided values for multiple aggregations on different variables after cartesian product") {

    createLabeledNode(Map("prop1" -> 1, "prop2" -> 4), "Label")
    createLabeledNode(Map("prop1" -> 2), "Label")

    val query = "MATCH (n:Awesome), (m:Label) RETURN min(n.prop1) AS min, count(m.prop1) AS count"
    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("CartesianProduct")
        .withLHS(aPlan("NodeByLabelScan").withExactVariables("m"))
        .withRHS(aPlan("NodeByLabelScan").withExactVariables("n"))

    //count = 20 because of two calls to createSomeNodes and cartesian product
    result.toList should equal(List(Map("min" -> 40, "count" -> 20)))
  }

  test("cannot use index provided values for multiple aggregations on different variables with index on both after cartesian product") {

    createLabeledNode(Map("prop1" -> 1, "prop2" -> 4), "Label")
    createLabeledNode(Map("prop1" -> 2), "Label")
    graph.createIndex("Label", "prop1")

    val query = "MATCH (n:Awesome), (m:Label) RETURN min(n.prop1) AS min, count(m.prop1) AS count"
    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("CartesianProduct")
        .withLHS(aPlan("NodeByLabelScan").withExactVariables("m"))
        .withRHS(aPlan("NodeByLabelScan").withExactVariables("n"))

    //count = 20 because of two calls to createSomeNodes and cartesian product
    result.toList should equal(List(Map("min" -> 40, "count" -> 20)))
  }

  test("should use index provided values for single aggregation with multiple variables") {

    val n = createLabeledNode(Map("prop1" -> 1, "prop2" -> 5), "LabelN")
    val m1 = createLabeledNode(Map("prop1" -> 2, "prop2" -> 4), "Label")
    val m2 = createLabeledNode(Map("prop1" -> 3, "prop2" -> 3), "Label")
    val m3 = createLabeledNode(Map("prop1" -> 4, "prop2" -> 2), "Label")
    val m4 = createLabeledNode(Map("prop1" -> 5, "prop2" -> 1), "Label")
    relate(n,m1)
    relate(n,m2)
    relate(n,m3)
    relate(n,m4)
    graph.createIndex("LabelN", "prop2")

    val query = "MATCH (n:LabelN)-[]->(m:Label) RETURN count(n.prop2) AS count"
    val result = executeWith(Configs.InterpretedAndSlotted + Configs.Version3_4, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeIndexScan").withExactVariables("n", s"cached[n.prop2]")

    result.toList should equal(List(Map("count" -> 4)))
  }

  test("cannot use index provided values for multiple aggregations on different variables") {

    val n = createLabeledNode(Map("prop1" -> 1, "prop2" -> 5), "LabelN")
    val m1 = createLabeledNode(Map("prop1" -> 2, "prop2" -> 4), "Label")
    val m2 = createLabeledNode(Map("prop1" -> 3, "prop2" -> 3), "Label")
    val m3 = createLabeledNode(Map("prop1" -> 4, "prop2" -> 2), "Label")
    val m4 = createLabeledNode(Map("prop1" -> 5, "prop2" -> 1), "Label")
    relate(n,m1)
    relate(n,m2)
    relate(n,m3)
    relate(n,m4)
    graph.createIndex("LabelN", "prop2")

    val query = "MATCH (n:LabelN)-[]->(m:Label) RETURN count(n.prop2) AS count, avg(m.prop1) AS avg"
    val result = executeWith(Configs.InterpretedAndSlotted, query, executeBefore = createSomeNodes)

    result.executionPlanDescription() should
      includeSomewhere.aPlan("NodeByLabelScan").withExactVariables("n")

    result.toList should equal(List(Map("count" -> 4, "avg" -> 3.5)))
  }

}
