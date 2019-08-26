/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.internal.kernel.api.procs.Neo4jTypes

class AggregationFunctionCallSupportAcceptanceTest extends ProcedureCallAcceptanceTest {

  test("should return correctly typed map result (even if converting to and from scala representation internally)") {
    val value = new util.HashMap[String, Any]()
    value.put("name", "Cypher")
    value.put("level", 9001)

    registerUserAggregationFunction(value)

    // Using graph execute to get a Java value
    graph.inTx(graph.execute("RETURN my.first.value()").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("my.first.value()", value)
    )))
  }

  test("should return correctly typed list result (even if converting to and from scala representation internally)") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")

    registerUserAggregationFunction(value)

    // Using graph execute to get a Java value
    graph.inTx(graph.execute("RETURN my.first.value() AS out").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("out", value)
    )))
  }

  test("should return correctly typed stream result (even if converting to and from scala representation internally)") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")
    val stream = value.stream()

    registerUserAggregationFunction(stream)

    // Using graph execute to get a Java value
    graph.inTx(graph.execute("RETURN my.first.value() AS out").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("out", value)
    )))
  }

  test("should not copy lists unnecessarily") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")

    registerUserAggregationFunction(value)

    // Using graph execute to get a Java value
    val returned = graph.inTx(graph.execute("RETURN my.first.value() AS out").next().get("out"))

    returned shouldBe an [util.ArrayList[_]]
    returned shouldBe value
  }

  test("should not copy unnecessarily with nested types") {
    val value = new util.ArrayList[Any]()
    val inner = new util.ArrayList[Any]()
    value.add("Norris")
    value.add(inner)

    registerUserAggregationFunction(value)

    // Using graph execute to get a Java value
    val returned = graph.inTx(graph.execute("RETURN my.first.value() AS out").next().get("out"))

    returned shouldBe an [util.ArrayList[_]]
    returned shouldBe value
  }

  test("should handle interacting with list") {
    val value = new util.ArrayList[Integer]()
    value.add(1)
    value.add(3)

    registerUserAggregationFunction(value, Neo4jTypes.NTList(Neo4jTypes.NTInteger))

    // Using graph execute to get a Java value
    val returned = graph.inTx(graph.execute("WITH my.first.value() AS list RETURN list[0] + list[1] AS out")
      .next().get("out"))

    returned should equal(4)
  }
}
