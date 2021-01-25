/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.internal.helpers.collection.MapUtil.map
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.impl.util.ValueUtils

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter

class FunctionCallSupportAcceptanceTest extends ProcedureCallAcceptanceTest {

  test("should return correctly typed map result (even if converting to and from scala representation internally)") {
    val value = new util.HashMap [String, Any]()
    value.put("name", "Cypher")
    value.put("level", 9001)

    registerUserFunction(ValueUtils.of(value))

    // Using graph execute to get a Java value
    graph.withTx( tx => tx.execute("RETURN my.first.value()").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("my.first.value()", value)
    )))
  }

  test("should not fail to type check this") { // Waiting for the next release of openCypher
    graph.getDependencyResolver.resolveDependency(classOf[GlobalProcedures]).registerFunction(classOf[TestFunction])

    // We just want to make sure that running the query does not throw exceptions
    graph.withTx( tx =>
      tx.execute("return round(0.4 * test.sum(collect(toInteger('12'))) / 12)")
        .stream().toArray.length should equal(1))
  }

  test("should return correctly typed list result (even if converting to and from scala representation internally)") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")

    registerUserFunction(ValueUtils.of(value))

    // Using graph execute to get a Java value
    graph.withTx( tx => tx.execute("RETURN my.first.value() AS out").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("out", value)
    )))
  }

  test("should return correctly typed stream result (even if converting to and from scala representation internally)") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")
    value.stream()

    registerUserFunction(ValueUtils.of(value))

    // Using graph execute to get a Java value
    graph.withTx( tx => tx.execute("RETURN my.first.value() AS out").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("out", value)
    )))
  }

  test("should not copy lists unnecessarily") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")

    registerUserFunction(ValueUtils.of(value))

    // Using graph execute to get a Java value
    graph.withTx( tx => {
      val returned = tx.execute("RETURN my.first.value() AS out").next().get("out")

      returned shouldBe an [util.ArrayList[_]]
      returned shouldBe value
    })
  }

  test("should not copy unnecessarily with nested types") {
    val value = new util.ArrayList[Any]()
    val inner = new util.ArrayList[Any]()
    value.add("Norris")
    value.add(inner)

    registerUserFunction(ValueUtils.of(value))

    // Using graph execute to get a Java value
    graph.withTx( tx => {
      val returned = tx.execute("RETURN my.first.value() AS out").next().get("out")

      returned shouldBe an [util.ArrayList[_]]
      returned shouldBe value
    })
  }

  test("should handle interacting with list") {
    val value = new util.ArrayList[Integer]()
    value.add(1)
    value.add(3)

    registerUserFunction(ValueUtils.of(value), Neo4jTypes.NTList(Neo4jTypes.NTInteger))

    // Using graph execute to get a Java value
    graph.withTx( tx => {
      val returned = tx.execute("WITH my.first.value() AS list RETURN list[0] + list[1] AS out")
        .next().get("out")

      returned should equal(4)
    })
  }

  test("should be able to use function returning list with list comprehension") {
    val value = new util.ArrayList[Integer]()
    value.add(1)
    value.add(2)

    registerUserFunction(ValueUtils.of(value), Neo4jTypes.NTAny)

    graph.withTx( tx => {
      val result = tx.execute("RETURN [x in my.first.value() | x + 1] as y")

      result.hasNext shouldBe true
      result.next.get("y").asInstanceOf[util.List[_]].asScala should equal(List(2, 3))
    })
  }

  test("should be able to use function returning list with ANY") {
    val value = new util.ArrayList[Integer]()
    value.add(1)
    value.add(2)

    registerUserFunction(ValueUtils.of(value), Neo4jTypes.NTAny)

    graph.withTx( tx => {
      val result = tx.execute("RETURN ANY(x in my.first.value() WHERE x = 2) as u")

      result.hasNext shouldBe true
      result.next.get("u") should equal(true)
    })
  }

  test("Fail when trying to pass map to function that takes string") {
    registerDummyInOutFunction(Neo4jTypes.NTString)

    val value = Map("index" -> 2, "apa" -> "monkey").asJava

    val transaction = graphOps.beginTx()
    try {
      // When & then
      val exception = the [QueryExecutionException] thrownBy transaction.execute("RETURN my.first.func($value) as u", map("value", value)).next()
      exception.getMessage should startWith("Type mismatch for parameter 'value': expected String but was Map, Node or Relationship")
    } finally {
      transaction.close()
    }
  }
}
