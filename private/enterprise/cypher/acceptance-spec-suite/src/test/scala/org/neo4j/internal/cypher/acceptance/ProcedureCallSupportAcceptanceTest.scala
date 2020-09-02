/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.collection.RawIterator
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.internal.helpers.collection.MapUtil.map
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.FieldSignature
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.stringValue

import scala.collection.JavaConverters.mapAsJavaMapConverter

class ProcedureCallSupportAcceptanceTest extends ProcedureCallAcceptanceTest {

  test("should return correctly typed map result (even if converting to and from scala representation internally)") {
    val value = new util.HashMap[String, Any]()
    value.put("name", "Cypher")
    value.put("level", 9001)

    registerProcedureReturningSingleValue(value)

    // Using graph execute to get a Java value
    graph.withTx( tx => {
      tx.execute("CALL my.first.value() YIELD out RETURN * LIMIT 1").stream().toArray.toList should equal(List(
        java.util.Collections.singletonMap("out", value)
      ))})
  }

  test("procedure calls and pattern comprehensions with complex query") {
    relate(createNode(), createNode(Map("age" -> 18L)))

    registerDummyInOutProcedure(Neo4jTypes.NTAny)

    // Using graph execute to get a Java value
    graph.withTx( tx => {
      tx.execute("CALL my.first.proc([reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)]) YIELD out0 UNWIND out0 AS result RETURN result").stream().toArray.toList should equal(List(
        java.util.Collections.singletonMap("result", 18L)
      ))})
  }

  test("should return correctly typed list result (even if converting to and from scala representation internally)") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")

    registerProcedureReturningSingleValue(value)

    // Using graph execute to get a Java value
    graph.withTx( tx => {
      tx.execute("CALL my.first.value() YIELD out RETURN * LIMIT 1").stream().toArray.toList should equal(List(
        java.util.Collections.singletonMap("out", value)
      ))})
  }

  test("should return correctly typed stream result (even if converting to and from scala representation internally)") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")
    val stream = value.stream()

    registerProcedureReturningSingleValue(stream)

    // Using graph execute to get a Java value
    graph.withTx( tx => {
      tx.execute("CALL my.first.value() YIELD out RETURN * LIMIT 1").stream().toArray.toList should equal(List(
        java.util.Collections.singletonMap("out", value)
      ))})
  }

  test("should not yield deprecated fields") {
    // given
    registerProcedure("something.with.deprecated.output") { builder =>
      builder.out(util.Arrays.asList(
        FieldSignature.outputField("one",Neo4jTypes.NTString),
        FieldSignature.outputField("oldTwo",Neo4jTypes.NTString, true),
        FieldSignature.outputField("newTwo",Neo4jTypes.NTString) ))
      new BasicProcedure(builder.build) {
        override def apply(ctx: Context,
                           input: Array[AnyValue],
                           resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] =
          RawIterator.of[Array[AnyValue], ProcedureException](Array(stringValue("alpha"), stringValue("junk"), stringValue("beta")))
      }
    }

    // then
    graph.withTx( tx => {
      tx.execute("CALL something.with.deprecated.output()").stream().toArray.toList should equal(List(
        map("one", "alpha", "newTwo", "beta")
      ))})
  }

  test("should be able to execute union of multiple token accessing procedures") {
    val a = createLabeledNode("Foo")
    val b = createLabeledNode("Bar")
    relate(a, b, "REL", Map("prop" -> 1))

    val query =
      "CALL db.labels() YIELD label RETURN label as result " +
        "UNION " +
        "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType as result " +
        "UNION " +
        "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey as result " +
        "UNION " +
        "CALL db.labels() YIELD label RETURN label as result"
    val transaction = graphOps.beginTx()
    try {
      val result = transaction.execute(query)

      result.columnAs[String]("result").stream().toArray.toList shouldEqual List(
        "Foo",
        "Bar",
        "REL",
        "prop"
      )
    } finally {
      transaction.close()
    }
  }

  test("Fail when trying to pass arbitrary object in and out of a procedure") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTAny)
    class Arbitrary {
      val foo = 42
    }
    val value = new Arbitrary
    val transaction = graphOps.beginTx()
    try {
      // When & then
      intercept[QueryExecutionException](transaction.execute("CALL my.first.proc($p)", map("p", value)).next())
    } finally {
      transaction.close()
    }
  }

  test("procedure calls and pattern comprehensions with simple query") {
    relate(createNode(), createNode(Map("age" -> 18L)))

    registerDummyInOutProcedure(Neo4jTypes.NTAny)

    // Using graph execute to get a Java value
    graph.withTx( tx => {
      tx.execute("CALL my.first.proc([reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)])").stream().toArray.toList should equal(List(
        java.util.Collections.singletonMap("out0", java.util.Collections.singletonList(18L))
      ))})
  }

  test("Fail when trying to pass map to procedure that takes string") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTString)
    val value = Map("a" -> "b").asJava
    val transaction = graphOps.beginTx()
    try {
      // When & then
      val exception = the[QueryExecutionException] thrownBy transaction.execute("CALL my.first.proc($p)", map("p", value)).next()
      exception.getMessage should startWith("Type mismatch for parameter 'p': expected String but was Map, Node or Relationship")
    } finally {
      transaction.close()
    }
  }

  test("should handle a procedure call followed by has-label check") {
    val labeledNodeId = createLabeledNode("Label").getId
    val transaction = graphOps.beginTx()
    val labeledNode = transaction.getNodeById(labeledNodeId)

    registerProcedure("getNode") { builder =>
      builder.out("node", Neo4jTypes.NTNode)
      new BasicProcedure(builder.build()) {

        override def apply(ctx: Context, input: Array[AnyValue], resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] =
          RawIterator.of(Array(ValueUtils.of(labeledNode)))
      }
    }

    val q =
      """CALL getNode() YIELD node
        |WHERE node.x IS NULL AND
        |      node.y IS NULL AND
        |      node.z IS NULL AND
        |      node:Label
        |RETURN node""".stripMargin

    val res = execute(q)
    res.toList shouldBe List(Map("node" -> labeledNode))

    transaction.close()
  }
}
