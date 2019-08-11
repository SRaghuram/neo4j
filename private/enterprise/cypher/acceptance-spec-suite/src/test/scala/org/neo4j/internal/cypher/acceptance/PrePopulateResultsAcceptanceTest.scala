/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscriberAdapter}
import org.neo4j.kernel.impl.util.{NodeProxyWrappingNodeValue, RelationshipProxyWrappingValue}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.neo4j.values.virtual._

class PrePopulateResultsAcceptanceTest extends ExecutionEngineFunSuite {

  test("should not populate node unless asked to") {
    val n1 = createNode()

    val query = "MATCH (n) RETURN n"

    assertOnOnlyReturnValue(query, false,
      {
        case n: NodeReference => // ok
        case n: NodeProxyWrappingNodeValue => if (n.isPopulated) fail("Node proxy is populated")
        case n: NodeValue => fail("did not expect populated node value")
      }
    )
  }

  test("should not populate relationship unless asked to") {
    val n1 = createNode()
    relate(n1, createNode())

    val query = "MATCH ()-[r]-() RETURN r"

    assertOnOnlyReturnValue(query, false,
      {
        case r: RelationshipReference => // ok
        case r: RelationshipProxyWrappingValue => if (r.isPopulated) fail("Relationship proxy is populated")
        case r: RelationshipValue => fail("did not expect populated relationship value")
      }
    )
  }

  test("should populate node if asked to") {
    createNode()

    val query = "MATCH (n) RETURN n"

    assertOnOnlyReturnValue(query, prePopulateResults = true, assertPopulatedNode)
  }

  test("should populate relationship if asked to") {
    val n1 = createNode()
    relate(n1, createNode())

    val query = "MATCH ()-[r]-() RETURN r"

    assertOnOnlyReturnValue(query, prePopulateResults = true, assertPopulatedRelationship)
  }

  test("should populate paths if asked to") {
    val n1 = createNode()
    relate(n1, createNode())

    val query = "MATCH p = ()-[r]-() RETURN p"

    assertOnOnlyReturnValue(query, prePopulateResults = true,
      value => {
        val path = value.asInstanceOf[PathValue]
        for (n <- path.nodes()) assertPopulatedNode(n)
        for (r <- path.relationships()) assertPopulatedRelationship(r)
      })
  }

  test("should populate inside maps and lists if asked to") {
    createNode()

    val query = "MATCH (n) RETURN {prop: [{prop: [n]}]}"

    assertOnOnlyReturnValue(query, true,
      {
        case m1: MapValue =>
          val l1 = m1.get("prop").asInstanceOf[ListValue]
          l1.value(0) match {
            case m2: MapValue =>
              val l2 = m2.get("prop").asInstanceOf[ListValue]
              assertPopulatedNode(l2.value(0))
          }
      }
    )
  }

  private def assertPopulatedNode(value: AnyValue): Unit =
    value match {
      case n: NodeReference => fail("did not expect node reference")
      case n: NodeProxyWrappingNodeValue => if (!n.isPopulated) fail("Node proxy is not populated")
      case n: NodeValue => // ok
    }

  private def assertPopulatedRelationship(value: AnyValue): Unit =
    value match {
      case r: RelationshipReference => fail("did not expect relationship reference")
      case r: RelationshipProxyWrappingValue => if (!r.isPopulated) fail("Relationship proxy is not populated")
      case r: RelationshipValue => // ok
    }

  private def assertOnOnlyReturnValue(query: String, prePopulateResults: Boolean, f: AnyValue => Unit): Unit = {
    val prefixes =
      List(
        "CYPHER runtime=interpreted",
        "CYPHER runtime=slotted",
        "CYPHER runtime=compiled",
        "CYPHER runtime=morsel",
        "CYPHER runtime=parallel",
        ""
      )

    for (prefix <- prefixes) {
      withClue(s"[$prefix]: ") {
        val q = prefix + " " + query
        graph.withTx { tx =>
          val context = graph.transactionalContext(tx, query = q -> Map.empty)
          try {
            val subscriber: QuerySubscriber = new QuerySubscriberAdapter {
              private var currentOffset = -1

              override def onRecord(): Unit = currentOffset = 0

              override def onRecordCompleted(): Unit = currentOffset = -1

              override def onField(value: AnyValue): Unit = {
                try {
                  if (currentOffset == 0) {
                    f(value)
                  }
                } finally {
                  currentOffset += 1
                }
              }
            }
            val result = eengine.execute(q,
              EMPTY_MAP,
              context,
              profile = false,
              prePopulate = prePopulateResults,
              subscriber = subscriber)

            result.consumeAll()
          } finally {
            context.close()
          }
        }
      }
    }
  }
}
