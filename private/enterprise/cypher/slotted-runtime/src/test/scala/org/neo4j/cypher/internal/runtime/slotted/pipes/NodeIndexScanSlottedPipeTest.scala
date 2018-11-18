/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ImplicitDummyPos, QueryStateHelper}
import org.neo4j.cypher.internal.v4_0.logical.plans.IndexOrderNone
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.opencypher.v9_0.expressions.{LabelName, LabelToken, PropertyKeyName, PropertyKeyToken}
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{LabelId, PropertyKeyId}

class NodeIndexScanSlottedPipeTest extends CypherFunSuite with ImplicitDummyPos with SlottedPipeTestHelper with IndexMockingHelp {

  private val label = LabelToken(LabelName("LabelName")_, LabelId(11))
  private val propertyKey = PropertyKeyToken(PropertyKeyName("PropertyName")_, PropertyKeyId(10))
  override val propertyKeys = Seq(propertyKey)
  private val node = nodeValue(11)

  private def nodeValue(id: Long) = {
    val node = mock[NodeValue]
    when(node.id()).thenReturn(id)
    node
  }

  test("should return nodes found by index scan when both labelId and property key id are solved at compile time") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = scanFor(Seq(nodeValueHit(node)))
    )

    // when
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
    val pipe = NodeIndexScanSlottedPipe("n", label, SlottedIndexedProperty(propertyKey.nameId.id, None), 0, IndexOrderNone, slots, slots.size())()
    val result = pipe.createResults(queryState)

    // then
    val list: Iterator[ExecutionContext] = result
    testableResult(list, slots).map(_("n")) should equal(List(node.id))
  }

  test("should use index provided values when available") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = scanFor(Seq(nodeValueHit(node, "hello")))
    )

    // when
    val nDotProp = "n." + propertyKey.name
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference(nDotProp, nullable = false, CTAny)
    val pipe = NodeIndexScanSlottedPipe("n", label, SlottedIndexedProperty(propertyKey.nameId.id, Some(slots.getReferenceOffsetFor(nDotProp))), 0, IndexOrderNone, slots, slots.size())()
    val result = pipe.createResults(queryState)

    // then
    val list: Iterator[ExecutionContext] = result
    testableResult(list, slots) should equal(List(
      Map("n" -> node.id, "n." + propertyKey.name -> Values.stringValue("hello"))
    ))
  }
}
