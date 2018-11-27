/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.mockito.Mockito.{RETURNS_DEEP_STUBS, when}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexMockingHelp
import org.neo4j.cypher.internal.runtime.vectorized.{Morsel, MorselExecutionContext, QueryState}
import org.neo4j.cypher.internal.runtime.{NodeValueHit, QueryContext}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.neo4j.cypher.internal.v3_5.expressions.{LabelName, LabelToken, PropertyKeyName, PropertyKeyToken}
import org.neo4j.cypher.internal.v3_5.util.symbols.{CTAny, CTNode}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util.{LabelId, PropertyKeyId}

class NodeIndexScanOperatorTest extends CypherFunSuite with ImplicitDummyPos with IndexMockingHelp {

  private val label = LabelToken(LabelName("LabelName") _, LabelId(11))
  private val propertyKey = PropertyKeyToken(PropertyKeyName("PropertyName") _, PropertyKeyId(10))
  override val propertyKeys = Seq(propertyKey)
  private val node = nodeValue(11)

  private def nodeValue(id: Long) = {
    val node = mock[NodeValue]
    when(node.id()).thenReturn(id)
    node
  }

  test("should use index provided values when available") {
    // given
    val queryContext = kernelScanFor(Seq(nodeValueHit(node, "hello")))

    // input data
    val inputMorsel = new Morsel(new Array[Long](0), new Array[AnyValue](0), 0)
    val inputRow = MorselExecutionContext(inputMorsel, 0, 0)

    // output data
    val numberOfLongs = 1
    val numberOfReferences = 1
    val outputRows = 1
    val outputMorsel = new Morsel(
      new Array[Long](numberOfLongs * outputRows),
      new Array[AnyValue](numberOfReferences * outputRows),
      outputRows)
    val outputRow = MorselExecutionContext(outputMorsel, numberOfLongs, numberOfReferences)

    val nDotProp = "n." + propertyKey.name
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference(nDotProp, nullable = false, CTAny)
    val operator = new NodeIndexScanOperator(slots.getLongOffsetFor("n"), label.nameId.id,
      SlottedIndexedProperty(propertyKey.nameId.id, Some(slots.getReferenceOffsetFor(nDotProp))), slots.size())

    // When
    operator.init(queryContext, QueryState.EMPTY, inputRow).operate(outputRow, queryContext, QueryState.EMPTY)

    // then
    outputMorsel.longs should equal(Array(
      node.id))
    outputMorsel.refs should equal(Array(
      Values.stringValue("hello")))
    outputMorsel.validRows should equal(1)
  }

  private def kernelScanFor(results: Iterable[NodeValueHit]): QueryContext = {
    import scala.collection.JavaConverters._

    val context = mock[QueryContext](RETURNS_DEEP_STUBS)

    val jIterator = results.map( hit => org.neo4j.helpers.collection.Pair.of(new java.lang.Long(hit.nodeId), hit.values)).iterator.asJava

    val cursor = new org.neo4j.internal.kernel.api.helpers.StubNodeValueIndexCursor(jIterator)
    when(context.transactionalContext.cursors.allocateNodeValueIndexCursor).thenReturn(cursor)
    context
  }
}
