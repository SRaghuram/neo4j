/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.mockito.Mockito.{RETURNS_DEEP_STUBS, when}
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexMockingHelp
import org.neo4j.cypher.internal.runtime.parallel.{WorkIdentity, WorkIdentityImpl}
import org.neo4j.cypher.internal.runtime.vectorized.{EmptyQueryState, Morsel, MorselExecutionContext, QueryResources}
import org.neo4j.cypher.internal.runtime.{NodeValueHit, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.{LabelName, LabelToken, PropertyKeyName, PropertyKeyToken}
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CTNode}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.{LabelId, PropertyKeyId}
import org.neo4j.internal.kernel.api.helpers.StubNodeValueIndexCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

class NodeIndexContainsScanOperatorTest extends CypherFunSuite with ImplicitDummyPos with IndexMockingHelp {

  private val resources = mock[QueryResources](RETURNS_DEEP_STUBS)

  private val workId: WorkIdentity = WorkIdentityImpl(42, "Work Identity Description")

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
    val inputRow = MorselExecutionContext.createSingleRow()

    // output data
    val numberOfLongs = 1
    val numberOfReferences = 1
    val outputRows = 1
    val outputMorsel = new Morsel(
      new Array[Long](numberOfLongs * outputRows),
      new Array[AnyValue](numberOfReferences * outputRows))
    val outputRow = MorselExecutionContext(outputMorsel, numberOfLongs, numberOfReferences, outputRows)

    val nDotProp = "n." + propertyKey.name
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference(nDotProp, nullable = false, CTAny)
    val operator = new NodeIndexContainsScanOperator(workId, slots.getLongOffsetFor("n"), label.nameId.id,
      SlottedIndexedProperty(propertyKey.nameId.id, Some(slots.getReferenceOffsetFor(nDotProp))), Literal("hell"), SlotConfiguration.Size.zero)

    // When
    operator.init(queryContext, EmptyQueryState(), inputRow, resources).operate(outputRow, queryContext, EmptyQueryState(), resources)

    // then
    outputMorsel.longs should equal(Array(
      node.id))
    outputMorsel.refs should equal(Array(
      Values.stringValue("hello")))
    outputRow.getValidRows should equal(1)
  }

  private def kernelScanFor(results: Iterable[NodeValueHit]): QueryContext = {
    import scala.collection.JavaConverters._

    val context = mock[QueryContext](RETURNS_DEEP_STUBS)

    val jIterator = results.map( hit => org.neo4j.helpers.collection.Pair.of(new java.lang.Long(hit.nodeId), hit.values)).iterator.asJava

    val cursor = new StubNodeValueIndexCursor(jIterator)
    when(context.transactionalContext.schemaRead.index(label.nameId.id, propertyKey.nameId.id).properties()).thenReturn(Array(propertyKey.nameId.id))
    when(resources.cursorPools.nodeValueIndexCursorPool.allocate()).thenReturn(cursor)
    context
  }
}
