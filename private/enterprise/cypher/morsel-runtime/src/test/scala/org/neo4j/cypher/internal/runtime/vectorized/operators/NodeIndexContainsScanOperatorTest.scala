/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.NodeValueHit
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexMockingHelp
import org.neo4j.cypher.internal.runtime.vectorized.EmptyQueryState
import org.neo4j.cypher.internal.v4_0.expressions.LabelName
import org.neo4j.cypher.internal.v4_0.expressions.LabelToken
import org.neo4j.cypher.internal.v4_0.expressions.PropertyKeyName
import org.neo4j.cypher.internal.v4_0.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.v4_0.util.LabelId
import org.neo4j.cypher.internal.v4_0.util.PropertyKeyId
import org.neo4j.cypher.internal.v4_0.util.symbols.CTAny
import org.neo4j.cypher.internal.v4_0.util.symbols.CTNode
import org.neo4j.internal.kernel.api.helpers.StubNodeValueIndexCursor
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

import scala.language.postfixOps

class NodeIndexContainsScanOperatorTest extends MorselUnitTest with ImplicitDummyPos with IndexMockingHelp {

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
    val queryContext = kernelScanFor(Seq(nodeValueHit(node, "hello")))

    val nDotProp = "n." + propertyKey.name
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference(nDotProp, nullable = false, CTAny)
    val given = new Given()
      .withOperator(new NodeIndexContainsScanOperator(workId, slots.getLongOffsetFor("n"), label.nameId.id,
        SlottedIndexedProperty(propertyKey.nameId.id, Some(slots.getReferenceOffsetFor(nDotProp))), Literal("hell"), SlotConfiguration.Size.zero)
      )
      .addInputRow()
      .withOutput(1 longs, 1 refs, 2 rows)
      .withContext(queryContext)
      .withQueryState(EmptyQueryState())

    val task = given.whenInit().shouldReturnNTasks(1).head
    task.whenOperate
      .shouldReturnRow(Longs(node.id), Refs(Values.stringValue("hello")))
      .shouldBeDone()
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
