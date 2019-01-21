/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ListLiteral
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexMockingHelp
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LockingUniqueIndexSeek
import org.neo4j.cypher.internal.runtime.morsel.EmptyQueryState
import org.neo4j.cypher.internal.v4_0.expressions.LabelName
import org.neo4j.cypher.internal.v4_0.expressions.LabelToken
import org.neo4j.cypher.internal.v4_0.expressions.PropertyKeyName
import org.neo4j.cypher.internal.v4_0.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.LabelId
import org.neo4j.cypher.internal.v4_0.util.PropertyKeyId
import org.neo4j.cypher.internal.v4_0.util.symbols.CTAny
import org.neo4j.cypher.internal.v4_0.util.symbols.CTNode
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

import scala.language.postfixOps

class NodeIndexSeekOperatorTest extends MorselUnitTest with ImplicitDummyPos with IndexMockingHelp {

  private val label = LabelToken(LabelName("LabelName") _, LabelId(11))
  private val propertyKey = Seq(PropertyKeyToken(PropertyKeyName("PropertyName") _, PropertyKeyId(10)))
  override val propertyKeys = propertyKey :+ PropertyKeyToken(PropertyKeyName("prop2") _, PropertyKeyId(11))
  private val node = nodeValue(1)
  private val node2 = nodeValue(2)

  private def nodeValue(id: Long) = {
    val node = mock[NodeValue]
    when(node.id()).thenReturn(id)
    node
  }

  test("should use index provided values when available") {
    // given
    val queryContext = indexFor[NodeWithValues](
      Seq("hello") -> Seq(nodeValueHit(node, "hello")),
      Seq("bye") -> Seq(nodeValueHit(node2, "bye"))
    )

    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKey(0).name, nullable = false, CTAny)
    val properties = propertyKey.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray
    val given = new Given()
      .withOperator(new NodeIndexSeekOperator(workId, slots.getLongOffsetFor("n"), label, properties, 0, IndexOrderNone, SlotConfiguration.Size.zero,
        ManyQueryExpression(ListLiteral(Literal("hello"), Literal("bye")))
      ))
      .addInputRow()
      .withOutput(1 longs, 1 refs, 3 rows)
      .withContext(queryContext)
      .withQueryState(EmptyQueryState())

    val task = given.whenInit().shouldReturnNTasks(1).head
    task.whenOperate
      .shouldReturnRow(Longs(node.id), Refs(Values.stringValue("hello")))
      .shouldReturnRow(Longs(node2.id), Refs(Values.stringValue("bye")))
      .shouldBeDone()
  }

  test("should use composite index provided values when available") {
    // given
    val queryContext = indexFor[NodeWithValues](
      Seq("hello", "world") -> Seq(nodeValueHit(node, "hello", "world")),
      Seq("bye", "cruel") -> Seq(nodeValueHit(node2, "bye", "cruel"))
    )

    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKeys(0).name, nullable = false, CTAny)
      .newReference("n." + propertyKeys(1).name, nullable = false, CTAny)
    val properties = propertyKeys.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray

    val given = new Given()
      .withOperator(new NodeIndexSeekOperator(workId, slots.getLongOffsetFor("n"), label, properties, 0, IndexOrderNone, SlotConfiguration.Size.zero,
        CompositeQueryExpression(Seq(
          ManyQueryExpression(ListLiteral(
            Literal("hello"), Literal("bye")
          )),
          ManyQueryExpression(ListLiteral(
            Literal("world"), Literal("cruel")
          ))))
      ))
      .addInputRow()
      .withOutput(1 longs, 2 refs, 3 rows)
      .withContext(queryContext)
      .withQueryState(EmptyQueryState())

    val task = given.whenInit().shouldReturnNTasks(1).head
    task.whenOperate
      .shouldReturnRow(Longs(node.id), Refs(Values.stringValue("hello"), Values.stringValue("world")))
      .shouldReturnRow(Longs(node2.id), Refs(Values.stringValue("bye"), Values.stringValue("cruel")))
      .shouldBeDone()
  }

  test("should use locking unique index provided values when available") {
    // given
    val queryContext = indexFor[NodeWithValues](
        Seq("hello") -> Seq(nodeValueHit(node, "hello")),
        Seq("world") -> Seq(nodeValueHit(node2, "bye"))
    )

    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKey(0).name, nullable = false, CTAny)
    val properties = propertyKey.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray

    val given = new Given()
      .withOperator(new NodeIndexSeekOperator(workId, slots.getLongOffsetFor("n"), label, properties, 0, IndexOrderNone, SlotConfiguration.Size.zero,
        ManyQueryExpression(ListLiteral(Literal("hello"), Literal("world"))), LockingUniqueIndexSeek)
      )
      .addInputRow()
      .withOutput(1 longs, 1 refs, 3 rows)
      .withContext(queryContext)
      .withQueryState(EmptyQueryState())

    val task = given.whenInit().shouldReturnNTasks(1).head
    task.whenOperate
      .shouldReturnRow(Longs(node.id), Refs(Values.stringValue("hello")))
      .shouldReturnRow(Longs(node2.id), Refs(Values.stringValue("bye")))
      .shouldBeDone()
  }
}
