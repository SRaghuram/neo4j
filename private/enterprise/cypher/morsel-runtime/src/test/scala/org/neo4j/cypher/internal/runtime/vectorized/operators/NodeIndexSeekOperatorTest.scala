/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{ListLiteral, Literal}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{IndexMockingHelp, LockingUniqueIndexSeek}
import org.neo4j.cypher.internal.runtime.vectorized.{EmptyQueryState, Morsel, MorselExecutionContext}
import org.neo4j.cypher.internal.v4_0.logical.plans.{CompositeQueryExpression, IndexOrderNone, ManyQueryExpression}
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.neo4j.cypher.internal.v4_0.expressions.{LabelName, LabelToken, PropertyKeyName, PropertyKeyToken}
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CTNode}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.{LabelId, PropertyKeyId}

class NodeIndexSeekOperatorTest extends CypherFunSuite with ImplicitDummyPos with IndexMockingHelp {

  private val cursors = new ExpressionCursors(mock[CursorFactory])

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

    // input data
    val inputRow = MorselExecutionContext.createSingleRow()

    // output data
    val numberOfLongs = 1
    val numberOfReferences = 1
    val outputRows = 2
    val outputMorsel = new Morsel(
      new Array[Long](numberOfLongs * outputRows),
      new Array[AnyValue](numberOfReferences * outputRows),
      outputRows)
    val outputRow = MorselExecutionContext(outputMorsel, numberOfLongs, numberOfReferences)

    // operator
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKey(0).name, nullable = false, CTAny)
    val properties = propertyKey.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray
    val operator = new NodeIndexSeekOperator(slots.getLongOffsetFor("n"), label, properties, 0, IndexOrderNone, SlotConfiguration.Size.zero,
      ManyQueryExpression(ListLiteral(
        Literal("hello"),
        Literal("bye")
      ))
    )

    // When
    operator.init(queryContext, EmptyQueryState(), inputRow, cursors).operate(outputRow, queryContext, EmptyQueryState(), cursors)

    // then
    outputMorsel.longs should equal(Array(
      node.id, node2.id))
    outputMorsel.refs should equal(Array(
      Values.stringValue("hello"), Values.stringValue("bye")))
    outputMorsel.validRows should equal(2)

  }

  test("should use composite index provided values when available") {
    // given
    val queryContext = indexFor[NodeWithValues](
      Seq("hello", "world") -> Seq(nodeValueHit(node, "hello", "world")),
      Seq("bye", "cruel") -> Seq(nodeValueHit(node2, "bye", "cruel"))
    )

    // input data
    val inputRow = MorselExecutionContext.createSingleRow()

    // output data
    val numberOfLongs = 1
    val numberOfReferences = 2
    val outputRows = 2
    val outputMorsel = new Morsel(
      new Array[Long](numberOfLongs * outputRows),
      new Array[AnyValue](numberOfReferences * outputRows),
      outputRows)
    val outputRow = MorselExecutionContext(outputMorsel, numberOfLongs, numberOfReferences)

    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKeys(0).name, nullable = false, CTAny)
      .newReference("n." + propertyKeys(1).name, nullable = false, CTAny)
    val properties = propertyKeys.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray
    val operator = new NodeIndexSeekOperator(slots.getLongOffsetFor("n"), label, properties, 0, IndexOrderNone, SlotConfiguration.Size.zero,
      CompositeQueryExpression(Seq(
        ManyQueryExpression(ListLiteral(
          Literal("hello"), Literal("bye")
        )),
        ManyQueryExpression(ListLiteral(
          Literal("world"), Literal("cruel")
        ))))
    )
    // When
    operator.init(queryContext, EmptyQueryState(), inputRow, cursors).operate(outputRow, queryContext, EmptyQueryState(), cursors)

    // then
    outputMorsel.longs should equal(Array(
      node.id,
      node2.id))
    outputMorsel.refs should equal(Array(
      Values.stringValue("hello"), Values.stringValue("world"),
      Values.stringValue("bye"), Values.stringValue("cruel")))
    outputMorsel.validRows should equal(2)
  }

  test("should use locking unique index provided values when available") {
    // given
    val queryContext = indexFor[NodeWithValues](
        Seq("hello") -> Seq(nodeValueHit(node, "hello")),
        Seq("world") -> Seq(nodeValueHit(node2, "bye"))
    )

    // input data
    val inputRow = MorselExecutionContext.createSingleRow()

    // output data
    val numberOfLongs = 1
    val numberOfReferences = 1
    val outputRows = 2
    val outputMorsel = new Morsel(
      new Array[Long](numberOfLongs * outputRows),
      new Array[AnyValue](numberOfReferences * outputRows),
      outputRows)
    val outputRow = MorselExecutionContext(outputMorsel, numberOfLongs, numberOfReferences)

    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKey(0).name, nullable = false, CTAny)
    val properties = propertyKey.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray

    val operator = new NodeIndexSeekOperator(slots.getLongOffsetFor("n"), label, properties, 0, IndexOrderNone, SlotConfiguration.Size.zero,
      ManyQueryExpression(ListLiteral(Literal("hello"), Literal("world"))), LockingUniqueIndexSeek)

    // When
    operator.init(queryContext, EmptyQueryState(), inputRow, cursors).operate(outputRow, queryContext, EmptyQueryState(), cursors)


    // then
    outputMorsel.longs should equal(Array(
      node.id,
      node2.id))
    outputMorsel.refs should equal(Array(
      Values.stringValue("hello"), Values.stringValue("bye")))
    outputMorsel.validRows should equal(2)
  }
}
