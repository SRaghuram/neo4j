/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.parallel.{WorkIdentity, WorkIdentityImpl}
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.{CursorFactory, NodeCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class AllNodeScanOperatorTest extends CypherFunSuite {

  private val resources = new QueryResources(mock[CursorFactory])

  private val workId: WorkIdentity = WorkIdentityImpl(42, "Work Identity Description")

  test("should copy argument over for every row") {
    // Given

    // input data
    val inputLongs = 3
    val inputRefs = 3
    val inputRows = 2
    val inputMorsel = new Morsel(
      Array[Long](1, 2, 3,
                  4, 5, 6),
      Array[AnyValue](Values.stringValue("a"), Values.stringValue("b"), Values.stringValue("c"),
                      Values.stringValue("d"), Values.stringValue("e"), Values.stringValue("f")))
    val inputRow = MorselExecutionContext(inputMorsel, inputLongs, inputRefs, inputRows)

    // output data (that can fit everything)
    val outputLongs = 3
    val outputRefs = 2
    val outputRows = 5
    val outputMorsel = new Morsel(
      new Array[Long](outputLongs * outputRows),
      new Array[AnyValue](outputRefs * outputRows))
    val outputRow = MorselExecutionContext(outputMorsel, outputLongs, outputRefs, outputRows)

    // operator and argument size
    val operator = new AllNodeScanOperator(workId, 2, SlotConfiguration.Size(2, 2))

    // mock cursor
    val context = mock[QueryContext](RETURNS_DEEP_STUBS)
    val cursor1 = mock[NodeCursor]
    val cursor2 = mock[NodeCursor]
    when(cursor1.next()).thenReturn(true, true, true, true, true, false)
    when(cursor2.next()).thenReturn(true, true, true, true, true, false)
    when(cursor1.nodeReference()).thenReturn(10, 11, 12, 13, 14)
    when(cursor2.nodeReference()).thenReturn(10, 11, 12, 13, 14)
    when(context.transactionalContext.cursors.allocateNodeCursor()).thenReturn(cursor1, cursor2)

    // When
    operator.init(context, null, inputRow, resources).operate(outputRow, context, EmptyQueryState(), resources)

    // Then
    outputMorsel.longs should equal(Array(
      1, 2, 10,
      1, 2, 11,
      1, 2, 12,
      1, 2, 13,
      1, 2, 14))
    outputMorsel.refs should equal(Array(
      Values.stringValue("a"), Values.stringValue("b"),
      Values.stringValue("a"), Values.stringValue("b"),
      Values.stringValue("a"), Values.stringValue("b"),
      Values.stringValue("a"), Values.stringValue("b"),
      Values.stringValue("a"), Values.stringValue("b")))
    outputRow.getValidRows should equal(5)

    // And when
    inputRow.moveToNextRow()
    outputRow.resetToFirstRow()
    operator.init(context, null, inputRow, resources).operate(outputRow, context, EmptyQueryState(), resources)

    // Then
    outputMorsel.longs should equal(Array(
      4, 5, 10,
      4, 5, 11,
      4, 5, 12,
      4, 5, 13,
      4, 5, 14))
    outputMorsel.refs should equal(Array(
      Values.stringValue("d"), Values.stringValue("e"),
      Values.stringValue("d"), Values.stringValue("e"),
      Values.stringValue("d"), Values.stringValue("e"),
      Values.stringValue("d"), Values.stringValue("e"),
      Values.stringValue("d"), Values.stringValue("e")))
    outputRow.getValidRows should equal(5)
  }

}
