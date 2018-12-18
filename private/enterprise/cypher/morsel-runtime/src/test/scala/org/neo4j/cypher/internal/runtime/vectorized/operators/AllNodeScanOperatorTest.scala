/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.values.storable.Values

import scala.language.postfixOps

class AllNodeScanOperatorTest extends MorselUnitTest {

  test("should copy argument over for every row") {
    // mock cursor
    val context = mock[QueryContext](RETURNS_DEEP_STUBS)
    val cursor1 = nodeCursor(10, 11, 12, 13, 14)
    val cursor2 = nodeCursor(10, 11, 12, 13, 14)
    when(resources.cursorPools.nodeCursorPool.allocate()).thenReturn(cursor1, cursor2)

    val given = new Given()
      .withOperator(new AllNodeScanOperator(workId, 2, SlotConfiguration.Size(2, 2)))
      .addInputRow(Longs(1, 2, 3), Refs(Values.stringValue("a"), Values.stringValue("b"), Values.stringValue("c")))
      .addInputRow(Longs(4, 5, 6), Refs(Values.stringValue("d"), Values.stringValue("e"), Values.stringValue("f")))
      .withOutput(3 longs, 2 refs, 6 rows)
      .withContext(context)
      .withQueryState(EmptyQueryState())

    val task = given.whenInit().shouldReturnNTasks(1).head

    task.whenOperate
      .shouldReturnRow(Longs(1, 2, 10), Refs(Values.stringValue("a"), Values.stringValue("b")))
      .shouldReturnRow(Longs(1, 2, 11), Refs(Values.stringValue("a"), Values.stringValue("b")))
      .shouldReturnRow(Longs(1, 2, 12), Refs(Values.stringValue("a"), Values.stringValue("b")))
      .shouldReturnRow(Longs(1, 2, 13), Refs(Values.stringValue("a"), Values.stringValue("b")))
      .shouldReturnRow(Longs(1, 2, 14), Refs(Values.stringValue("a"), Values.stringValue("b")))
      .shouldReturnRow(Longs(4, 5, 10), Refs(Values.stringValue("d"), Values.stringValue("e")))
      .shouldContinue()

    task.whenOperate
      .shouldReturnRow(Longs(4, 5, 11), Refs(Values.stringValue("d"), Values.stringValue("e")))
      .shouldReturnRow(Longs(4, 5, 12), Refs(Values.stringValue("d"), Values.stringValue("e")))
      .shouldReturnRow(Longs(4, 5, 13), Refs(Values.stringValue("d"), Values.stringValue("e")))
      .shouldReturnRow(Longs(4, 5, 14), Refs(Values.stringValue("d"), Values.stringValue("e")))
      .shouldBeDone()
  }

}
