/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.internal.kernel.api.NodeCursor

import scala.language.postfixOps

class AllNodeScanOperatorTest extends MorselUnitTest {

  test("should work with parallel scan in parallel mode") {
    // mock cursor
    val context = mock[QueryContext](RETURNS_DEEP_STUBS)
    val cursor1 = mock[NodeCursor] // the cursor the first task will get
    val cursor2 = mock[NodeCursor] // the cursor the second task will get
    when(resources.cursorPools.nodeCursorPool.allocate()).thenReturn(cursor1, cursor2)
    val scan = nodeCursorScan(Map(
      cursor1 -> List(Longs(11, 12, 13, 14), Longs(15, 16, 17)),
      cursor2 -> List(Longs(18, 19), Longs(20, 21, 22))
    ))
    when(context.transactionalContext.dataRead.allNodesScan()).thenReturn(scan)

    val queryState = mock[QueryState]
    when(queryState.morselSize).thenReturn(5)
    when(queryState.singeThreaded).thenReturn(false)
    when(queryState.numberOfWorkers).thenReturn(2) // the number of tasks we'll get

    val given = new Given()
      .withOperator(new AllNodeScanOperator(workId, 0, SlotConfiguration.Size(0, 0)))
      .addInputRow()
      .withOutput(1 longs, 0 refs, 2 rows)
      .withContext(context)
      .withQueryState(queryState)

    val tasks = given.whenInit().shouldReturnNTasks(2)

    tasks(0).whenOperate // stops because full output
      .shouldReturnRow(Longs(11))
      .shouldReturnRow(Longs(12))
      .shouldContinue()
    tasks(1).whenOperate // output full and cursor exhausted at the same time
      .shouldReturnRow(Longs(18))
      .shouldReturnRow(Longs(19))
      .shouldContinue()
    tasks(1).whenOperate // stops because full output
      .shouldReturnRow(Longs(20))
      .shouldReturnRow(Longs(21))
      .shouldContinue()
    tasks(0).whenOperate // output full and cursor exhausted at the same time
      .shouldReturnRow(Longs(13))
      .shouldReturnRow(Longs(14))
      .shouldContinue()
    tasks(0).whenOperate // stops because full output
      .shouldReturnRow(Longs(15))
      .shouldReturnRow(Longs(16))
      .shouldContinue()
    tasks(0).whenOperate // stops because there is no more data
      .shouldReturnRow(Longs(17))
      .shouldBeDone()
    tasks(1).whenOperate // stops because there is no more data
      .shouldReturnRow(Longs(22))
      .shouldBeDone()
  }

  test("should work with parallel scan in parallel with multiple input rows") {
    // mock cursor
    val context = mock[QueryContext](RETURNS_DEEP_STUBS)
    val cursor1 = mock[NodeCursor] // the cursor the first task will get
    val cursor2 = mock[NodeCursor] // the cursor the second task will get
    when(resources.cursorPools.nodeCursorPool.allocate()).thenReturn(cursor1, cursor2)
    val scan = nodeCursorScan(Map(
      cursor1 -> List(Longs(10, 11, 12), Longs(13)),
      cursor2 -> List(Longs(20, 21), Longs(22))
    ))
    when(context.transactionalContext.dataRead.allNodesScan()).thenReturn(scan)

    val queryState = mock[QueryState]
    when(queryState.morselSize).thenReturn(5)
    when(queryState.singeThreaded).thenReturn(false)
    when(queryState.numberOfWorkers).thenReturn(2) // the number of tasks we'll get

    val given = new Given()
      .withOperator(new AllNodeScanOperator(workId, 3, SlotConfiguration.Size(3, 0)))
      .addInputRow(Longs(1, 2, 3))
      .addInputRow(Longs(4, 5, 6))
      .addInputRow(Longs(7, 8, 9))
      .withOutput(4 longs, 0 refs, 2 rows)
      .withContext(context)
      .withQueryState(queryState)

    val tasks = given.whenInit().shouldReturnNTasks(2)

    tasks(0).whenOperate // stops because full output
      .shouldReturnRow(Longs(1, 2, 3, 10))
      .shouldReturnRow(Longs(4, 5, 6, 10))
      .shouldContinue()
    tasks(0).whenOperate // stops because full output
      .shouldReturnRow(Longs(7, 8, 9, 10))
      .shouldReturnRow(Longs(1, 2, 3, 11))
      .shouldContinue()
    tasks(0).whenOperate // output full and cursor exhausted at the same time
      .shouldReturnRow(Longs(4, 5, 6, 11))
      .shouldReturnRow(Longs(7, 8, 9, 11))
      .shouldContinue()
    tasks(0).whenOperate // stops because full output
      .shouldReturnRow(Longs(1, 2, 3, 12))
      .shouldReturnRow(Longs(4, 5, 6, 12))
      .shouldContinue()
    tasks(0).whenOperate // stops because full output
      .shouldReturnRow(Longs(7, 8, 9, 12))
      .shouldReturnRow(Longs(1, 2, 3, 13))
      .shouldContinue()
    tasks(0).whenOperate // output full and cursor exhausted and input exhausted at the same time, but done
      .shouldReturnRow(Longs(4, 5, 6, 13))
      .shouldReturnRow(Longs(7, 8, 9, 13))
      .shouldBeDone()

    tasks(1).whenOperate // stops because full output
      .shouldReturnRow(Longs(1, 2, 3, 20))
      .shouldReturnRow(Longs(4, 5, 6, 20))
      .shouldContinue()
    tasks(1).whenOperate // stops because full output
      .shouldReturnRow(Longs(7, 8, 9, 20))
      .shouldReturnRow(Longs(1, 2, 3, 21))
      .shouldContinue()
    tasks(1).whenOperate // output full and cursor exhausted and input exhausted at the same time
      .shouldReturnRow(Longs(4, 5, 6, 21))
      .shouldReturnRow(Longs(7, 8, 9, 21))
      .shouldContinue()
    tasks(1).whenOperate // stops because full output
      .shouldReturnRow(Longs(1, 2, 3, 22))
      .shouldReturnRow(Longs(4, 5, 6, 22))
      .shouldContinue()
    tasks(1).whenOperate // stops because no more work
      .shouldReturnRow(Longs(7, 8, 9, 22))
      .shouldBeDone()
  }

  test("should work with single-threaded scan in single-threaded mode") {
    // mock cursor
    val context = mock[QueryContext](RETURNS_DEEP_STUBS)
    val cursor1 = nodeCursor(10, 11, 12, 13, 14)
    when(resources.cursorPools.nodeCursorPool.allocate()).thenReturn(cursor1)

    val queryState = mock[QueryState]
    when(queryState.singeThreaded).thenReturn(true)

    val given = new Given()
      .withOperator(new AllNodeScanOperator(workId, 0, SlotConfiguration.Size(0, 0)))
      .addInputRow()
      .withOutput(1 longs, 0 refs, 3 rows)
      .withContext(context)
      .withQueryState(queryState)

    val task = given.whenInit().shouldReturnNTasks(1).head

    task.whenOperate
      .shouldReturnRow(Longs(10))
      .shouldReturnRow(Longs(11))
      .shouldReturnRow(Longs(12))
      .shouldContinue()
    task.whenOperate
      .shouldReturnRow(Longs(13))
      .shouldReturnRow(Longs(14))
      .shouldBeDone()
  }

  test("should work with single-threaded scan in single-threaded mode with multiple input rows") {
    // mock cursor
    val context = mock[QueryContext](RETURNS_DEEP_STUBS)
    val cursor1 = nodeCursor(10, 11, 12, 13, 14)
    val cursor2 = nodeCursor(15, 16)
    val cursor3 = nodeCursor(17, 18, 19, 20)
    when(resources.cursorPools.nodeCursorPool.allocate()).thenReturn(cursor1, cursor2, cursor3)

    val queryState = mock[QueryState]
    when(queryState.singeThreaded).thenReturn(true)

    val given = new Given()
      .withOperator(new AllNodeScanOperator(workId, 3, SlotConfiguration.Size(3, 0)))
      .addInputRow(Longs(1, 2, 3))
      .addInputRow(Longs(4, 5, 6))
      .addInputRow(Longs(7, 8, 9))
      .withOutput(4 longs, 0 refs, 9 rows)
      .withContext(context)
      .withQueryState(queryState)

    val task = given.whenInit().shouldReturnNTasks(1).head

    task.whenOperate
      .shouldReturnRow(Longs(1, 2, 3, 10))
      .shouldReturnRow(Longs(1, 2, 3, 11))
      .shouldReturnRow(Longs(1, 2, 3, 12))
      .shouldReturnRow(Longs(1, 2, 3, 13))
      .shouldReturnRow(Longs(1, 2, 3, 14))
      .shouldReturnRow(Longs(4, 5, 6, 15))
      .shouldReturnRow(Longs(4, 5, 6, 16))
      .shouldReturnRow(Longs(7, 8, 9, 17))
      .shouldReturnRow(Longs(7, 8, 9, 18))
      .shouldContinue()

    task.whenOperate
      .shouldReturnRow(Longs(7, 8, 9, 19))
      .shouldReturnRow(Longs(7, 8, 9, 20))
      .shouldBeDone()
  }

}
