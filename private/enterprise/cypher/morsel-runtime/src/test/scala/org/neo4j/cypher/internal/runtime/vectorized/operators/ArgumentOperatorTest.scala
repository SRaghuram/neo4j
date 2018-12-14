/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.values.storable.Values

import scala.language.postfixOps

class ArgumentOperatorTest extends MorselUnitTest {

  test("should copy argument over and produce a single row") {
    val given = new Given()
      .operator(new ArgumentOperator(workId, SlotConfiguration.Size(1, 1)))
      .inputRow(Longs(1, 2, 3), Refs(Values.stringValue("a")))
      .inputRow(Longs(4, 5, 6), Refs(Values.stringValue("b")))
      .inputRow(Longs(7, 8, 9), Refs(Values.stringValue("c")))
      .output(2 longs)
      .output(2 refs)
      .output(1 rows)

    var task = given.whenInit().shouldReturnNTasks(1).head
    task.whenOperate
        .shouldReturnRow(Longs(1, 0), Refs(Values.stringValue("a"), null))
        .shouldBeDone()

    task = given.whenInit(rowNum = 1).shouldReturnNTasks(1).head
    task.whenOperate
      .shouldReturnRow(Longs(4, 0), Refs(Values.stringValue("b"), null))
      .shouldBeDone()

    task = given.whenInit(rowNum = 2).shouldReturnNTasks(1).head
    task.whenOperate
      .shouldReturnRow(Longs(7, 0), Refs(Values.stringValue("c"), null))
      .shouldBeDone()
  }

}
