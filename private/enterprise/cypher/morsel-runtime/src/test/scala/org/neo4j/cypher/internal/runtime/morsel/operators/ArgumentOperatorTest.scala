/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physical_planning.SlotConfiguration
import org.neo4j.values.storable.Values

import scala.language.postfixOps

class ArgumentOperatorTest extends MorselUnitTest {

  test("should copy argument over and produce a single row") {
    val given = new Given()
      .withOperator(new ArgumentOperator(workId, SlotConfiguration.Size(1, 1)))
      .addInputRow(Longs(1, 2, 3), Refs(Values.stringValue("a")))
      .addInputRow(Longs(4, 5, 6), Refs(Values.stringValue("b")))
      .addInputRow(Longs(7, 8, 9), Refs(Values.stringValue("c")))
      .withOutput(2 longs)
      .withOutput(2 refs)
      .withOutput(1 rows)

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
