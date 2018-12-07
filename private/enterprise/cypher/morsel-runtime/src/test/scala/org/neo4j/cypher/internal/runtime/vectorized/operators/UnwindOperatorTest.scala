/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ListLiteral
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.values.storable.Values.numberValue
import org.neo4j.values.storable.Values.stringValue

import scala.language.postfixOps

class UnwindOperatorTest extends MorselUnitTest {

  test("should work with multiple incoming rows") {
    val given = new Given()
      .withOperator(new UnwindOperator(workId, ListLiteral(Literal(10), Literal(11)), 1))
      .addInputRow(Longs(1, 2, 3), Refs(stringValue("a")))
      .addInputRow(Longs(4, 5, 6), Refs(stringValue("b")))
      .withOutput(3 longs, 2 refs, 3 rows)

    val task = given.whenInit().shouldReturnNTasks(1).head

    task.whenOperate
      .shouldReturnRow(Longs(1, 2, 3), Refs(stringValue("a"), numberValue(10L)))
      .shouldReturnRow(Longs(1, 2, 3), Refs(stringValue("a"), numberValue(11L)))
      .shouldReturnRow(Longs(4, 5, 6), Refs(stringValue("b"), numberValue(10L)))
      .shouldContinue()

    task.whenOperate
      .shouldReturnRow(Longs(4, 5, 6), Refs(stringValue("b"), numberValue(11L)))
      .shouldBeDone()
  }

  test("should work with zero list elements to unwind") {
    val given = new Given()
      .withOperator(new UnwindOperator(workId, ListLiteral(), 1))
      .addInputRow(Longs(1, 2, 3), Refs(stringValue("a")))
      .addInputRow(Longs(4, 5, 6), Refs(stringValue("b")))
      .withOutput(3 longs, 2 refs, 3 rows)

    val task = given.whenInit().shouldReturnNTasks(1).head

    task.whenOperate.shouldBeDone()
  }

  test("should work with zero incoming rows") {
    val given = new Given()
      .withOperator(new UnwindOperator(workId, ListLiteral(Literal(10), Literal(11)), 1))
      .withNoInputRow(3, 1)
      .withOutput(3 longs, 2 refs, 3 rows)

    val task = given.whenInit().shouldReturnNTasks(1).head

    task.whenOperate.shouldBeDone()
  }
}
