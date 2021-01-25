/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.runtime.slotted.pipes.TopSlottedPipeTestSupport.AscendingOrder
import org.neo4j.cypher.internal.runtime.slotted.pipes.TopSlottedPipeTestSupport.list
import org.neo4j.cypher.internal.runtime.slotted.pipes.TopSlottedPipeTestSupport.singleColumnTop1WithTiesWithInput
import org.neo4j.cypher.internal.runtime.slotted.pipes.TopSlottedPipeTestSupport.twoColumnTop1WithTiesWithInput
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class Top1WithTiesSlottedPipeTest extends CypherFunSuite {

  test("empty input gives empty output") {
    val result = singleColumnTop1WithTiesWithInput(List(), orderBy = AscendingOrder)
    result should be(empty)
  }

  test("simple sorting works as expected") {
    val input = List("B", "A")
    val result = singleColumnTop1WithTiesWithInput(input, orderBy = AscendingOrder)
    result should equal(list("A"))
  }

  test("two ties for the first place are all returned") {
    val input = List(
      (1, 1),
      (1, 2),
      (2, 3),
      (2, 4)
    )

    val result = twoColumnTop1WithTiesWithInput(input, orderBy = Seq(AscendingOrder))

    result should equal(list(
      (1, 1),
      (1, 2)
    ))
  }

  test("if only null is present, it should be returned") {
    val input = List(
      (null, 1),
      (null, 2)
    )

    val result = twoColumnTop1WithTiesWithInput(input, orderBy = Seq(AscendingOrder))

    result should equal(list(
      (null, 1),
      (null, 2)
    ))
  }

  test("null should not be returned if other values are present") {
    val input = List(
      (1, 1),
      (null, 2),
      (2, 3)
    )

    val result = twoColumnTop1WithTiesWithInput(input, orderBy = Seq(AscendingOrder))

    result should equal(list(
      (1, 1)
    ))
  }

  test("comparing arrays") {
    val smaller = Array(1, 2)
    val input = List(
      (Array(3,4), 2),
      (smaller, 1)
    )

    val result = twoColumnTop1WithTiesWithInput(input, orderBy = Seq(AscendingOrder))

    result should equal(list(
      (smaller, 1)
    ))
  }

  test("comparing numbers and strings") {
    val input = List(
      (1, 1),
      ("A", 2)
    )

    val result = twoColumnTop1WithTiesWithInput(input, orderBy = Seq(AscendingOrder))

    result should equal(list(
      ("A", 2)
    ))
  }
}
