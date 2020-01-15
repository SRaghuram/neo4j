/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.slotted

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementTestBase
import org.neo4j.exceptions.TransactionOutOfMemoryException
import org.neo4j.kernel.impl.util.ValueUtils

trait WithSlotsMemoryManagementTestBase {
  self: MemoryManagementTestBase[EnterpriseRuntimeContext] =>

  override protected def estimateSize(data: ValueToEstimate): Long = {
    data match {
      case E_INT => ValueUtils.of(0).estimatedHeapUsage()
      case E_INT_IN_DISTINCT => ValueUtils.of(0).estimatedHeapUsage() // Slotted does not wrap columns in lists for distinct
      case E_NODE_PRIMITIVE => 8 // Just a long in slotted
      case E_NODE_VALUE => 64 // Size of a NodeValue
    }
  }

}

trait SlottedMemoryManagementTestBase extends WithSlotsMemoryManagementTestBase {
  self: MemoryManagementTestBase[EnterpriseRuntimeContext] =>

  test("should kill primitive grouping aggregation query before it runs out of memory") {
    // given
    val input = infiniteNodeInput(estimateSize(E_INT))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(nodes = Seq("x"))
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

    test("should kill primitive distinct query before it runs out of memory") {
      // given
      val input = infiniteNodeInput(estimateSize(E_NODE_PRIMITIVE) * 2)

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .distinct("x AS x", "x AS y")
        .input(nodes = Seq("x"), nullable = false)
        .build()

      // then
      a[TransactionOutOfMemoryException] should be thrownBy {
        consume(execute(logicalQuery, runtime, input))
      }
    }

  test("should kill single primitive distinct query before it runs out of memory") {
    // given
    val input = infiniteNodeInput(estimateSize(E_NODE_PRIMITIVE))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(nodes = Seq("x"), nullable = false)
      .build()

    // then
    a[TransactionOutOfMemoryException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }
}
