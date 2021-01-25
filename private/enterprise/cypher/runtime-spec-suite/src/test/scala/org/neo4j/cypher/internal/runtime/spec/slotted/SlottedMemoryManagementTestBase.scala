/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.slotted

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementTestBase
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.memory.MemoryLimitExceededException
import org.neo4j.values.virtual.VirtualValues

trait WithSlotsMemoryManagementTestBase {
  self: MemoryManagementTestBase[EnterpriseRuntimeContext] =>

  override protected def estimateSize(data: ValueToEstimate): Long = {
    data match {
      case E_INT => ValueUtils.of(0L).estimatedHeapUsage()
      case E_INT_IN_DISTINCT => ValueUtils.of(0L).estimatedHeapUsage() // Slotted does not wrap single columns in lists for distinct
      case E_NODE_PRIMITIVE => java.lang.Long.BYTES // Just a long in slotted
      case E_NODE_VALUE => VirtualValues.node(0).estimatedHeapUsage()
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
    a[MemoryLimitExceededException] should be thrownBy {
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
    a[MemoryLimitExceededException] should be thrownBy {
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
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }

  test("should kill primitive ordered distinct query before it runs out of memory") {
    // given
    val sameNode = runtimeTestSupport.tx.createNode()
    val input = infiniteNodeInput(estimateSize(E_NODE_PRIMITIVE), Some(_ => Array(sameNode, runtimeTestSupport.tx.createNode())))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .orderedDistinct(Seq("x"),"x AS x", "y AS y")
      .input(nodes = Seq("x", "y"), nullable = false)
      .build()

    // then
    a[MemoryLimitExceededException] should be thrownBy {
      consume(execute(logicalQuery, runtime, input))
    }
  }
}
