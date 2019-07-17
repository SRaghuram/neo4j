/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.slotted

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementTestBase
import org.neo4j.cypher.internal.v4_0.util.TransactionOutOfMemoryException

trait SlottedMemoryManagementTestBase {
  self: MemoryManagementTestBase[EnterpriseRuntimeContext] =>

  test("should kill primitive grouping aggregation query before it runs out of memory") {
    // given
    val input = infiniteNodeInput()

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
}
