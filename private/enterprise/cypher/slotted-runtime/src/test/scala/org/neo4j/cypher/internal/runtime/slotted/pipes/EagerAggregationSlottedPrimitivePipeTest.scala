/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CountStar
import org.neo4j.values.storable.Values.longValue
import org.neo4j.cypher.internal.v3_5.util.symbols._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class EagerAggregationSlottedPrimitivePipeTest extends CypherFunSuite with SlottedPipeTestHelper {
  test("should aggregate count(*) on two grouping columns") {
    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("b", nullable = false, CTNode)
      .newReference("count(*)", nullable = false, CTInteger)

    def source = FakeSlottedPipe(List(
      Map[String, Any]("a" -> 1, "b" -> 1),
      Map[String, Any]("a" -> 1, "b" -> 1),
      Map[String, Any]("a" -> 1, "b" -> 2),
      Map[String, Any]("a" -> 2, "b" -> 2)), slots)

    val grouping = createReturnItemsFor(slots,"a", "b")
    val aggregation = Map(slots("count(*)").offset -> CountStar())
    def aggregationPipe = EagerAggregationSlottedPrimitivePipe(source, slots, grouping, grouping, aggregation)()

    testableResult(aggregationPipe.createResults(QueryStateHelper.empty), slots) should be(List(
      Map[String, Any]("a" -> 1, "b" -> 1, "count(*)" -> longValue(2)),
      Map[String, Any]("a" -> 1, "b" -> 2, "count(*)" -> longValue(1)),
      Map[String, Any]("a" -> 2, "b" -> 2, "count(*)" -> longValue(1))
    ))
  }

  private def createReturnItemsFor(slots: SlotConfiguration, names: String*): Array[Int] = names.map(k => slots(k).offset).toArray

}
