/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{CountStar, Expression}
import org.neo4j.cypher.internal.runtime.slotted.expressions.ReferenceFromSlot
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.{intValue, longValue}

class EagerAggregationSlottedPipeTest extends CypherFunSuite with SlottedPipeTestHelper {
  test("should aggregate count(*) on two grouping columns") {
    val slots = SlotConfiguration.empty
      .newReference("a", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)
      .newReference("count(*)", nullable = false, CTInteger)

    def source = FakeSlottedPipe(List(
      Map[String, Any]("a" -> 1, "b" -> 1),
      Map[String, Any]("a" -> 1, "b" -> 1),
      Map[String, Any]("a" -> 1, "b" -> 2),
      Map[String, Any]("a" -> 2, "b" -> 2)), slots)

    val grouping = createReturnItemsFor(slots,"a", "b")
    val aggregation = Map(slots("count(*)").offset -> CountStar())
    def aggregationPipe = EagerAggregationSlottedPipe(source, slots,
                                                      SlottedGroupingExpression(grouping.map(t => SlotExpression(t._1, t._2)).toArray), aggregation)()

    testableResult(aggregationPipe.createResults(QueryStateHelper.empty), slots) should be(List(
      Map[String, AnyValue]("a" -> intValue(1), "b" -> intValue(1), "count(*)" -> longValue(2)),
      Map[String, AnyValue]("a" -> intValue(1), "b" -> intValue(2), "count(*)" -> longValue(1)),
      Map[String, AnyValue]("a" -> intValue(2), "b" -> intValue(2), "count(*)" -> longValue(1))
    ))
  }

  private def createReturnItemsFor(slots: SlotConfiguration, names: String*): Map[Slot, Expression] = names.map(k => slots(k) -> ReferenceFromSlot(slots(k).offset)).toMap

}
