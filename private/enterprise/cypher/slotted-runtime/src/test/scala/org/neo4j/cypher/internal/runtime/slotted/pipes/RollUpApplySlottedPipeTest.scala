/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.FakeEntityTestSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeTestSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.expressions.NodeFromSlot
import org.neo4j.cypher.internal.runtime.slotted.expressions.ReferenceFromSlot
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils.fromNodeEntity
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.VirtualValues

class RollUpApplySlottedPipeTest extends CypherFunSuite with PipeTestSupport with FakeEntityTestSupport {
  private val slots = SlotConfiguration
    .empty
    .newReference("a", nullable = true, CTNumber)
    .newReference("x", nullable = false, CTList(CTNumber)) // NOTE: This has to be last since that is the order in which the slots are assumed to be allocated

  private val collectionRefSlotOffset = slots.getReferenceOffsetFor("x")

  test("when rhs returns nothing, an empty collection should be produced") {
    // given
    val lhs = createLhs(1)
    val rhs = pipeWithResults { state => Iterator() }
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset,
      identifierToCollect = "y" -> ReferenceFromSlot(0),
      nullableIdentifiers = Set("a"), slots)()

    // when
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList

    // then
    val a_offset = slots.get("a").get.offset
    val x_offset = slots.get("x").get.offset
    result.head.getRefAt(a_offset) should equal(Values.longValue(1))
    result.head.getRefAt(x_offset) should equal(VirtualValues.EMPTY_LIST)
  }

  test("when rhs has null values on nullableIdentifiers, a null value should be produced") {
    // given
    val lhs = createLhs(null, 1)
    val rhs = pipeWithResults { state => Iterator() }
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset,
      identifierToCollect = "y" -> ReferenceFromSlot(0),
      nullableIdentifiers = Set("a"), slots)()

    // when
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList

    // then
    val a_offset = slots.get("a").get.offset
    val x_offset = slots.get("x").get.offset
    result(0).getRefAt(a_offset) should equal(NO_VALUE)
    result(0).getRefAt(x_offset) should equal(NO_VALUE)
    result(1).getRefAt(a_offset) should equal(Values.intValue(1))
    result(1).getRefAt(x_offset) should equal(VirtualValues.EMPTY_LIST)
  }

  test("when rhs produces multiple rows with values, they are turned into a collection") {
    // given
    val lhs = createLhs(1)
    val rhs = createRhs(1, 2, 3, 4)
    val yOffset = rhs.slots.getReferenceOffsetFor("y")
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset,
      identifierToCollect = "y" -> ReferenceFromSlot(yOffset),
      nullableIdentifiers = Set("a"), slots)()

    // when
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList

    // then
    val a_offset = slots.get("a").get.offset
    val x_offset = slots.get("x").get.offset
    result(0).getRefAt(a_offset) should equal(Values.longValue(1))
    result(0).getRefAt(x_offset) should equal(VirtualValues.list(Values.longValue(1), Values.longValue(2), Values.longValue(3), Values.longValue(4)))
  }

  test("should support node values on rhs") {

    // given
    val lhs = createLhs(1)
    val rhs = createRhsWithNumberOfNodes(2)
    val yOffset = rhs.slots.getLongOffsetFor("y")
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset,
      identifierToCollect = "y" -> NodeFromSlot(yOffset),
      nullableIdentifiers = Set("a"), slots)()

    val queryContext = Mockito.mock(classOf[QueryContext])
    val node0 = new FakeNode {
      override def getId(): Long = 0L
    }
    val node1 = new FakeNode {
      override def getId(): Long = 1L
    }
    val nodeOps = Mockito.mock(classOf[NodeOperations])
    when(queryContext.nodeOps).thenReturn(nodeOps)
    when(nodeOps.getById(0)).thenReturn(fromNodeEntity(node0))
    when(nodeOps.getById(1)).thenReturn(fromNodeEntity(node1))
    val queryState = QueryStateHelper.emptyWith(query = queryContext)

    // when
    val result = pipe.createResults(queryState).toList

    // then
    val a_offset = slots.get("a").get.offset
    val x_offset = slots.get("x").get.offset
    result.head.getRefAt(a_offset) should equal(Values.longValue(1))
    result.head.getRefAt(x_offset) should equal(VirtualValues.list(
      fromNodeEntity(node0),
      fromNodeEntity(node1)))
  }

  test("should set the QueryState when calling down to the RHS") {
    // given
    val lhs = createLhs(1)
    val rhs = mock[Pipe]
    when(rhs.createResults(any())).then(new Answer[Iterator[ExecutionContext]] {
      override def answer(invocation: InvocationOnMock) = {
        val state = invocation.getArguments.apply(0).asInstanceOf[QueryState]
        state.initialContext should not be empty
        Iterator.empty
      }
    })
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset,
      identifierToCollect = "y" -> ReferenceFromSlot(0),
      nullableIdentifiers = Set("a"), slots)()

    // when
    pipe.createResults(QueryStateHelper.empty).toList

    // then should not throw exception
  }

  private def createRhs(data: Any*) = {
    val rhsData = data.map(v => Map[Any, Any]("y" -> v))
    val rhsPipeline = slots.copy()
      .newReference("y", nullable = false, CTNumber)
    FakeSlottedPipe(rhsData, rhsPipeline)
  }

  private def createRhsWithNumberOfNodes(numberOfNodes: Int) = {
    val rhsData = for (i <- 0 until numberOfNodes) yield Map[Any, Any]("y" -> i)
    val rhsPipeline = slots.copy()
      .newLong("y", nullable = false, CTNode)
    FakeSlottedPipe(rhsData, rhsPipeline)
  }

  private def createLhs(data: Any*) = {
    val lhsData = data.map(v => Map[Any, Any]("a" -> v))
    FakeSlottedPipe(lhsData, slots)
  }
}

