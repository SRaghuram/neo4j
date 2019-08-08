/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.{makeGetValueFromSlotFunctionFor, makeSetValueInSlotFunctionFor}
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues.list

case class SlotExpression(slot: Slot, expression: Expression, ordered: Boolean = false)

case object EmptyGroupingExpression extends GroupingExpression {
  override type KeyType = AnyValue

  override def registerOwningPipe(pipe: Pipe): Unit = {}
  override def computeGroupingKey(context: ExecutionContext,
                                  state: QueryState): AnyValue = Values.NO_VALUE
  override def computeOrderedGroupingKey(groupingKey: AnyValue): AnyValue = Values.NO_VALUE

  override def getGroupingKey(context: ExecutionContext): AnyValue = Values.NO_VALUE
  override def isEmpty: Boolean = true
  override def project(context: ExecutionContext,
                       groupingKey: AnyValue): Unit = {}
}

case class SlottedGroupingExpression1(groupingExpression: SlotExpression) extends GroupingExpression {
  override type KeyType = AnyValue

  private val setter = makeSetValueInSlotFunctionFor(groupingExpression.slot)
  private val getter = makeGetValueFromSlotFunctionFor(groupingExpression.slot)

  private val ordered: AnyValue => AnyValue =
    if (groupingExpression.ordered) identity else _ => Values.NO_VALUE

  override def registerOwningPipe(pipe: Pipe): Unit = groupingExpression.expression.registerOwningPipe(pipe)
  override def computeGroupingKey(context: ExecutionContext, state: QueryState): AnyValue = groupingExpression.expression(context, state)
  override def computeOrderedGroupingKey(groupingKey: AnyValue): AnyValue = ordered(groupingKey)

  override def getGroupingKey(context: ExecutionContext): AnyValue = getter(context)

  override def isEmpty: Boolean = false
  override def project(context: ExecutionContext,
                       groupingKey: AnyValue): Unit = setter(context, groupingKey)
}

case class SlottedGroupingExpression2(groupingExpression1: SlotExpression,
                                      groupingExpression2: SlotExpression) extends GroupingExpression {
  override type KeyType = ListValue

  private val setter1 = makeSetValueInSlotFunctionFor(groupingExpression1.slot)
  private val setter2 = makeSetValueInSlotFunctionFor(groupingExpression2.slot)
  private val getter1 = makeGetValueFromSlotFunctionFor(groupingExpression1.slot)
  private val getter2 = makeGetValueFromSlotFunctionFor(groupingExpression2.slot)

  private val ordered: ListValue => AnyValue =
    if (groupingExpression1.ordered) {
      if (groupingExpression2.ordered) {
        identity
      } else {
        l => l.head()
      }
    } else if (groupingExpression2.ordered) {
      l => l.last()
    } else {
      _ => Values.NO_VALUE
    }

  override def registerOwningPipe(pipe: Pipe): Unit = {
    groupingExpression1.expression.registerOwningPipe(pipe)
    groupingExpression2.expression.registerOwningPipe(pipe)
  }
  override def computeGroupingKey(context: ExecutionContext, state: QueryState): ListValue = list(
    groupingExpression1.expression(context, state),
    groupingExpression2.expression(context, state))

  override def computeOrderedGroupingKey(groupingKey: ListValue): AnyValue = ordered(groupingKey)

  override def getGroupingKey(context: ExecutionContext): ListValue = list(getter1(context), getter2(context))

  override def isEmpty: Boolean = false
  override def project(context: ExecutionContext,
                       groupingKey: ListValue): Unit = {
    setter1(context, groupingKey.value(0))
    setter2(context, groupingKey.value(1))
  }
}

case class SlottedGroupingExpression3(groupingExpression1: SlotExpression,
                                      groupingExpression2: SlotExpression,
                                      groupingExpression3: SlotExpression) extends GroupingExpression {
  override type KeyType = ListValue

  private val setter1 = makeSetValueInSlotFunctionFor(groupingExpression1.slot)
  private val setter2 = makeSetValueInSlotFunctionFor(groupingExpression2.slot)
  private val setter3 = makeSetValueInSlotFunctionFor(groupingExpression3.slot)
  private val getter1 = makeGetValueFromSlotFunctionFor(groupingExpression1.slot)
  private val getter2 = makeGetValueFromSlotFunctionFor(groupingExpression2.slot)
  private val getter3 = makeGetValueFromSlotFunctionFor(groupingExpression3.slot)

  private val ordered: ListValue => AnyValue =
    if (groupingExpression1.ordered) {
      if (groupingExpression2.ordered) {
        if (groupingExpression3.ordered) {
          identity
        } else {
          l => l.take(2)
        }
      } else if (groupingExpression3.ordered) {
        l => list(l.head(), l.last())
      } else {
        l => l.head()
      }
    } else if (groupingExpression2.ordered) {
      if (groupingExpression3.ordered) {
        l => l.drop(1)
      } else {
        l => l.value(1)
      }
    } else if (groupingExpression3.ordered) {
      l => l.last()
    } else {
      _ => Values.NO_VALUE
    }

  override def registerOwningPipe(pipe: Pipe): Unit = {
    groupingExpression1.expression.registerOwningPipe(pipe)
    groupingExpression2.expression.registerOwningPipe(pipe)
    groupingExpression3.expression.registerOwningPipe(pipe)
  }
  override def computeGroupingKey(context: ExecutionContext, state: QueryState): ListValue = list(
    groupingExpression1.expression(context, state),
    groupingExpression2.expression(context, state),
    groupingExpression3.expression(context, state))

  override def getGroupingKey(context: ExecutionContext): ListValue = list(getter1(context), getter2(context), getter3(context))

  override def computeOrderedGroupingKey(groupingKey: ListValue): AnyValue = ordered(groupingKey)

  override def isEmpty: Boolean = false

  override def project(context: ExecutionContext,
                       groupingKey: ListValue): Unit = {
    setter1(context, groupingKey.value(0))
    setter2(context, groupingKey.value(1))
    setter3(context, groupingKey.value(2))
  }
}

case class SlottedGroupingExpression(groupingExpressions: Array[SlotExpression]) extends GroupingExpression {

  override type KeyType = ListValue

  private val sortedGroupingExpression = groupingExpressions.sortBy(s => (!s.ordered, s.slot.offset))
  private val setters = sortedGroupingExpression.map(e => makeSetValueInSlotFunctionFor(e.slot))
  private val getters = sortedGroupingExpression.map(e => makeGetValueFromSlotFunctionFor(e.slot))
  // First the ordered columns, then the unordered ones
  private val expressions = sortedGroupingExpression.map(_.expression)
  private val numberOfSortedColumns = sortedGroupingExpression.count(_.ordered)

  override def registerOwningPipe(pipe: Pipe): Unit = {
    groupingExpressions.foreach(e => e.expression.registerOwningPipe(pipe))
  }
  override def computeGroupingKey(context: ExecutionContext, state: QueryState): ListValue = {
    val values = new Array[AnyValue](expressions.length)
    var i = 0
    while (i < values.length) {
      values(i) = expressions(i)(context, state)
      i += 1
    }
    list(values:_*)
  }

  override def computeOrderedGroupingKey(groupingKey: ListValue): AnyValue = {
    groupingKey.take(numberOfSortedColumns)
  }

  override def getGroupingKey(context: ExecutionContext): ListValue = {
    val values = new Array[AnyValue](expressions.length)
    var i = 0
    while (i < values.length) {
      values(i) = getters(i)(context)
      i += 1
    }
    list(values:_*)
  }

  override def isEmpty: Boolean = false
  override def project(context: ExecutionContext,
                       groupingKey: ListValue): Unit = {
    var i = 0
    while (i < groupingKey.size()) {
      setters(i)(context, groupingKey.value(i))
      i += 1
    }
  }
}
