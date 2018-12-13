package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.Slot
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, GroupingExpression}
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils.makeSetValueInSlotFunctionFor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues.list

case object EmptyGroupingExpression extends GroupingExpression {
  override type T = AnyValue
  override def registerOwningPipe(pipe: Pipe): Unit = {}
  override def groupingKey(context: ExecutionContext,
                           state: QueryState): AnyValue = Values.NO_VALUE
  override def isEmpty: Boolean = true
  override def project(context: ExecutionContext,
                       groupingKey: AnyValue): Unit = {}
}

case class SlottedGroupingExpression1(slot: Slot, expression: Expression) extends GroupingExpression {
  override type T = AnyValue
  private val setter = makeSetValueInSlotFunctionFor(slot)

  override def registerOwningPipe(pipe: Pipe): Unit = expression.registerOwningPipe(pipe)
  override def groupingKey(context: ExecutionContext,
                           state: QueryState): AnyValue = expression(context, state)
  override def isEmpty: Boolean = false
  override def project(context: ExecutionContext,
                       groupingKey: AnyValue): Unit = setter(context, groupingKey)
}

case class SlottedGroupingExpression2( slot1: Slot, e1: Expression,
                                       slot2: Slot, e2: Expression) extends GroupingExpression {
  private val setter1 = makeSetValueInSlotFunctionFor(slot1)
  private val setter2 = makeSetValueInSlotFunctionFor(slot2)

  override type T = ListValue

  override def registerOwningPipe(pipe: Pipe): Unit = {
    e1.registerOwningPipe(pipe)
    e2.registerOwningPipe(pipe)
  }
  override def groupingKey(context: ExecutionContext,
                           state: QueryState): ListValue = list(e1(context, state), e2(context, state))
  override def isEmpty: Boolean = false
  override def project(context: ExecutionContext,
                       groupingKey: ListValue): Unit = {
    setter1(context, groupingKey.value(0))
    setter2(context, groupingKey.value(1))
  }
}

case class SlottedGroupingExpression3( slot1: Slot, e1: Expression,
                                       slot2: Slot, e2: Expression,
                                       slot3: Slot, e3: Expression ) extends GroupingExpression {
  private val setter1 = makeSetValueInSlotFunctionFor(slot1)
  private val setter2 = makeSetValueInSlotFunctionFor(slot2)
  private val setter3 = makeSetValueInSlotFunctionFor(slot3)

  override type T = ListValue

  override def registerOwningPipe(pipe: Pipe): Unit = {
    e1.registerOwningPipe(pipe)
    e2.registerOwningPipe(pipe)
    e3.registerOwningPipe(pipe)
  }
  override def groupingKey(context: ExecutionContext,
                           state: QueryState): ListValue = list(e1(context, state), e2(context, state), e3(context, state))
  override def isEmpty: Boolean = false

  override def project(context: ExecutionContext,
                       groupingKey: ListValue): Unit = {
    setter1(context, groupingKey.value(0))
    setter2(context, groupingKey.value(1))
    setter3(context, groupingKey.value(2))
  }
}

case class SlottedGroupingExpression(groupingExpressions: Map[Slot, Expression]) extends GroupingExpression {

  private val setters = groupingExpressions.keys.map(makeSetValueInSlotFunctionFor).toArray
  private val expressions = groupingExpressions.values.toArray

  override type T = ListValue

  override def registerOwningPipe(pipe: Pipe): Unit = {
    groupingExpressions.values.foreach(_.registerOwningPipe(pipe))
  }
  override def groupingKey(context: ExecutionContext,
                           state: QueryState): ListValue = {
    val values = new Array[AnyValue](expressions.length)
    var i = 0
    while (i < values.length) {
      values(i) = expressions(i)(context, state)
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
