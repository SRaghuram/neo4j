/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.booleanValue

abstract class CheckDegreePrimitive(offset: Int, typ: Option[String], maxDegree: Expression)
  extends Expression
  with SlottedExpression {

  protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean
  protected def computePredicate(state: QueryState, node: Long, relType: Int, max: Int): Boolean

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {

    typ match {
      case None => maxDegree.apply(row, state) match {
        case x if x eq Values.NO_VALUE => Values.NO_VALUE
        case e => booleanValue(computePredicate(state, row.getLongAt(offset), NumericHelper.asPrimitiveInt(e)))
      }
      case Some(t) => state.query.getOptRelTypeId(t) match {
        case None => Values.FALSE
        case Some(relTypeId) => maxDegree.apply(row, state) match {
          case x if x eq Values.NO_VALUE => Values.NO_VALUE
          case e => booleanValue(computePredicate(state, row.getLongAt(offset), relTypeId, NumericHelper.asPrimitiveInt(e)))
        }
      }
    }
  }
  override def children: Seq[AstNode[_]] = Seq.empty
}

case class HasDegreeGreaterThanPrimitive(offset: Int, typ: Option[String], direction: SemanticDirection, maxDegree: Expression)
  extends CheckDegreePrimitive(offset, typ, maxDegree) {
  override protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1, node, direction, state.cursors.nodeCursor) > max
  override protected def computePredicate(state: QueryState, node: Long, relType:Int, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1, node, direction, relType, state.cursors.nodeCursor) > max
}

case class HasDegreeGreaterThanOrEqualPrimitive(offset: Int, typ: Option[String], direction: SemanticDirection, maxDegree: Expression)
  extends CheckDegreePrimitive(offset, typ, maxDegree) {
  override protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max, node, direction, state.cursors.nodeCursor) >= max
  override protected def computePredicate(state: QueryState, node: Long, relType:Int, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max, node, direction, relType, state.cursors.nodeCursor) >= max
}

case class HasDegreePrimitive(offset: Int, typ: Option[String], direction: SemanticDirection, maxDegree: Expression)
  extends CheckDegreePrimitive(offset, typ, maxDegree) {
  override protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1, node, direction, state.cursors.nodeCursor) == max
  override protected def computePredicate(state: QueryState, node: Long, relType:Int, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1, node, direction, relType, state.cursors.nodeCursor) == max
}

case class HasDegreeLessThanPrimitive(offset: Int, typ: Option[String], direction: SemanticDirection, maxDegree: Expression)
  extends CheckDegreePrimitive(offset, typ, maxDegree) {
  override protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max, node, direction, state.cursors.nodeCursor) < max
  override protected def computePredicate(state: QueryState, node: Long, relType:Int, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max, node, direction, relType, state.cursors.nodeCursor) < max
}

case class HasDegreeLessThanOrEqualPrimitive(offset: Int, typ: Option[String], direction: SemanticDirection, maxDegree: Expression)
  extends CheckDegreePrimitive(offset, typ, maxDegree) {
  override protected def computePredicate(state: QueryState, node: Long, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1 , node, direction, state.cursors.nodeCursor) <= max
  override protected def computePredicate(state: QueryState, node: Long, relType:Int, max: Int): Boolean =
    state.query.nodeGetDegreeWithMax(max + 1, node, direction, relType, state.cursors.nodeCursor) <=  max
}
