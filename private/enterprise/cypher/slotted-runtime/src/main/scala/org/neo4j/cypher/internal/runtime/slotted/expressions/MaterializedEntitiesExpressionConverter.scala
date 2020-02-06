/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Function
import org.neo4j.cypher.internal.expressions.functions.Keys
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.CastSupport
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NullInNullOutExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

case class MaterializedEntitiesExpressionConverter(tokenContext: TokenContext) extends ExpressionConverter {

  override def toCommandExpression(id: Id, expression: expressions.Expression, self: ExpressionConverters): Option[commands.expressions.Expression] =
    expression match {
      case e: expressions.Property           => toCommandProperty(id, e, self)
      case e: expressions.HasLabels          => hasLabels(id, e, self)
      case e: expressions.FunctionInvocation => toCommandExpression(id, e.function, e, self)
      case _                         => None
    }

  override def toGroupingExpression(id: Id, groupings: Map[String, expressions.Expression], orderToLeverage: Seq[expressions.Expression],
                                    self: ExpressionConverters): Option[GroupingExpression] = None

  override def toCommandProjection(id: Id, projections: Map[String, expressions.Expression], self: ExpressionConverters): Option[CommandProjection] = None

  private def toCommandExpression(id: Id, expression: Function, invocation: expressions.FunctionInvocation,
                                  self: ExpressionConverters): Option[commands.expressions.Expression] =
    (expression, invocation.arguments.head) match {
      case (Exists, property: expressions.Property) =>
        Some(MaterializedEntityPropertyExists(self.toCommandExpression(id, property.map), getPropertyKey(property.propertyKey)))

      case (Keys, arg)   => Some(MaterializedEntityKeysFunction(self.toCommandExpression(id, arg)))
      case (Labels, arg) => Some(MaterializedEntityLabelsFunction(self.toCommandExpression(id, arg)))
      case _             => None
    }

  private def toCommandProperty(id: Id, e: expressions.LogicalProperty, self: ExpressionConverters): Option[commands.expressions.Expression] =
    e match {
      case Property(map, propertyKey) => Some(MaterializedEntityProperty(self.toCommandExpression(id, map), getPropertyKey(propertyKey)))
      case _                          => None
    }

  private def getPropertyKey(propertyKey: PropertyKeyName) = tokenContext.getOptPropertyKeyId(propertyKey.name) match {
    case Some(propertyKeyId) =>
      PropertyKey(propertyKey.name, propertyKeyId)
    case _                   =>
      PropertyKey(propertyKey.name)
  }

  private def hasLabels(id: Id, e: expressions.HasLabels, self: ExpressionConverters): Option[Predicate] =
    Some(e.labels
      .map { l =>
        MaterializedEntityHasLabel(self.toCommandExpression(id, e.expression),
          commands.values.KeyToken.Unresolved(l.name, commands.values.TokenType.Label)): Predicate
      }
      .reduceLeft(predicates.And.apply)
    )
}

case class MaterializedEntityProperty(mapExpr: commands.expressions.Expression, propertyKey: KeyToken) extends commands.expressions.Expression
                                                                                                     with Product with Serializable {

  private val property = commands.expressions.Property(mapExpr, propertyKey)

  def apply(ctx: CypherRow, state: QueryState): AnyValue = mapExpr(ctx, state) match {
    case n: NodeValue         => n.properties().get(propertyKey.name)
    case r: RelationshipValue => r.properties().get(propertyKey.name)
    case _                    => property.apply(ctx, state)
  }

  override def rewrite(f: commands.expressions.Expression => commands.expressions.Expression): commands.expressions.Expression
  = f(MaterializedEntityProperty(mapExpr.rewrite(f), propertyKey.rewrite(f)))

  override def children = Seq(mapExpr, propertyKey)

  override def arguments: Seq[commands.expressions.Expression] = Seq(mapExpr)

  override def toString = s"$mapExpr.${propertyKey.name}"
}

case class MaterializedEntityPropertyExists(variable: commands.expressions.Expression, propertyKey: KeyToken) extends Predicate {

  private val propertyExists = commands.predicates.PropertyExists(variable, propertyKey)

  override def isMatch(m: CypherRow, state: QueryState): Option[Boolean] = variable(m, state) match {
    case n: NodeValue         => Some(n.properties().containsKey(propertyKey.name))
    case r: RelationshipValue => Some(r.properties().containsKey(propertyKey.name))
    case _                    => propertyExists.isMatch(m, state)
  }

  override def toString: String = s"hasProp($variable.${propertyKey.name})"

  override def containsIsNull = false

  override def rewrite(f: commands.expressions.Expression => commands.expressions.Expression): commands.expressions.Expression =
    f(MaterializedEntityPropertyExists(variable.rewrite(f), propertyKey.rewrite(f)))

  override def arguments: Seq[commands.expressions.Expression] = Seq(variable)

  override def children: Seq[AstNode[_]] = Seq(variable, propertyKey)
}

case class MaterializedEntityHasLabel(entity: commands.expressions.Expression, label: KeyToken) extends Predicate {

  override def isMatch(m: CypherRow, state: QueryState): Option[Boolean] = entity(m, state) match {

    case IsNoValue() =>
      None

    case value =>
      val node = CastSupport.castOrFail[NodeValue](value)

      var i = 0
      while (i < node.labels().length()) {
        if (node.labels().stringValue(i).equals(label.name)) {
          return Some(true)
        }

        i += 1
      }

      Some(false)
  }

  override def toString = s"$entity:${label.name}"

  override def rewrite(f: commands.expressions.Expression => commands.expressions.Expression): commands.expressions.Expression =
    f(MaterializedEntityHasLabel(entity.rewrite(f), label.typedRewrite[KeyToken](f)))

  override def children: Seq[commands.expressions.Expression] = Seq(label, entity)

  override def arguments: Seq[commands.expressions.Expression] = Seq(entity)

  override def containsIsNull = false
}

case class MaterializedEntityKeysFunction(expr: Expression) extends NullInNullOutExpression(expr) {

  override def compute(value: AnyValue, ctx: CypherRow, state: QueryState): ListValue =
    value match {
      case n: NodeValue         => n.properties().keys()
      case r: RelationshipValue => r.properties().keys()
      case _                    =>
        CypherFunctions.keys(value, state.query, state.cursors.nodeCursor, state.cursors.relationshipScanCursor, state.cursors.propertyCursor)
    }

  override def rewrite(f: Expression => Expression): Expression = f(MaterializedEntityKeysFunction(expr.rewrite(f)))

  override def arguments: Seq[Expression] = Seq(expr)

  override def children: Seq[AstNode[_]] = Seq(expr)
}

case class MaterializedEntityLabelsFunction(nodeExpr: Expression) extends NullInNullOutExpression(nodeExpr) {

  override def compute(value: AnyValue, m: CypherRow, state: QueryState): AnyValue = {
    value match {
      case n: NodeValue => n.labels()
      case _            => CypherFunctions.labels(value, state.query, state.cursors.nodeCursor)
    }
  }

  override def rewrite(f: Expression => Expression): Expression = f(MaterializedEntityLabelsFunction(nodeExpr.rewrite(f)))

  override def arguments: Seq[Expression] = Seq(nodeExpr)

  override def children: Seq[AstNode[_]] = Seq(nodeExpr)
}


