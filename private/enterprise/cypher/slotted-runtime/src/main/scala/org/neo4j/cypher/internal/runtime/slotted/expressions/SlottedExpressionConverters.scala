/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{ast => runtimeAst}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.{expressions => commands}
import org.neo4j.cypher.internal.runtime.interpreted.{CommandProjection, GroupingExpression}
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath._
import org.neo4j.cypher.internal.runtime.slotted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.{expressions => runtimeExpression}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.{expressions => ast}

case class SlottedExpressionConverters(physicalPlan: PhysicalPlan) extends ExpressionConverter {

  override def toCommandProjection(id: Id, projections: Map[String, Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = {
    val slots = physicalPlan.slotConfigurations(id)
    val projected = for {(k, v) <- projections} yield slots.get(k).get.offset -> self.toCommandExpression(id, v)
    Some(SlottedCommandProjection(projected))
  }

  override def toGroupingExpression(id: Id, projections: Map[String, Expression],
                                    self: ExpressionConverters): Option[GroupingExpression] = {
    val slots = physicalPlan.slotConfigurations(id)
    val projected = for {(k, v) <- projections} yield slots.get(k).get -> self.toCommandExpression(id, v)
    projected.toList match {
      case Nil => Some(EmptyGroupingExpression)
      case (slot, e)::Nil => Some(SlottedGroupingExpression1(slot, e))
      case (s1, e1)::(s2, e2)::Nil => Some(SlottedGroupingExpression2(s1, e1, s2, e2))
      case (s1, e1)::(s2, e2)::(s3, e3)::Nil => Some(SlottedGroupingExpression3(s1, e1, s2, e2, s3, e3))
      case _ => Some(SlottedGroupingExpression(projected))
    }
  }

  override def toCommandExpression(id: Id, expression: ast.Expression, self: ExpressionConverters): Option[commands.Expression] =
    expression match {
      case runtimeAst.NodeFromSlot(offset, _) =>
        Some(runtimeExpression.NodeFromSlot(offset))
      case runtimeAst.RelationshipFromSlot(offset, _) =>
        Some(runtimeExpression.RelationshipFromSlot(offset))
      case runtimeAst.ReferenceFromSlot(offset, _) =>
        Some(runtimeExpression.ReferenceFromSlot(offset))
      case runtimeAst.NodeProperty(offset, token, _) =>
        Some(runtimeExpression.NodeProperty(offset, token))
      case runtimeAst.CachedNodeProperty(offset, token, cachedPropertyOffset) =>
        Some(runtimeExpression.SlottedCachedNodeProperty(offset, token, cachedPropertyOffset))
      case runtimeAst.RelationshipProperty(offset, token, _) =>
        Some(runtimeExpression.RelationshipProperty(offset, token))
      case runtimeAst.IdFromSlot(offset) =>
        Some(runtimeExpression.IdFromSlot(offset))
      case runtimeAst.NodePropertyLate(offset, propKey, _) =>
        Some(runtimeExpression.NodePropertyLate(offset, propKey))
      case runtimeAst.CachedNodePropertyLate(offset, propertyKey, cachedPropertyOffset) =>
        Some(runtimeExpression.SlottedCachedNodePropertyLate(offset, propertyKey, cachedPropertyOffset))
      case runtimeAst.RelationshipPropertyLate(offset, propKey, _) =>
        Some(runtimeExpression.RelationshipPropertyLate(offset, propKey))
      case runtimeAst.PrimitiveEquals(a, b) =>
        val lhs = self.toCommandExpression(id, a)
        val rhs = self.toCommandExpression(id, b)
        Some(runtimeExpression.PrimitiveEquals(lhs, rhs))
      case runtimeAst.GetDegreePrimitive(offset, typ, direction) =>
        Some(runtimeExpression.GetDegreePrimitive(offset, typ, direction))
      case runtimeAst.NodePropertyExists(offset, token, _) =>
        Some(runtimeExpression.NodePropertyExists(offset, token))
      case runtimeAst.NodePropertyExistsLate(offset, token, _) =>
        Some(runtimeExpression.NodePropertyExistsLate(offset, token))
      case runtimeAst.RelationshipPropertyExists(offset, token, _) =>
        Some(runtimeExpression.RelationshipPropertyExists(offset, token))
      case runtimeAst.RelationshipPropertyExistsLate(offset, token, _) =>
        Some(runtimeExpression.RelationshipPropertyExistsLate(offset, token))
      case runtimeAst.NullCheck(offset, inner) =>
        val a = self.toCommandExpression(id, inner)
        Some(runtimeExpression.NullCheck(offset, a))
      case runtimeAst.NullCheckVariable(offset, inner) =>
        val a = self.toCommandExpression(id, inner)
        Some(runtimeExpression.NullCheck(offset, a))
      case runtimeAst.NullCheckProperty(offset, inner) =>
        val a = self.toCommandExpression(id, inner)
        Some(runtimeExpression.NullCheck(offset, a))
      case e: ast.PathExpression =>
        Some(toCommandProjectedPath(id, e, self))
      case runtimeAst.IsPrimitiveNull(offset) =>
        Some(runtimeExpression.IsPrimitiveNull(offset))
      case _ =>
        None
    }

  def toCommandProjectedPath(id:Id, e: ast.PathExpression, self: ExpressionConverters): SlottedProjectedPath = {
    def project(pathStep: PathStep): Projector = pathStep match {

      case NodePathStep(nodeExpression, next) =>
        singleNodeProjector(toCommandExpression(id, nodeExpression, self).get, project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.INCOMING, next) =>
        singleIncomingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.OUTGOING, next) =>
        singleOutgoingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.BOTH, next) =>
        singleUndirectedRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.INCOMING, next) =>
        multiIncomingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.OUTGOING, next) =>
        multiOutgoingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.BOTH, next) =>
        multiUndirectedRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case NilPathStep =>
        nilProjector
    }

    val projector = project(e.step)
    // Symbol table dependencies is only used for runtime expressions by PatternMatcher. It would be nice to
    // get rid of this from runtime expressions.
    val dependencies = e.step.dependencies.flatMap(_.dependencies).map(_.name)

    SlottedProjectedPath(dependencies, projector)
  }

}
