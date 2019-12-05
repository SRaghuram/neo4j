/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.physicalplanning.{PhysicalPlan, SlotConfiguration, ast => runtimeAst}
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.{expressions => commands, predicates => commandPredicates}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NestedPipeExpression
import org.neo4j.cypher.internal.runtime.interpreted.{CommandProjection, GroupingExpression}
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters.orderGroupingKeyExpressions
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath._
import org.neo4j.cypher.internal.runtime.slotted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.{expressions => runtimeExpression}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.{expressions => ast}

object SlottedExpressionConverters {
  // This is a shared method to provide consistent ordering of grouping keys for all slot-based runtimes
  def orderGroupingKeyExpressions(groupings: Iterable[(String, Expression)], orderToLeverage: Seq[Expression])
                                 (slots: SlotConfiguration): Seq[(String, Expression, Boolean)] = {
    // SlotConfiguration is a separate parameter list so it can be applied at a later stage
    groupings.toSeq.map(a => (a._1, a._2, orderToLeverage.contains(a._2)))
      // Sort grouping key (1) by expressions with provided order first, followed by unordered expressions
      //          and then (2) by slot offset
      .sortBy(b => (!b._3, slots(b._1).offset))
  }
}

case class SlottedExpressionConverters(physicalPlan: PhysicalPlan) extends ExpressionConverter {

  override def toCommandProjection(id: Id, projections: Map[String, Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = {
    val slots = physicalPlan.slotConfigurations(id)
    val projected = for {(k, v) <- projections if !slots(k).isLongSlot } yield slots(k).offset -> self.toCommandExpression(id, v)
    Some(SlottedCommandProjection(projected))
  }

  override def toGroupingExpression(id: Id,
                                    projections: Map[String, Expression],
                                    orderToLeverage: Seq[Expression],
                                    self: ExpressionConverters): Option[GroupingExpression] = {
    val slots = physicalPlan.slotConfigurations(id)
    val orderedGroupings = orderGroupingKeyExpressions(projections, orderToLeverage)(slots)
      .map(e => (slots(e._1), self.toCommandExpression(id, e._2), e._3))

    orderedGroupings.toList match {
      case Nil => Some(EmptyGroupingExpression)
      case (slot, e, ordered)::Nil => Some(SlottedGroupingExpression1(SlotExpression(slot, e, ordered)))
      case (s1, e1, o1)::(s2, e2, o2)::Nil => Some(SlottedGroupingExpression2(SlotExpression(s1, e1, o1), SlotExpression(s2, e2, o2)))
      case (s1, e1, o1)::(s2, e2, o2)::(s3, e3, o3)::Nil => Some(SlottedGroupingExpression3(SlotExpression(s1, e1, o1), SlotExpression(s2, e2, o2), SlotExpression(s3, e3, o3)))
      case _ => Some(SlottedGroupingExpression(orderedGroupings.map(t => SlotExpression(t._1, t._2, t._3)).toArray))
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
      case runtimeAst.SlottedCachedPropertyWithPropertyToken(_, _, offset, offsetIsForLongSlot, token, cachedPropertyOffset, NODE_TYPE) =>
        Some(runtimeExpression.SlottedCachedNodeProperty(offset, offsetIsForLongSlot, token, cachedPropertyOffset))
      case runtimeAst.SlottedCachedPropertyWithPropertyToken(_, _, offset, offsetIsForLongSlot, token, cachedPropertyOffset, RELATIONSHIP_TYPE) =>
        Some(runtimeExpression.SlottedCachedRelationshipProperty(offset, offsetIsForLongSlot, token, cachedPropertyOffset))
      case runtimeAst.RelationshipProperty(offset, token, _) =>
        Some(runtimeExpression.RelationshipProperty(offset, token))
      case runtimeAst.IdFromSlot(offset) =>
        Some(runtimeExpression.IdFromSlot(offset))
      case runtimeAst.LabelsFromSlot(offset) =>
        Some(runtimeExpression.LabelsFromSlot(offset))
      case e: runtimeAst.HasLabelsFromSlot =>
        Some(hasLabelsFromSlot(id, e, self))
      case runtimeAst.RelationshipTypeFromSlot(offset) =>
        Some(runtimeExpression.RelationshipTypeFromSlot(offset))
      case runtimeAst.NodePropertyLate(offset, propKey, _) =>
        Some(runtimeExpression.NodePropertyLate(offset, propKey))
      case runtimeAst.SlottedCachedPropertyWithoutPropertyToken(_, _, offset, offsetIsForLongSlot, propertyKey, cachedPropertyOffset, NODE_TYPE) =>
        Some(runtimeExpression.SlottedCachedNodePropertyLate(offset, offsetIsForLongSlot, propertyKey, cachedPropertyOffset))
      case runtimeAst.SlottedCachedPropertyWithoutPropertyToken(_, _, offset, offsetIsForLongSlot, propertyKey, cachedPropertyOffset, RELATIONSHIP_TYPE) =>
        Some(runtimeExpression.SlottedCachedRelationshipPropertyLate(offset, offsetIsForLongSlot, propertyKey, cachedPropertyOffset))
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
      case e: ExpressionVariable =>
        Some(commands.ExpressionVariable(e.offset, e.name))
      case e: NestedPipeExpression =>
        Some(runtimeExpression.NestedPipeSlottedExpression(e.pipe,
                                                           self.toCommandExpression(id, e.projection),
                                                           physicalPlan.nestedPlanArgumentConfigurations(e.pipe.id),
                                                           e.availableExpressionVariables.map(commands.ExpressionVariable.of).toArray))
      case _ =>
        None
    }

  private def hasLabelsFromSlot(id: Id, e: runtimeAst.HasLabelsFromSlot, self: ExpressionConverters): Predicate = {
    val preds =
      e.resolvedLabelTokens.map { labelId =>
        HasLabelFromSlot(e.offset, labelId)
      } ++
      e.lateLabels.map { labelName =>
        HasLabelFromSlotLate(e.offset, labelName): Predicate
      }
    commandPredicates.Ands(preds: _*)
  }

  def toCommandProjectedPath(id:Id, e: ast.PathExpression, self: ExpressionConverters): SlottedProjectedPath = {
    def project(pathStep: PathStep): Projector = pathStep match {

      case NodePathStep(nodeExpression, next) =>
        singleNodeProjector(toCommandExpression(id, nodeExpression, self).get, project(next))

      case ast.SingleRelationshipPathStep(relExpression, _, Some(targetNodeExpression), next) =>
        singleRelationshipWithKnownTargetProjector(toCommandExpression(id, relExpression, self).get,
                                                   toCommandExpression(id, targetNodeExpression, self).get,
                                                   project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.INCOMING, _, next) =>
        singleIncomingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.OUTGOING, _, next) =>
        singleOutgoingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.BOTH, _, next) =>
        singleUndirectedRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.INCOMING, Some(targetNodeExpression), next) =>
        multiIncomingRelationshipWithKnownTargetProjector(
          toCommandExpression(id, relExpression, self).get,
          toCommandExpression(id, targetNodeExpression, self).get,
          project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.INCOMING, _, next) =>
        multiIncomingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.OUTGOING, Some(targetNodeExpression), next) =>
        multiOutgoingRelationshipWithKnownTargetProjector(
          toCommandExpression(id, relExpression, self).get,
          toCommandExpression(id, targetNodeExpression, self).get,
          project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.OUTGOING, _, next) =>
        multiOutgoingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.BOTH, Some(targetNodeExpression), next) =>
        multiUndirectedRelationshipWithKnownTargetProjector(
          toCommandExpression(id, relExpression, self).get,
          toCommandExpression(id, targetNodeExpression, self).get,
          project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.BOTH, _, next) =>
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
