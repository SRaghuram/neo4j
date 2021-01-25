/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Policy that determines what parts of an operator tree belong together.
 *
 * One such part is called a Pipeline, and will have one shared slot configuration.
 */
trait PipelineBreakingPolicy {

  /**
   * True if the an operator should be the start of a new pipeline.
   */
  def breakOn(lp: LogicalPlan, outerApplyPlanId: Id): Boolean

  def invoke(lp: LogicalPlan, slots: SlotConfiguration, argumentSlots: SlotConfiguration, outerApplyPlanId: Id): SlotConfiguration =
    if (breakOn(lp, outerApplyPlanId)) {
      lp match {
        case _: AggregatingPlan => argumentSlots.copy()
        case _ => slots.copy()
      }
    } else slots

  def nestedPlanBreakingPolicy: PipelineBreakingPolicy = this
}

object BREAK_FOR_LEAFS extends PipelineBreakingPolicy {
  override def breakOn(lp: LogicalPlan, outerApplyPlanId: Id): Boolean = lp.isInstanceOf[LogicalLeafPlan]
}

object PipelineBreakingPolicy {
  def breakFor(logicalPlans: LogicalPlan*): PipelineBreakingPolicy =
    new PipelineBreakingPolicy {
      override def breakOn(lp: LogicalPlan, outerApplyPlanId: Id): Boolean = logicalPlans.contains(lp)
    }
  def breakForIds(ids: Id*): PipelineBreakingPolicy =
    new PipelineBreakingPolicy {
      override def breakOn(lp: LogicalPlan, outerApplyPlanId: Id): Boolean = ids.contains(lp.id)
    }
}
