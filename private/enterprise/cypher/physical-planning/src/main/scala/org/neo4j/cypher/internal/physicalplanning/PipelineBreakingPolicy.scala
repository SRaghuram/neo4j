/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.{AggregatingPlan, LogicalLeafPlan, LogicalPlan}

/**
  * Policy that determines what parts of an operator tree belong together.
  *
  * One such part is called a Pipeline, and will have one shared slot configuration.
  */
trait PipelineBreakingPolicy {

  /**
    * True if the an operator should be the start of a new pipeline.
    */
  def breakOn(lp: LogicalPlan): Boolean

  /**
    * A nested operator is always the start of a new pipeline. This callback
    * simply gives the breaking policy a possibility to throw if this is not
    * acceptable. And it's nice that the [[PipelineBreakingPolicy]] gets notified
    * of all breaks.
    */
  def onNestedPlanBreak(): Unit

  def invoke(lp: LogicalPlan, slots: SlotConfiguration, argumentSlots: SlotConfiguration): SlotConfiguration =
    if (breakOn(lp)) {
      lp match {
        case _: AggregatingPlan => argumentSlots.copy()
        case _ => slots.copy()
      }
    } else slots
}

object BREAK_FOR_LEAFS extends PipelineBreakingPolicy {
  override def breakOn(lp: LogicalPlan): Boolean = lp.isInstanceOf[LogicalLeafPlan]
  override def onNestedPlanBreak(): Unit = ()
}

object PipelineBreakingPolicy {
  def breakFor(logicalPlans: LogicalPlan*): PipelineBreakingPolicy =
    new PipelineBreakingPolicy {
      override def breakOn(lp: LogicalPlan): Boolean = logicalPlans.contains(lp)
      override def onNestedPlanBreak(): Unit = ()
    }
}
