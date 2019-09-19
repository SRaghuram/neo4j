/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

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

/**
  * Policy that determines if a plan can be fused or not.
  */
sealed trait OperatorFusionPolicy {

  /**
    * `true` if plan is fusable otherwise `false`
    * @param lp the plan to check
    * @return `true` if plan is fusable otherwise `false`
    */
  def canFuse(lp: LogicalPlan): Boolean


  /**
    * `true` if plan is can be fused over pipeline break otherwise `false`
    * @param lp the plan to check
    * @return `true` if plan is fusable otherwise `false`
    */
  def canFuseOverPipeline(lp: LogicalPlan): Boolean
}

object OperatorFusionPolicy {

  def apply(fusingEnabled: Boolean, parallelExecution: Boolean): OperatorFusionPolicy =
    if (!fusingEnabled) OPERATOR_FUSION_DISABLED
    else {
      if (parallelExecution) OPERATOR_FUSION_WITHIN_PIPELINE else OPERATOR_FUSION_OVER_PIPELINES
    }

  private case object OPERATOR_FUSION_DISABLED extends OperatorFusionPolicy {

    override def canFuse(lp: LogicalPlan): Boolean = false

    override def canFuseOverPipeline(lp: LogicalPlan): Boolean = false
  }

  private case object OPERATOR_FUSION_OVER_PIPELINES extends OperatorFusionPolicy {

    override def canFuse(lp: LogicalPlan): Boolean = {
      lp match {
        // leaf operators
        case _: AllNodesScan |
             _: NodeByLabelScan |
             _: NodeIndexSeek |
             _: NodeUniqueIndexSeek |
             _: NodeIndexContainsScan |
             _: NodeIndexEndsWithScan |
             _: NodeIndexScan |
             _: NodeByIdSeek |
             _: DirectedRelationshipByIdSeek |
             _: UndirectedRelationshipByIdSeek |
             _: NodeCountFromCountStore |
             _: RelationshipCountFromCountStore |
             _: Input
        => true

        // one child operators
        case p: Expand if p.mode == ExpandAll =>
          true

        case _: Selection |
             _: Projection |
             _: UnwindCollection |
             _: Limit
        => true

        // two child operators
        case _: Apply =>
          true

        case _ =>
          false
      }
    }

    override def canFuseOverPipeline(lp: LogicalPlan): Boolean = canFuse(lp)
  }

  private case object OPERATOR_FUSION_WITHIN_PIPELINE extends OperatorFusionPolicy {

    override def canFuse(lp: LogicalPlan): Boolean = OPERATOR_FUSION_OVER_PIPELINES.canFuse(lp)

    override def canFuseOverPipeline(lp: LogicalPlan): Boolean = false
  }
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
  def breakForIds(ids: Id*): PipelineBreakingPolicy =
    new PipelineBreakingPolicy {
      override def breakOn(lp: LogicalPlan): Boolean = ids.contains(lp.id)
      override def onNestedPlanBreak(): Unit = ()
    }
}
