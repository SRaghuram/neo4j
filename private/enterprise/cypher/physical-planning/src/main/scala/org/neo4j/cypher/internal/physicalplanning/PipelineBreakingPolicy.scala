/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
   * @return `true` if any fusion at all is enabled with this policy, otherwise `false`
   */
  def fusionEnabled: Boolean

  /**
   * @return `true` if fusion over pipelines is enabled with this policy, otherwise `false`
   */
  def fusionOverPipelineEnabled: Boolean

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

  def apply(fusionEnabled: Boolean, fusionOverPipelinesEnabled: Boolean): OperatorFusionPolicy =
    if (!fusionEnabled) OPERATOR_FUSION_DISABLED
    else {
      if (fusionOverPipelinesEnabled) OPERATOR_FUSION_OVER_PIPELINES else OPERATOR_FUSION_WITHIN_PIPELINE
    }

  case object OPERATOR_FUSION_DISABLED extends OperatorFusionPolicy {

    override def canFuse(lp: LogicalPlan): Boolean = false

    override def canFuseOverPipeline(lp: LogicalPlan): Boolean = false

    override def fusionEnabled: Boolean = false

    override def fusionOverPipelineEnabled: Boolean = false
  }

  case object OPERATOR_FUSION_OVER_PIPELINES extends OperatorFusionPolicy {

    override def fusionEnabled: Boolean = true

    override def fusionOverPipelineEnabled: Boolean = true

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
             _: Input |
             _: Argument
        => true

        // one child operators
        case p: Expand if p.mode == ExpandAll =>
          true

        case _: Selection |
             _: Projection |
             _: UnwindCollection |
             _: Limit |
             _: VarExpand |
             _: CacheProperties
        => true

        // two child operators
        case _: Apply =>
          true

        case _ =>
          false
      }
    }

    override def canFuseOverPipeline(lp: LogicalPlan): Boolean = lp match {
      case _: Argument |
           _: Input =>
        false

      //because of how fused var expand is implemented where we evaluate predicate in a separate specialized
      //implementation of a VarExpandCursor we can at this moment not fuse VarExpand if it contains
      //predicates
      case e: VarExpand if e.nodePredicate.isDefined || e.relationshipPredicate.isDefined =>
        false

      case _ =>
        canFuse(lp)
    }
  }

  case object OPERATOR_FUSION_WITHIN_PIPELINE extends OperatorFusionPolicy {

    override def fusionEnabled: Boolean = true

    override def fusionOverPipelineEnabled: Boolean = false

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
