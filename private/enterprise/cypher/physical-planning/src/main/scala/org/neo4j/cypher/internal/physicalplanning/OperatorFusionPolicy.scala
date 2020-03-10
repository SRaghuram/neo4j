/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Policy that determines if a plan can be fused or not.
 */
trait OperatorFusionPolicy {
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
  def canFuse(lp: LogicalPlan, outerApplyPlanId: Id): Boolean


  /**
   * `true` if plan is can be fused over pipeline break otherwise `false`
   * @param lp the plan to check
   * @return `true` if plan is fusable otherwise `false`
   */
  def canFuseOverPipeline(lp: LogicalPlan, outerApplyPlanId: Id): Boolean

  def operatorFuserFactory(physicalPlan: PhysicalPlan,
                           tokenContext: TokenContext,
                           readOnly: Boolean,
                           indexRegistrator: QueryIndexRegistrator,
                           parallelExecution: Boolean,
                           codeGenerationMode: CodeGeneration.CodeGenerationMode): OperatorFuserFactory
}

object OperatorFusionPolicy {

  case object OPERATOR_FUSION_DISABLED extends OperatorFusionPolicy {
    override def canFuse(lp: LogicalPlan, outerApplyPlanId: Id): Boolean = false
    override def canFuseOverPipeline(lp: LogicalPlan, outerApplyPlanId: Id): Boolean = false
    override def fusionEnabled: Boolean = false
    override def fusionOverPipelineEnabled: Boolean = false
    override def operatorFuserFactory(physicalPlan: PhysicalPlan,
                                      tokenContext: TokenContext,
                                      readOnly: Boolean,
                                      indexRegistrator: QueryIndexRegistrator,
                                      parallelExecution: Boolean,
                                      codeGenerationMode: CodeGeneration.CodeGenerationMode): OperatorFuserFactory = OperatorFuserFactory.NO_FUSION
  }
}

trait OperatorFuserFactory {
  def newOperatorFuser(headPlanId: Id, inputSlotConfiguration: SlotConfiguration): OperatorFuser
}

object OperatorFuserFactory {
  val NO_FUSION: OperatorFuserFactory =
    (_: Id, _: SlotConfiguration) => new OperatorFuser {
      override def fuseIn(plan: LogicalPlan): Boolean = false
      override def fuseIn(output: OutputDefinition): Boolean = false
      override def fusedPlans: IndexedSeq[LogicalPlan] = IndexedSeq.empty
    }
}

trait OperatorFuser {
  def fuseIn(plan: LogicalPlan): Boolean
  def fuseIn(output: OutputDefinition): Boolean
  def fusedPlans: IndexedSeq[LogicalPlan]
}
