/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.OperatorFuserFactory
import org.neo4j.cypher.internal.physicalplanning.OperatorFusionPolicy
import org.neo4j.cypher.internal.physicalplanning.OperatorFusionPolicy.OPERATOR_FUSION_DISABLED
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.runtime.pipelined.TemplateOperators.NewTemplate
import org.neo4j.cypher.internal.util.attribution.Id

object TemplateOperatorPolicy {

  def apply(fusionEnabled: Boolean, fusionOverPipelinesEnabled: Boolean, fusionOverPipelineLimit: Int, readOnly: Boolean, parallelExecution: Boolean): OperatorFusionPolicy[NewTemplate] =
    if (fusionEnabled)
      new TemplateOperatorPolicy(fusionOverPipelinesEnabled, fusionOverPipelineLimit, readOnly, parallelExecution)
    else
      OPERATOR_FUSION_DISABLED
}

class TemplateOperatorPolicy(override val fusionOverPipelineEnabled: Boolean,
                             override val fusionOverPipelineLimit: Int,
                             readOnly: Boolean,
                             parallelExecution: Boolean)
  extends TemplateOperators(readOnly, parallelExecution, fusionOverPipelineEnabled) with OperatorFusionPolicy[NewTemplate] {

  override def fusionEnabled: Boolean = true

  override def canFuse(lp: LogicalPlan, outerApplyPlanId: Id): Boolean =
    createTemplate(lp, isHeadOperator = true, hasNoNestedArguments = outerApplyPlanId == Id.INVALID_ID).isDefined ||
    lp.isInstanceOf[Apply] // For the purpose of the policy, Apply can be fused, although it doesn't generate any code.

  override def canFuseOverPipeline(lp: LogicalPlan, outerApplyPlanId: Id): Boolean =
    fusionOverPipelineEnabled && createTemplate(lp, isHeadOperator = false, hasNoNestedArguments = outerApplyPlanId == Id.INVALID_ID).isDefined

  override def operatorFuserFactory(physicalPlan: PhysicalPlan, readOnly: Boolean, parallelExecution: Boolean): OperatorFuserFactory[NewTemplate] =
    new TemplateOperatorFuserFactory(physicalPlan, readOnly, parallelExecution, fusionOverPipelineEnabled)

  override def toString: String = {
    s"${this.getClass.getSimpleName}(fusionOverPipelineEnabled=$fusionOverPipelineEnabled,readOnly=$readOnly,parallelExecution=$parallelExecution,fusionOverPipelineLimit=$fusionOverPipelineLimit)"
  }
}
