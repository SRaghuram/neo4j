/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertingMultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.EagerLogicalPlan
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LockNodes
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.NonFuseable
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PreserveOrder
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.physicalplanning.OperatorFusionPolicy
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.TemplateOperators.NewTemplate
import org.neo4j.cypher.internal.util.attribution.Attribute
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException

case class PipelinedPipelineBreakingPolicy(fusionPolicy: OperatorFusionPolicy[NewTemplate],
                                           interpretedPipesPolicy: InterpretedPipesFallbackPolicy,
                                           parallelExecution: Boolean,
                                           override val nestedPlanBreakingPolicy: PipelineBreakingPolicy) extends PipelineBreakingPolicy {

  private class BreakOn extends Attribute[LogicalPlan, (Boolean, Int)]

  private val cache: BreakOn = {
    if (DebugSupport.PHYSICAL_PLANNING.enabled) {
      DebugSupport.PHYSICAL_PLANNING.log("New breaking policy cache for %s", this)
    }
    new BreakOn
  }

  override def breakOn(lp: LogicalPlan, outerApplyPlanId: Id): Boolean = {
    val (break, _) = breakOnInternal(lp, outerApplyPlanId)
    break
  }

  private def breakOnInternal(lp: LogicalPlan, outerApplyPlanId: Id): (Boolean, Int) = {
    if (!cache.isDefinedAt(lp.id)) {
      // Build upon an already cached source plan
      // NOTE: This assumes that the first use of this policy, which will build up the cache,
      //       is traversing the plan tree in a producer-to-consumer order (i.e. starting from the leftmost leaf).
      //       This should hold true for SlotAllocation, which is the first currently known usage of this policy, as well as PipelineTreeBuilder.
      //       As a fallback it will rely on recursion, which _could_ cause stack overflow (e.g. when called on the root first on a very big plan),
      //       but this kind of usage pattern is currently only expected from tests. (If we for some reason need to be able to use the breaking policy
      //       like this in production we could reuse the mechanics of TreeBuilder, which maintains its own stack)
      val sourceFuseOverPipelineCount = findSourcePlanFuseOverPipelineCount(lp, outerApplyPlanId)
      val break = computeBreakOn(lp, outerApplyPlanId, sourceFuseOverPipelineCount)
      if (DebugSupport.PHYSICAL_PLANNING.enabled) {
        DebugSupport.PHYSICAL_PLANNING.log(f" + Breaking policy for ${s"${lp.getClass.getSimpleName}(${lp.id.x})"}%-32s break=${break._1}%-8s fuseOverPipelineCount=${break._2}%3d")
      }
      cache.set(lp.id, break)
      break
    } else {
      cache.get(lp.id)
    }
  }

  private def findSourcePlanFuseOverPipelineCount(lp: LogicalPlan, outerApplyPlanId: Id) = {
    (lp.lhs, lp.rhs) match {
      case (Some(source), None) =>
        val (_, sourceFuseOverPipelineCount) = breakOnInternal(source, outerApplyPlanId)
        sourceFuseOverPipelineCount
      case (Some(_), Some(rhs)) if lp.isInstanceOf[Apply] =>
        // For Apply we follow the right-hand side
        val (_, sourceFuseOverPipelineCount) = breakOnInternal(rhs, outerApplyPlanId)
        sourceFuseOverPipelineCount
      case _ =>
        // In the other cases (leaf or two-children plans) we assume that lp is the first plan following a pipeline break
        0
    }
  }

  private def computeBreakOn(lp: LogicalPlan, outerApplyPlanId: Id, fuseIndex: Int): (Boolean, Int) = {
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
           _: MultiNodeIndexSeek |
           _: AssertingMultiNodeIndexSeek |
           _: DirectedRelationshipByIdSeek |
           _: UndirectedRelationshipByIdSeek |
           _: DirectedRelationshipTypeScan |
           _: UndirectedRelationshipTypeScan |
           _: NodeCountFromCountStore |
           _: RelationshipCountFromCountStore |
           _: Input |
           _: Argument // TODO: breaking on argument is often silly. Let's not do that when avoidable.
      => (true, fuseIndex)

      // 1 child operators
      case _: Expand |
           _: OptionalExpand |
           _: UnwindCollection |
           _: Sort |
           _: PartialSort |
           _: Top |
           _: PartialTop |
           _: Aggregation |
           _: OrderedAggregation |
           _: Optional |
           _: Anti |
           _: VarExpand |
           _: TriadicBuild |
           _: TriadicFilter |
           _: PreserveOrder
      => breakOnFuseableOperator(lp, outerApplyPlanId, fuseIndex)

      // Void procedures preserve cardinality and are always non-breaking
      case ProcedureCall(_, call) if !parallelExecution && call.containsNoUpdates && call.signature.isVoid =>
        (false, fuseIndex)

      case ProcedureCall(_, call) if !parallelExecution && call.containsNoUpdates =>
        breakOnFuseableOperator(lp, outerApplyPlanId, fuseIndex)

      // Undirected is cardinality increasing if nothing is in scope, otherwise not
      case p: ProjectEndpoints if !p.directed && !p.startInScope && !p.endInScope =>
        breakOnFuseableOperator(lp, outerApplyPlanId, fuseIndex)
      case _: ProjectEndpoints =>
        (false, fuseIndex)

      case _: ProduceResult |
           _: Limit |
           _: ExhaustiveLimit |
           _: Skip |
           _: Distinct |
           _: OrderedDistinct |
           _: Projection |
           _: CacheProperties |
           _: Selection |
           _: NonFuseable |
           _: LockNodes |
           _: EmptyResult
      => (false, fuseIndex)

        //WRITE operators
      case _: Create |
           _: SetProperty
        => (false, fuseIndex)

      // 2 child operators
      case _: Apply
      => (false, fuseIndex)

      case _: NodeHashJoin |
           _: RightOuterHashJoin |
           _: LeftOuterHashJoin |
           _: ValueHashJoin |
           _: CartesianProduct |
           _: Union |
           _: ConditionalApply |
           _: AntiConditionalApply |
           _: SelectOrSemiApply |
           _: SelectOrAntiSemiApply
      => (true, fuseIndex)

      case plan =>
        if (interpretedPipesPolicy.breakOn(plan))
          breakOnFuseableOperator(plan, outerApplyPlanId, fuseIndex)
        else
          (false, fuseIndex)
    }
  }

  /**
   * Checks if the current one-child operator can be fused.
   *
   * An operator is deemed fusable iff the the fusion policy allows the operator and its child operators all the way
   * down to the last break to be fused. For example if we have a plan like `UNFUSABLE -> Expand` we will say that
   * `Expand` can't be fused and instead insert a pipeline break, whereas for `AllNodesScan -> Expand` we might be
   * able to fuse them together and don't have to insert a pipeline break.
   */
  private def breakOnFuseableOperator(lp: LogicalPlan, outerApplyPlanId: Id, fuseOverPipelineCount: Int): (Boolean, Int) = {
    require(lp.rhs.isEmpty)

    def previous(p: LogicalPlan): LogicalPlan = p match {
      case Apply(_, right) => right
      case _ => p.lhs.orNull
    }

    if (!fusionPolicy.canFuseOverPipeline(lp, outerApplyPlanId)) {
      return (true, 0)
    }

    val newFuseOverPipelineCount = fuseOverPipelineCount + 1 // We increase this count every time we decide we can fuse over pipelines (by a recursive call)
    if (newFuseOverPipelineCount >= fusionPolicy.fusionOverPipelineLimit) {
      return (true, 0)
    }

    var current = previous(lp)
    while (current != null) {
      if (!fusionPolicy.canFuse(current, outerApplyPlanId)) {
        return (true, 0)
      }
      //we made it all the way down to a pipeline break
      val (break, _) = breakOnInternal(current, outerApplyPlanId)
      if (break) {
        return (false, newFuseOverPipelineCount)
      }
      current = previous(current)
    }
    (false, newFuseOverPipelineCount)
  }
}

/************************************************************************************
 * Policy that determines if a plan can be backed by an interpreted pull pipe or not.
 */
trait InterpretedPipesFallbackPolicy {
  /**
   * True if the fallback only allows read-only plans
   */
  def readOnly: Boolean

  /**
   * True if the an operator should be the start of a new pipeline.
   * @throws CantCompileQueryException if the logical plan is not supported with this policy
   */
  def breakOn(lp: LogicalPlan): Boolean
}

object InterpretedPipesFallbackPolicy {

  def apply(interpretedPipesFallbackOption: CypherInterpretedPipesFallbackOption, parallelExecution: Boolean, runtimeName: String): InterpretedPipesFallbackPolicy =
    interpretedPipesFallbackOption match {
      case CypherInterpretedPipesFallbackOption.disabled =>
        INTERPRETED_PIPES_FALLBACK_DISABLED(runtimeName)

      case CypherInterpretedPipesFallbackOption.default |
           CypherInterpretedPipesFallbackOption.whitelistedPlansOnly =>
        INTERPRETED_PIPES_FALLBACK_FOR_WHITELISTED_PLANS_ONLY(parallelExecution, runtimeName)

      case CypherInterpretedPipesFallbackOption.allPossiblePlans =>
        INTERPRETED_PIPES_FALLBACK_FOR_ALL_POSSIBLE_PLANS(parallelExecution, runtimeName)
    }

  //===================================
  // DISABLED
  private case class INTERPRETED_PIPES_FALLBACK_DISABLED(runtimeName: String) extends InterpretedPipesFallbackPolicy {

    override def readOnly: Boolean = true

    override def breakOn(lp: LogicalPlan): Boolean = {
      throw unsupported(lp.getClass.getSimpleName, runtimeName)
    }
  }

  //===================================
  // WHITELIST
  private case class INTERPRETED_PIPES_FALLBACK_FOR_WHITELISTED_PLANS_ONLY(parallelExecution: Boolean, runtimeName: String) extends InterpretedPipesFallbackPolicy {

    override def readOnly: Boolean = true

    val WHITELIST: PartialFunction[LogicalPlan, Boolean] = {
      //------------------------------------------------------------------------------------
      // Whitelisted breaking plans - All cardinality increasing plans need to break

      case _: PruningVarExpand |
           _: FindShortestPaths =>
        true

      //------------------------------------------------------------------------------------
      // Whitelisted non-breaking plans
      case _: ErrorPlan =>
        false
    }

    override def breakOn(lp: LogicalPlan): Boolean = {
      WHITELIST.applyOrElse[LogicalPlan, Boolean](lp, _ =>
        // All other plans not explicitly whitelisted are not supported
        throw unsupported(lp.getClass.getSimpleName, runtimeName))
    }
  }

  //===================================
  // BLACKLIST
  private case class INTERPRETED_PIPES_FALLBACK_FOR_ALL_POSSIBLE_PLANS(parallelExecution: Boolean, runtimeName: String) extends InterpretedPipesFallbackPolicy {

    override def readOnly: Boolean = false

    private val WHITELIST = INTERPRETED_PIPES_FALLBACK_FOR_WHITELISTED_PLANS_ONLY(parallelExecution, runtimeName).WHITELIST

    val BLACKLIST: PartialFunction[LogicalPlan, Boolean] = {
      // Blacklisted non-eager plans
      case lp: Skip => // Maintains state
        throw unsupported(lp.getClass.getSimpleName, runtimeName)
      case lp: Limit => // Maintains state
        throw unsupported(lp.getClass.getSimpleName, runtimeName)

      // Not supported in parallel execution
      case lp: ProcedureCall if parallelExecution =>
        throw unsupported(lp.getClass.getSimpleName, runtimeName)
      case lp: LoadCSV if parallelExecution =>
        throw unsupported(lp.getClass.getSimpleName, runtimeName)

      // We do not support any eager plans
      case lp: EagerLogicalPlan =>
        throw unsupported(lp.getClass.getSimpleName, runtimeName)

      // No leaf plans (but they should all be supported by operators anyway...)
      case lp if lp.isLeaf =>
        throw unsupported(lp.getClass.getSimpleName, runtimeName)

      // No two-children plans
      case lp if lp.lhs.isDefined && lp.rhs.isDefined =>
        throw unsupported(lp.getClass.getSimpleName, runtimeName)

      // Updating plans and exclusive locking plans are not supported in parallel execution
      case lp @ (_: UpdatingPlan |
                 _: LockNodes) if parallelExecution =>
        throw unsupported(lp.getClass.getSimpleName, runtimeName)
    }

    override def breakOn(lp: LogicalPlan): Boolean = {
      WHITELIST.orElse(BLACKLIST).applyOrElse[LogicalPlan, Boolean](lp, {
        case _: LoadCSV =>
          true

        // All other one child plans are supported and assumed non-breaking (unless defined differently by the WHITELIST)
        case _ =>
          false
      })
    }
  }

  def unsupported(thing: String, runtimeName: String): CantCompileQueryException =
    new CantCompileQueryException(s"$runtimeName does not yet support the plans including `$thing`, use another runtime.")
}
