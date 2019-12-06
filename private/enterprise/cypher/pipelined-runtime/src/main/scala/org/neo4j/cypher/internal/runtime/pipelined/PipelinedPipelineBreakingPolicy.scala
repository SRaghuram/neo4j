/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.{OperatorFusionPolicy, PipelineBreakingPolicy}
import org.neo4j.exceptions.CantCompileQueryException

case class PipelinedPipelineBreakingPolicy(fusionPolicy: OperatorFusionPolicy, interpretedPipesPolicy: InterpretedPipesFallbackPolicy) extends PipelineBreakingPolicy {

  override def breakOn(lp: LogicalPlan): Boolean = {

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
           _: Argument // TODO: breaking on argument is often silly. Let's not do that when avoidable.
      => true

      // 1 child operators
      case e: OptionalExpand if e.mode == ExpandAll => !canFuseOneChildOperator(e)

      case _: Expand |
           _: UnwindCollection |
           _: Sort |
           _: Top |
           _: Aggregation |
           _: Optional |
           _: VarExpand
      => !canFuseOneChildOperator(lp)

      case _: ProduceResult |
           _: Limit |
           _: Distinct |
           _: Projection |
           _: CacheProperties |
           _: Selection
      => false

      // 2 child operators
      case _: Apply |
           _: NodeHashJoin |
           _: CartesianProduct
      => true

      case plan =>
        interpretedPipesPolicy.breakOn(plan) && !canFuseOneChildOperator(plan)
    }
  }

  override def onNestedPlanBreak(): Unit = throw unsupported("NestedPlanExpression")

  private def unsupported(thing: String): CantCompileQueryException =
    new CantCompileQueryException(s"Pipelined does not yet support the plans including `$thing`, use another runtime.")

  /**
    * Checks if the current one-child operator can be fused.
    *
    * An operator is deemed fusable iff the the fusion policy allows the operator and its child operators all the way
    * down to the last break to be fused. For example if we have a plan like `UNFUSABLE -> Expand` we will say that
    * `Expand` can't be fused and instead insert a pipeline break, whereas for `AllNodesScan -> Expand` we might be
    * able to fuse them together and don't have to insert a pipeline break.
    */
  private def canFuseOneChildOperator(lp: LogicalPlan):Boolean = {
    assert(lp.rhs.isEmpty)

    if (!fusionPolicy.canFuseOverPipeline(lp)) {
      return false
    }
    var current = lp.lhs.orNull
    while (current != null) {
      if (!fusionPolicy.canFuse(current)) {
        return false
      }
      //we made it all the way down to a pipeline break
      if (breakOn(current)) {
        return true
      }
      current = current.lhs.orNull
    }
    true
  }
}

/************************************************************************************
 * Policy that determines if a plan can be backed by an interpreted pull pipe or not.
 */
sealed trait InterpretedPipesFallbackPolicy {
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

  def apply(interpretedPipesFallbackOption: CypherInterpretedPipesFallbackOption, parallelExecution: Boolean): InterpretedPipesFallbackPolicy =
    interpretedPipesFallbackOption match {
      case CypherInterpretedPipesFallbackOption.disabled =>
        INTERPRETED_PIPES_FALLBACK_DISABLED

      case CypherInterpretedPipesFallbackOption.whitelistedPlansOnly =>
        INTERPRETED_PIPES_FALLBACK_FOR_WHITELISTED_PLANS_ONLY(parallelExecution)

      case CypherInterpretedPipesFallbackOption.allPossiblePlans =>
        INTERPRETED_PIPES_FALLBACK_FOR_ALL_POSSIBLE_PLANS(parallelExecution)
    }

  //===================================
  // DISABLED
  case object INTERPRETED_PIPES_FALLBACK_DISABLED extends InterpretedPipesFallbackPolicy {

    override def readOnly: Boolean = true

    override def breakOn(lp: LogicalPlan): Boolean = {
      throw unsupported(lp.getClass.getSimpleName)
    }
  }

  //===================================
  // WHITELIST
  private case class INTERPRETED_PIPES_FALLBACK_FOR_WHITELISTED_PLANS_ONLY(parallelExecution: Boolean) extends InterpretedPipesFallbackPolicy {

    override def readOnly: Boolean = true

    val WHITELIST: PartialFunction[LogicalPlan, Boolean] = {
      //------------------------------------------------------------------------------------
      // Whitelisted breaking plans - All cardinality increasing plans need to break
      case e: Expand if e.mode == ExpandInto =>
        true

      case _: PruningVarExpand |
           _: OptionalExpand |
           _: FindShortestPaths =>
        true

      //------------------------------------------------------------------------------------
      // Whitelisted plans that can be breaking or non-breaking
      case ProcedureCall(_, call) if !parallelExecution =>
        !call.signature.isVoid // Void procedures preserve cardinality and are non-breaking

      case p: ProjectEndpoints =>
        !p.directed // Undirected is cardinality increasing, directed is not

      //------------------------------------------------------------------------------------
      // Whitelisted non-breaking plans
      case _: ErrorPlan =>
        false
    }

    override def breakOn(lp: LogicalPlan): Boolean = {
      WHITELIST.applyOrElse[LogicalPlan, Boolean](lp, _ =>
        // All other plans not explicitly whitelisted are not supported
        throw unsupported(lp.getClass.getSimpleName))
    }
  }

  //===================================
  // BLACKLIST
  private case class INTERPRETED_PIPES_FALLBACK_FOR_ALL_POSSIBLE_PLANS(parallelExecution: Boolean) extends InterpretedPipesFallbackPolicy {

    override def readOnly: Boolean = false

    private val WHITELIST = INTERPRETED_PIPES_FALLBACK_FOR_WHITELISTED_PLANS_ONLY(parallelExecution).WHITELIST

    val BLACKLIST: PartialFunction[LogicalPlan, Boolean] = {
      // Blacklisted non-eager plans
      case lp: Skip => // Maintains state
        throw unsupported(lp.getClass.getSimpleName)

      // Not supported in parallel execution
      case lp: ProcedureCall if parallelExecution =>
        throw unsupported(lp.getClass.getSimpleName)
      case lp: LoadCSV if parallelExecution =>
        throw unsupported(lp.getClass.getSimpleName)

      // We do not support any eager plans
      case lp: EagerLogicalPlan =>
        throw unsupported(lp.getClass.getSimpleName)

      // No leaf plans (but they should all be supported by operators anyway...)
      case lp if lp.isLeaf =>
        throw unsupported(lp.getClass.getSimpleName)

      // No two-children plans
      case lp if lp.lhs.isDefined && lp.rhs.isDefined =>
        throw unsupported(lp.getClass.getSimpleName)

      // Updating plans and exclusive locking plans are not supported in parallel execution
      case lp @ (_: UpdatingPlan |
                 _: LockNodes) if parallelExecution =>
        throw unsupported(lp.getClass.getSimpleName)
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

  private def unsupported(thing: String): CantCompileQueryException =
    new CantCompileQueryException(s"Pipelined does not yet support the plans including `$thing`, use another runtime.")
}
