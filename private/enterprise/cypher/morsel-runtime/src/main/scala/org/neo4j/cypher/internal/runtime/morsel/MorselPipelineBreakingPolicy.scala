/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.configuration.GraphDatabaseSettings.CypherMorselUseInterpretedPipes
import org.neo4j.cypher.internal.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.{OperatorFusionPolicy, PipelineBreakingPolicy}
import org.neo4j.exceptions.CantCompileQueryException

// TODO: Replace config with interpretedPipesPolicy
case class MorselPipelineBreakingPolicy(config: CypherRuntimeConfiguration, fusionPolicy: OperatorFusionPolicy) extends PipelineBreakingPolicy {

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
      case e: Expand =>
        if (e.mode == ExpandAll) !canFuseOneChildOperator(e)
        else
          throw unsupported("ExpandInto")

      case _: UnwindCollection |
           _: Sort |
           _: Top |
           _: Aggregation |
           _: Optional |
           _: VarExpand |
           _: PruningVarExpand |
           _: ProcedureCall
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
           _: NodeHashJoin
      => true

      case plan =>
        if (config.useInterpretedPipes == CypherMorselUseInterpretedPipes.ALL_POSSIBLE_PLANS) {
          plan match {
            // Blacklisted non-eager plans
            case _: Skip =>
              // TODO: LoadCSV if parallelExecution
              throw unsupported(plan.getClass.getSimpleName)

            // We do not support any eager plans
            case _: EagerLogicalPlan |
                 _: EmptyResult =>
              throw unsupported(plan.getClass.getSimpleName)

// The old morsel blacklist, for context:
// * Now implemented
// *          case _: plans.Limit | // Limit keeps state (remaining counter) between iterator.next() calls, so we cannot re-create iterator
// *               _: plans.Optional | // Optional pipe relies on a pull-based dataflow and needs a different solution for push
//                 _: plans.Skip | // Skip pipe eagerly drops n rows upfront which does not work with feed pipe
//                 _: plans.Eager | // We do not support eager plans since the resulting iterators cannot be recreated and fed a single input row at a time
//                 _: plans.PartialSort | // same as above
//                 _: plans.PartialTop | // same as above
//                 _: plans.EmptyResult | // Eagerly exhausts the source iterator
// *               _: plans.Distinct | // Even though the Distinct pipe is not really eager it still keeps state
//                 _: plans.LoadCSV | // Not verified to be thread safe
//                 _: plans.ProcedureCall => // Even READ_ONLY Procedures are not allowed because they will/might access the
//                                              transaction via Core API reads, which is not thread safe because of the transaction
//                                              bound CursorFactory.

            // Cardinality increasing plans need to break
            case e: Expand if e.mode == ExpandInto =>
              true

            case _: OptionalExpand |
                 _: FindShortestPaths =>
              true

            case p: ProjectEndpoints if !p.directed => // Undirected is cardinality increasing
              true

            case _ =>
              false
          }
        }
        else
          throw unsupported(plan.getClass.getSimpleName)
    }
  }

  override def onNestedPlanBreak(): Unit = throw unsupported("NestedPlanExpression")

  private def unsupported(thing: String): CantCompileQueryException =
    new CantCompileQueryException(s"Morsel does not yet support the plans including `$thing`, use another runtime.")

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