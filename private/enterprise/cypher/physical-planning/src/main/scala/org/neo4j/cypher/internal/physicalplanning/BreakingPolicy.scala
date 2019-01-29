/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.v4_0.logical.plans.{Aggregation, Distinct, LogicalPlan}

/**
  * Policy that determines what parts of an operator tree belong together.
  *
  * One such part is called a Pipeline, and will have one shared slot configuration.
  */
trait BreakingPolicy {

  /**
    * True if the an operator should be the start of a new pipeline.
    */
  def breakOn(lp: LogicalPlan): Boolean

  /**
    * True if a nested operator should be the start of a new pipeline.
    */
  def breakOnNestedPlan: Boolean

  def invoke(lp: LogicalPlan, slots: SlotConfiguration): SlotConfiguration =
    if (breakOn(lp)) {
      lp match {
        case _: Distinct => slots.emptyWithCachedProperties()
        case _: Aggregation => SlotConfiguration.empty
        case _ => slots.copy()
      }
    } else slots

  def invokeOnNestedPlan(slots: SlotConfiguration): SlotConfiguration =
    if (breakOnNestedPlan) slots.copy() else slots

}

object I_BREAK_FOR_NOONE extends BreakingPolicy {
  override def breakOn(lp: LogicalPlan): Boolean = false
  override def breakOnNestedPlan: Boolean = false
}

