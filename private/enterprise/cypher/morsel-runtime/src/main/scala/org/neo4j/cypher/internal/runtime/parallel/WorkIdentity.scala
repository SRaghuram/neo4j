/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan

object WorkIdentity {
  def fromPlan(plan: LogicalPlan): WorkIdentity =
    WorkIdentityImpl(plan.id.x, plan.getClass.getSimpleName)
}

trait HasWorkIdentity {
  def workIdentity: WorkIdentity
}

trait WorkIdentity {
  /**
    * Identifies the work/computation performed by this task, as opposed to identifying the task itself.
    * If multiple different tasks all execute the same logic (e.g., operator pipeline) they should return the same <code>workId</code>.
    */
  def workId: Int

  /**
    * Describes the work/computation performed by this task, as opposed to describing the task itself.
    * Multiple tasks that each execute the same logic (e.g., operator pipeline) should return the same value.
    * E.g., OperatorPipeline[AllNodesScan].
    * Two tasks may return the same value <code>workDescription</code> but different values for <code>workId</code>.
    */
  def workDescription: String
}

case class WorkIdentityImpl(workId: Int, workDescription: String) extends WorkIdentity
