/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.parallel.{NOOP, Scheduler}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.util.TaskCloser

class Dispatcher(morselSize: Int, scheduler: Scheduler) {

  val spatula = NOOP.NoSchedulerTracer

  def execute[E <: Exception](operators: Pipeline,
                              queryContext: QueryContext,
                              params: MapValue,
                              taskCloser: TaskCloser)
                             (visitor: QueryResultVisitor[E]): Unit = {
    val leaf = getLeaf(operators)

    val state = QueryState(params, visitor, morselSize, false)
    val initialTask = leaf.init(MorselExecutionContext.EMPTY, queryContext, state)
    val queryExecution = scheduler.execute(initialTask, spatula)
    val maybeError = queryExecution.await()
    maybeError match {
      case Some(error) =>
        taskCloser.close(false)
        throw error

      case None =>
        taskCloser.close(true)
    }
  }

  private def getLeaf(pipeline: Pipeline): StreamingPipeline = {
    var leafOp = pipeline
    while (leafOp.upstream.nonEmpty) {
      leafOp = leafOp.upstream.get
    }

    leafOp.asInstanceOf[StreamingPipeline]
  }
}
