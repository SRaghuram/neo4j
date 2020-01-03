/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled

import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.profiling.ProfilingTracer
import org.neo4j.cypher.internal.runtime.{ExecutionMode, QueryContext}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

case class CompiledPlan(updating: Boolean,
                        columns: Seq[String],
                        executionResultBuilder: RunnablePlan)

trait RunnablePlan {
  def apply(queryContext: QueryContext,
            execMode: ExecutionMode,
            tracer: Option[ProfilingTracer],
            params: MapValue,
            prePopulateResults: Boolean,
            subscriber: QuerySubscriber): RuntimeResult

  def metadata: Seq[Argument]
}
