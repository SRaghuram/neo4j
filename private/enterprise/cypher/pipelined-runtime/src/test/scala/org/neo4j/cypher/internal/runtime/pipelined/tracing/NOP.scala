/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.tracing

import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.pipelined.Task
import org.neo4j.cypher.internal.runtime.pipelined.operators.PreparedOutput
import org.neo4j.cypher.internal.util.attribution.Id

case object NOP extends Task[Unit.type] {
  override def executeWorkUnit(resource: Unit.type,
                               workUnitEvent: WorkUnitEvent,
                               queryProfiler: QueryProfiler): PreparedOutput = resources => {}

  override def canContinue: Boolean = false

  override def workId: Id = Id(0)

  override def workDescription: String = getClass.getSimpleName
}
