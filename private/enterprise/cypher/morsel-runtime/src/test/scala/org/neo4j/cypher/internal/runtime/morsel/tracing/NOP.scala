/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.tracing

import org.neo4j.cypher.internal.runtime.morsel.Task
import org.neo4j.cypher.internal.runtime.morsel.operators.PreparedOutput

case object NOP extends Task[Unit.type] {
  override def executeWorkUnit(resource: Unit.type, workUnitEvent: WorkUnitEvent): PreparedOutput = () => ()

  override def canContinue: Boolean = false

  override def workId: Int = 0

  override def workDescription: String = getClass.getSimpleName
}