/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.ArrayResultExecutionContextFactory
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

case class ProduceResultSlottedPipe(source: Pipe, columns: Seq[(String, Expression)])
                                   (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {

  columns.map(_._2).foreach(_.registerOwningPipe(this))

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    // do not register this pipe as parent as it does not do anything except filtering of already fetched
    // key-value pairs and thus should not have any stats

    // create one resultFactory per execution, to avoid synchronization problems.
    val resultFactory = ArrayResultExecutionContextFactory(columns)

    input.map {
      original => resultFactory.newResult(original, state, state.prePopulateResults)
    }
  }
}
