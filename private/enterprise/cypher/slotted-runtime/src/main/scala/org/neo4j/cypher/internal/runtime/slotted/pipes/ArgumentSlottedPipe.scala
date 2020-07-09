/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class ArgumentSlottedPipe()(val id: Id = Id.INVALID_ID)
  extends Pipe {

  def internalCreateResults(state: QueryState): Iterator[CypherRow] = {
    val context = state.newRowWithArgument(rowFactory)
    Iterator(context)
  }
}
