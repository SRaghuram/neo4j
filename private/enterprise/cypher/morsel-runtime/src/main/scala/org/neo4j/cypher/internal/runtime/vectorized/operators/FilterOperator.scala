/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.storable.Values

/**
 * Takes an input morsel and compacts all rows to the beginning of it, only keeping the rows that match a predicate
 */
class FilterOperator(val workIdentity: WorkIdentity,
                     predicate: Predicate) extends StatelessOperator {

    override def operate(readingRow: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Unit = {

      val writingRow = readingRow.shallowCopy()
      val queryState = new OldQueryState(context, resources = null, params = state.params, cursors, Array.empty[IndexReadSession])

      while (readingRow.hasMoreRows) {
        val matches = predicate(readingRow, queryState) == Values.TRUE
        if (matches) {
          writingRow.copyFrom(readingRow)
          writingRow.moveToNextRow()
        }
        readingRow.moveToNextRow()
      }

      // We need to set validRows of the provided context
      // to the current row of the local writing context
      readingRow.finishedWritingUsing(writingRow)
  }
}
