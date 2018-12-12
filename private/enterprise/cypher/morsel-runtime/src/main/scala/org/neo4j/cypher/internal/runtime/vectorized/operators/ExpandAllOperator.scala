/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyTypes
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection

class ExpandAllOperator(val workIdentity: WorkIdentity,
                        fromOffset: Int,
                        relOffset: Int,
                        toOffset: Int,
                        dir: SemanticDirection,
                        types: LazyTypes) extends StreamingOperator {

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselExecutionContext,
                    resources: QueryResources): ContinuableOperatorTask =
    new OTask(inputMorsel)

  class OTask(val inputRow: MorselExecutionContext) extends StreamingContinuableOperatorTask {

    /*
    This might look wrong, but it's like this by design. This allows the loop to terminate early and still be
    picked up at any point again - all without impacting the tight loop.
    The mutable state is an unfortunate cost for this feature.
     */
    private var relationships: RelationshipSelectionCursor = _

    protected override def initializeInnerLoop(inputRow: MorselExecutionContext,
                                               context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources): AutoCloseable = {
      val fromNode = inputRow.getLongAt(fromOffset)
      if (entityIsNull(fromNode))
        null
      else {
        relationships = context.getRelationshipsCursor(fromNode, dir, types.types(context))
        relationships
      }
    }

    override def innerLoop(outputRow: MorselExecutionContext,
                           context: QueryContext,
                           state: QueryState): Unit = {

      while (outputRow.hasMoreRows && relationships.next()) {
        val relId = relationships.relationshipReference()
        val otherSide = relationships.otherNodeReference()

        // Now we have everything needed to create a row.
        outputRow.copyFrom(inputRow)
        outputRow.setLongAt(relOffset, relId)
        outputRow.setLongAt(toOffset, otherSide)
        outputRow.moveToNextRow()
      }
    }
  }
}
