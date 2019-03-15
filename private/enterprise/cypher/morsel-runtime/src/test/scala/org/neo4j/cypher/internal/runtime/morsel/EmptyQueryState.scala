/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.internal.kernel.api.IndexReadSession
import org.scalatest.mock.MockitoSugar

object EmptyQueryState extends MockitoSugar {
  private val session: IndexReadSession = mock[IndexReadSession]
  def apply(): QueryState = {
    QueryState(Array.empty,
               null,
               10000,
               Array(session),
               NO_TRANSACTION_BINDER,
               numberOfWorkers = 1,
               nExpressionSlots = 1,
               NoInput)
  }
}
