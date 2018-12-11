/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.virtual.VirtualValues
import org.scalatest.mock.MockitoSugar

object EmptyQueryState extends MockitoSugar {
  val session: IndexReadSession = mock[IndexReadSession]
  def apply(): QueryState = {
    QueryState(VirtualValues.EMPTY_MAP, null, 10000, Array(session), NO_TRANSACTION_BINDER)
  }
}
