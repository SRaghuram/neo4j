/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.internal.kernel.api.Transaction

/**
  * Interface which binds a transaction to the current Thread. This is needed to accommodate
  * Core API functionality, which could be used to read entities in result visitors, procedures or
  * functions.
  *
  * Ideally this should not be needed once we remove [[org.neo4j.kernel.impl.util.NodeProxyWrappingNodeValue]] and
  * [[org.neo4j.kernel.impl.util.RelationshipProxyWrappingValue]].
  */
trait TransactionBinder {
  def bindToThread(transaction: Transaction): Unit
  def unbindFromThread(): Unit
}

object NO_TRANSACTION_BINDER extends TransactionBinder {
  override def bindToThread(transaction: Transaction): Unit = {}
  override def unbindFromThread(): Unit = {}
}

