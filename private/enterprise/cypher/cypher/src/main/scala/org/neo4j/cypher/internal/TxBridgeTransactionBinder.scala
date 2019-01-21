/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.runtime.morsel.TransactionBinder
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge

class TxBridgeTransactionBinder(txBridge: ThreadToStatementContextBridge) extends TransactionBinder {

  override def bindToThread(transaction: Transaction): Unit =
    txBridge.bindTransactionToCurrentThread(transaction.asInstanceOf[KernelTransaction])

  override def unbindFromThread(): Unit =
    txBridge.unbindTransactionFromCurrentThread()
}
