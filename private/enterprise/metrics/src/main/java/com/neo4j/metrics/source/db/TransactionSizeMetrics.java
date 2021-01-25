/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;

import org.neo4j.kernel.impl.api.transaction.monitor.TransactionSizeMonitor;

public class TransactionSizeMetrics implements TransactionSizeMonitor
{
    private final Histogram heapTxSizeHistogram = new Histogram( new UniformReservoir() );
    private final Histogram nativeTxSizeHistogram = new Histogram( new UniformReservoir() );

    @Override
    public void addHeapTransactionSize( long transactionSizeHeap )
    {
        heapTxSizeHistogram.update( transactionSizeHeap );
    }

    @Override
    public void addNativeTransactionSize( long transactionSizeNative )
    {
        nativeTxSizeHistogram.update( transactionSizeNative );
    }

    Histogram heapTxSizeHistogram()
    {
        return heapTxSizeHistogram;
    }

    Histogram nativeTxSizeHistogram()
    {
        return nativeTxSizeHistogram;
    }
}
