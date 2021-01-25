/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsCounter;
import com.neo4j.metrics.metric.MetricsRegister;

import java.util.function.Supplier;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.TransactionIdStore;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database transaction metrics" )
public class TransactionMetrics extends LifecycleAdapter
{
    private static final String TRANSACTION_PREFIX = "transaction";

    @Documented( "The total number of started transactions. (counter)" )
    private static final String TX_STARTED_TEMPLATE = name( TRANSACTION_PREFIX, "started" );
    @Documented( "The highest peak of concurrent transactions. (counter)" )
    private static final String TX_PEAK_CONCURRENT_TEMPLATE = name( TRANSACTION_PREFIX, "peak_concurrent" );

    @Documented( "The number of currently active transactions. (gauge)" )
    private static final String TX_ACTIVE_TEMPLATE = name( TRANSACTION_PREFIX, "active" );
    @Documented( "The number of currently active read transactions. (gauge)" )
    private static final String READ_TX_ACTIVE_TEMPLATE = name( TRANSACTION_PREFIX, "active_read" );
    @Documented( "The number of currently active write transactions. (gauge)" )
    private static final String WRITE_TX_ACTIVE_TEMPLATE = name( TRANSACTION_PREFIX, "active_write" );

    @Documented( "The total number of committed transactions. (counter)" )
    private static final String TX_COMMITTED_TEMPLATE = name( TRANSACTION_PREFIX, "committed" );
    @Documented( "The total number of committed read transactions. (counter)" )
    private static final String READ_TX_COMMITTED_TEMPLATE = name( TRANSACTION_PREFIX, "committed_read" );
    @Documented( "The total number of committed write transactions. (counter)" )
    private static final String WRITE_TX_COMMITTED_TEMPLATE = name( TRANSACTION_PREFIX, "committed_write" );

    @Documented( "The total number of rolled back transactions. (counter)" )
    private static final String TX_ROLLBACKS_TEMPLATE = name( TRANSACTION_PREFIX, "rollbacks" );
    @Documented( "The total number of rolled back read transactions. (counter)" )
    private static final String READ_TX_ROLLBACKS_TEMPLATE = name( TRANSACTION_PREFIX, "rollbacks_read" );
    @Documented( "The total number of rolled back write transactions. (counter)" )
    private static final String WRITE_TX_ROLLBACKS_TEMPLATE = name( TRANSACTION_PREFIX, "rollbacks_write" );

    @Documented( "The total number of terminated transactions. (counter)" )
    private static final String TX_TERMINATED_TEMPLATE = name( TRANSACTION_PREFIX, "terminated" );
    @Documented( "The total number of terminated read transactions. (counter)" )
    private static final String READ_TX_TERMINATED_TEMPLATE = name( TRANSACTION_PREFIX, "terminated_read" );
    @Documented( "The total number of terminated write transactions. (counter)" )
    private static final String WRITE_TX_TERMINATED_TEMPLATE = name( TRANSACTION_PREFIX, "terminated_write" );

    @Documented( "The ID of the last committed transaction. (counter)" )
    private static final String LAST_COMMITTED_TX_ID_TEMPLATE = name( TRANSACTION_PREFIX, "last_committed_tx_id" );
    @Documented( "The ID of the last closed transaction. (counter)" )
    private static final String LAST_CLOSED_TX_ID_TEMPLATE = name( TRANSACTION_PREFIX, "last_closed_tx_id" );

    @Documented( "The transactions' size on heap in bytes. (histogram)" )
    private static final String TX_SIZE_HEAP_TEMPLATE = name( TRANSACTION_PREFIX, "tx_size_heap" );
    @Documented( "The transactions' size in native memory in bytes. (histogram)" )
    private static final String TX_SIZE_NATIVE_TEMPLATE = name( TRANSACTION_PREFIX, "tx_size_native" );

    private final String txStarted;
    private final String txPeakConcurrent;

    private final String txActive;
    private final String readTxActive;
    private final String writeTxActive;

    private final String txCommitted;
    private final String readTxCommitted;
    private final String writeTxCommitted;

    private final String txRollbacks;
    private final String readTxRollbacks;
    private final String writeTxRollbacks;

    private final String txTerminated;
    private final String readTxTerminated;
    private final String writeTxTerminated;

    private final String lastCommittedTxId;
    private final String lastClosedTxId;

    private final String txSizeHeap;
    private final String txSizeNative;
    private final TransactionSizeMetrics transactionSizeMetrics = new TransactionSizeMetrics();

    private final MetricsRegister registry;
    private final TransactionCounters transactionCounters;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;

    public TransactionMetrics( String metricsPrefix, MetricsRegister registry,
            Supplier<TransactionIdStore> transactionIdStoreSupplier, TransactionCounters transactionCounters )
    {
        this.txStarted = name( metricsPrefix, TX_STARTED_TEMPLATE );
        this.txPeakConcurrent = name( metricsPrefix, TX_PEAK_CONCURRENT_TEMPLATE );
        this.txActive = name( metricsPrefix, TX_ACTIVE_TEMPLATE );
        this.readTxActive = name( metricsPrefix, READ_TX_ACTIVE_TEMPLATE );
        this.writeTxActive = name( metricsPrefix, WRITE_TX_ACTIVE_TEMPLATE );
        this.txCommitted = name( metricsPrefix, TX_COMMITTED_TEMPLATE );
        this.readTxCommitted = name( metricsPrefix, READ_TX_COMMITTED_TEMPLATE );
        this.writeTxCommitted = name( metricsPrefix, WRITE_TX_COMMITTED_TEMPLATE );
        this.txRollbacks = name( metricsPrefix, TX_ROLLBACKS_TEMPLATE );
        this.readTxRollbacks = name( metricsPrefix, READ_TX_ROLLBACKS_TEMPLATE );
        this.writeTxRollbacks = name( metricsPrefix, WRITE_TX_ROLLBACKS_TEMPLATE );
        this.txTerminated = name( metricsPrefix, TX_TERMINATED_TEMPLATE );
        this.readTxTerminated = name( metricsPrefix, READ_TX_TERMINATED_TEMPLATE );
        this.writeTxTerminated = name( metricsPrefix, WRITE_TX_TERMINATED_TEMPLATE );
        this.lastCommittedTxId = name( metricsPrefix, LAST_COMMITTED_TX_ID_TEMPLATE );
        this.lastClosedTxId = name( metricsPrefix, LAST_CLOSED_TX_ID_TEMPLATE );
        this.txSizeHeap = name( metricsPrefix, TX_SIZE_HEAP_TEMPLATE );
        this.txSizeNative = name( metricsPrefix, TX_SIZE_NATIVE_TEMPLATE );
        this.registry = registry;
        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.transactionCounters = transactionCounters;
    }

    @Override
    public void start()
    {
        transactionCounters.setTransactionSizeCallback( transactionSizeMetrics );

        registry.register( txStarted, () -> new MetricsCounter( transactionCounters::getNumberOfStartedTransactions ) );
        registry.register( txPeakConcurrent, () -> new MetricsCounter( transactionCounters::getPeakConcurrentNumberOfTransactions ) );

        registry.register( txActive, () -> (Gauge<Long>) transactionCounters::getNumberOfActiveTransactions );
        registry.register( readTxActive, () -> (Gauge<Long>) transactionCounters::getNumberOfActiveReadTransactions );
        registry.register( writeTxActive, () -> (Gauge<Long>) transactionCounters::getNumberOfActiveWriteTransactions );

        registry.register( txCommitted, () -> new MetricsCounter( transactionCounters::getNumberOfCommittedTransactions ) );
        registry.register( readTxCommitted, () -> new MetricsCounter( transactionCounters::getNumberOfCommittedReadTransactions ) );
        registry.register( writeTxCommitted, () -> new MetricsCounter( transactionCounters::getNumberOfCommittedWriteTransactions ) );

        registry.register( txRollbacks, () -> new MetricsCounter(  transactionCounters::getNumberOfRolledBackTransactions ) );
        registry.register( readTxRollbacks, () -> new MetricsCounter( transactionCounters::getNumberOfRolledBackReadTransactions ) );
        registry.register( writeTxRollbacks, () -> new MetricsCounter( transactionCounters::getNumberOfRolledBackWriteTransactions ) );

        registry.register( txTerminated, () -> new MetricsCounter( transactionCounters::getNumberOfTerminatedTransactions ) );
        registry.register( readTxTerminated, () -> new MetricsCounter( transactionCounters::getNumberOfTerminatedReadTransactions ) );
        registry.register( writeTxTerminated, () -> new MetricsCounter( transactionCounters::getNumberOfTerminatedWriteTransactions ) );

        registry.register( lastCommittedTxId, () -> new MetricsCounter( () -> transactionIdStoreSupplier.get().getLastCommittedTransactionId() ) );
        registry.register( lastClosedTxId, () -> new MetricsCounter( () -> transactionIdStoreSupplier.get().getLastClosedTransactionId() ) );

        registry.register( txSizeHeap, () -> transactionSizeMetrics.heapTxSizeHistogram() );
        registry.register( txSizeNative, () -> transactionSizeMetrics.nativeTxSizeHistogram() );
    }

    @Override
    public void stop()
    {
        registry.remove( txStarted );
        registry.remove( txPeakConcurrent );

        registry.remove( txActive );
        registry.remove( readTxActive );
        registry.remove( writeTxActive );

        registry.remove( txCommitted );
        registry.remove( readTxCommitted );
        registry.remove( writeTxCommitted );

        registry.remove( txRollbacks );
        registry.remove( readTxRollbacks );
        registry.remove( writeTxRollbacks );

        registry.remove( txTerminated );
        registry.remove( readTxTerminated );
        registry.remove( writeTxTerminated );

        registry.remove( lastCommittedTxId );
        registry.remove( lastClosedTxId );

        registry.remove( txSizeHeap );
        registry.remove( txSizeNative );

        transactionCounters.setTransactionSizeCallback( null );
    }
}
